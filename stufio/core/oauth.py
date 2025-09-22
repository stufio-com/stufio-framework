"""
OAuth utilities for Google and Apple authentication
"""
import json
import logging
from typing import Optional, Dict, Any
from datetime import datetime, timezone
import jwt
from jwt.algorithms import RSAAlgorithm
import requests
from google.auth.transport import requests as google_requests
from google.oauth2 import id_token

from stufio.core.config import get_settings
from stufio.schemas.oauth import GoogleUserInfo, AppleUserInfo, OAuthUserInfo

logger = logging.getLogger(__name__)
settings = get_settings()


class OAuthError(Exception):
    """Base exception for OAuth-related errors"""
    pass


class GoogleOAuthVerifier:
    """Google OAuth ID token verification"""
    
    @staticmethod
    async def verify_id_token(token: str) -> GoogleUserInfo:
        """
        Verify Google ID token and extract user information
        
        Args:
            token: Google ID token (JWT)
            
        Returns:
            GoogleUserInfo: Verified user information
            
        Raises:
            OAuthError: If token verification fails
        """
        try:
            # Get Google client ID from settings
            google_client_id = getattr(settings, 'GOOGLE_CLIENT_ID', None)
            if not google_client_id:
                raise OAuthError("Google client ID not configured")
            
            # Verify the token
            idinfo = id_token.verify_oauth2_token(
                token, 
                google_requests.Request(), 
                google_client_id
            )
            
            # Check if token is from correct issuer
            if idinfo['iss'] not in ['accounts.google.com', 'https://accounts.google.com']:
                raise OAuthError("Invalid token issuer")
            
            # Extract user information
            user_info = GoogleUserInfo(
                sub=idinfo['sub'],
                email=idinfo.get('email'),
                name=idinfo.get('name', ''),
                picture=idinfo.get('picture')
            )
            
            logger.info(f"Successfully verified Google ID token for user: {user_info.email}")
            return user_info
            
        except ValueError as e:
            logger.error(f"Google ID token verification failed: {str(e)}")
            raise OAuthError(f"Invalid Google ID token: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error during Google token verification: {str(e)}")
            raise OAuthError(f"Google authentication failed: {str(e)}")


class AppleOAuthVerifier:
    """Apple OAuth verification using authorization code and ID token"""

    APPLE_TOKEN_URL = "https://appleid.apple.com/auth/token"
    APPLE_KEYS_URL = "https://appleid.apple.com/auth/keys"

    @staticmethod
    def _create_client_secret(use_ios_config: bool = False) -> str:
        """
        Create client secret JWT for Apple OAuth
        
        Args:
            use_ios_config: If True, use iOS configuration, otherwise use web configuration
        
        Returns:
            str: Signed JWT client secret
            
        Raises:
            OAuthError: If client secret creation fails
        """
        try:
            # Get Apple configuration from settings
            if use_ios_config:
                team_id = getattr(settings, 'APPLE_IOS_TEAM_ID', None)
                key_id = getattr(settings, 'APPLE_IOS_KEY_ID', None)  
                client_id = getattr(settings, 'APPLE_IOS_CLIENT_ID', None)
                private_key_path = getattr(settings, 'APPLE_IOS_PRIVATE_KEY_PATH', None)
            else:
                team_id = getattr(settings, 'APPLE_TEAM_ID', None)
                key_id = getattr(settings, 'APPLE_KEY_ID', None)  
                client_id = getattr(settings, 'APPLE_CLIENT_ID', None)
                private_key_path = getattr(settings, 'APPLE_PRIVATE_KEY_PATH', None)

            if not all([team_id, key_id, client_id, private_key_path]):
                raise OAuthError("Apple OAuth configuration incomplete")

            # Read private key
            try:
                if private_key_path:
                    with open(private_key_path, 'r') as f:
                        private_key = f.read()
                else:
                    raise OAuthError("Apple private key path not configured")
            except FileNotFoundError:
                raise OAuthError(f"Apple private key file not found: {private_key_path}")

            # Create JWT payload
            now = datetime.now(timezone.utc)
            payload = {
                'iss': team_id,
                'iat': int(now.timestamp()),
                'exp': int(now.timestamp()) + 3600,  # 1 hour expiration
                'aud': 'https://appleid.apple.com',
                'sub': client_id,
            }

            # Sign JWT
            client_secret = jwt.encode(
                payload,
                private_key,
                algorithm='ES256',
                headers={'kid': key_id}
            )

            return client_secret

        except Exception as e:
            logger.error(f"Failed to create Apple client secret: {str(e)}")
            raise OAuthError(f"Apple client secret creation failed: {str(e)}")

    @staticmethod
    async def _exchange_authorization_code(authorization_code: str, use_ios_config: bool = False) -> Dict[str, Any]:
        """
        Exchange Apple authorization code for tokens
        
        Args:
            authorization_code: Apple authorization code
            use_ios_config: If True, use iOS configuration, otherwise use web configuration
            
        Returns:
            Dict containing access_token, id_token, etc.
            
        Raises:
            OAuthError: If token exchange fails
        """
        try:
            client_secret = AppleOAuthVerifier._create_client_secret(use_ios_config)
            
            if use_ios_config:
                client_id = getattr(settings, 'APPLE_IOS_CLIENT_ID', None)
                redirect_uri = getattr(
                    settings,
                    "APPLE_IOS_REDIRECT_URI",
                    "http://localhost:3000/auth/callback/apple/ios",
                )
            else:
                client_id = getattr(settings, 'APPLE_CLIENT_ID', None)
                redirect_uri = getattr(
                    settings,
                    "APPLE_REDIRECT_URI",
                    "http://localhost:3000/auth/callback/apple",
                )

            data = {
                'client_id': client_id,
                'client_secret': client_secret,
                'code': authorization_code,
                'grant_type': 'authorization_code',
                'redirect_uri': redirect_uri
            }

            response = requests.post(AppleOAuthVerifier.APPLE_TOKEN_URL, data=data)

            if response.status_code != 200:
                logger.error(f"Apple token exchange failed: {response.status_code} {response.text}")
                raise OAuthError(f"Apple token exchange failed: {response.status_code}")

            return response.json()

        except requests.RequestException as e:
            logger.error(f"Network error during Apple token exchange: {str(e)}")
            raise OAuthError(f"Apple token exchange network error: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error during Apple token exchange: {str(e)}")
            raise OAuthError(f"Apple token exchange failed: {str(e)}")

    @staticmethod
    async def _get_apple_public_keys() -> Dict[str, Any]:
        """
        Fetch Apple's public keys for JWT verification
        
        Returns:
            Dict: Apple's public keys in JWKS format
            
        Raises:
            OAuthError: If key fetching fails
        """
        try:
            response = requests.get(AppleOAuthVerifier.APPLE_KEYS_URL)
            response.raise_for_status()
            return response.json()

        except requests.RequestException as e:
            logger.error(f"Failed to fetch Apple public keys: {str(e)}")
            raise OAuthError(f"Failed to fetch Apple public keys: {str(e)}")

    @staticmethod
    async def _verify_id_token(id_token_str: str, use_ios_config: bool = False) -> Dict[str, Any]:
        """
        Verify Apple ID token with proper signature verification
        
        Args:
            id_token_str: Apple ID token (JWT)
            use_ios_config: If True, use iOS configuration, otherwise use web configuration
            
        Returns:
            Dict: Decoded token payload
            
        Raises:
            OAuthError: If token verification fails
        """
        try:
            # Get Apple's public keys
            keys_data = await AppleOAuthVerifier._get_apple_public_keys()
            
            # Get the key ID from the token header
            unverified_header = jwt.get_unverified_header(id_token_str)
            kid = unverified_header.get('kid')
            
            if not kid:
                raise OAuthError("No key ID found in Apple ID token header")
            
            # Find the matching public key JWK
            public_key_jwk = None
            for key in keys_data.get('keys', []):
                if key.get('kid') == kid:
                    public_key_jwk = key
                    break
            
            if not public_key_jwk:
                raise OAuthError(f"Public key not found for key ID: {kid}")
            
            # Get client ID for audience validation
            if use_ios_config:
                client_id = getattr(settings, 'APPLE_IOS_CLIENT_ID', None)
                if not client_id:
                    raise OAuthError("Apple iOS client ID not configured")
            else:
                client_id = getattr(settings, 'APPLE_CLIENT_ID', None)
                if not client_id:
                    raise OAuthError("Apple client ID not configured")
            
            # Convert JWK to RSA public key object
            public_key = RSAAlgorithm.from_jwk(public_key_jwk)
            
            # Verify the token with the RSA public key
            decoded_token = jwt.decode(
                id_token_str,
                public_key,  # type: ignore - PyJWT accepts RSAPublicKey objects
                algorithms=['RS256'],
                audience=client_id,
                issuer='https://appleid.apple.com'
            )

            logger.info(f"Successfully verified Apple ID token signature for user: {decoded_token.get('sub')}")
            return decoded_token

        except jwt.ExpiredSignatureError:
            logger.error("Apple ID token has expired")
            raise OAuthError("Apple ID token has expired")
        except jwt.InvalidAudienceError as e:
            client_id = getattr(settings, 'APPLE_CLIENT_ID', 'unknown')
            logger.error(f"Invalid audience in Apple ID token. Expected: {client_id}")
            raise OAuthError("Invalid audience in Apple ID token")
        except jwt.InvalidIssuerError:
            logger.error("Invalid issuer in Apple ID token")
            raise OAuthError("Invalid issuer in Apple ID token")
        except jwt.InvalidTokenError as e:
            logger.error(f"Apple ID token verification failed: {str(e)}")
            raise OAuthError(f"Invalid Apple ID token: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error during Apple ID token verification: {str(e)}")
            raise OAuthError(f"Apple ID token verification failed: {str(e)}")

    @staticmethod
    async def verify_authorization_code(
        authorization_code: str, 
        identity_token: Optional[str] = None,
        user_data: Optional[str] = None,
        use_ios_config: bool = False
    ) -> AppleUserInfo:
        """
        Verify Apple authorization code and extract user information
        
        Args:
            authorization_code: Apple authorization code
            identity_token: Optional Apple ID token
            user_data: Optional user data JSON string (first login only)
            use_ios_config: If True, use iOS configuration, otherwise use web configuration
            
        Returns:
            AppleUserInfo: Verified user information
            
        Raises:
            OAuthError: If verification fails
        """
        try:
            # Exchange authorization code for tokens
            token_response = await AppleOAuthVerifier._exchange_authorization_code(authorization_code, use_ios_config)

            # Use provided identity_token or the one from token exchange
            id_token_str = identity_token or token_response.get('id_token')

            if not id_token_str:
                raise OAuthError("No ID token available from Apple")

            # Verify ID token
            decoded_token = await AppleOAuthVerifier._verify_id_token(id_token_str, use_ios_config)

            # Extract user information
            apple_user_id = decoded_token['sub']
            email = decoded_token.get('email')

            # Parse user data if provided (first login only)
            full_name = None
            if user_data:
                try:
                    user_info = json.loads(user_data)
                    name_info = user_info.get('name', {})
                    if isinstance(name_info, dict):
                        first_name = name_info.get('firstName', '')
                        last_name = name_info.get('lastName', '')
                        full_name = f"{first_name} {last_name}".strip()
                except (json.JSONDecodeError, KeyError):
                    logger.warning("Failed to parse Apple user data")

            user_info = AppleUserInfo(
                provider_user_id=apple_user_id,
                email=email,
                full_name=full_name or ''
            )

            logger.info(f"Successfully verified Apple authorization code for user: {user_info.email}")
            return user_info

        except Exception as e:
            logger.error(f"Apple authorization code verification failed: {str(e)}")
            if isinstance(e, OAuthError):
                raise
            raise OAuthError(f"Apple authentication failed: {str(e)}")


async def verify_oauth_provider(
    provider: str, 
    token: Optional[str] = None,
    authorization_code: Optional[str] = None,
    identity_token: Optional[str] = None,
    user_data: Optional[str] = None
) -> OAuthUserInfo:
    """
    Verify OAuth token/code for any supported provider
    
    Args:
        provider: OAuth provider name ('google', 'apple', or 'apple_ios')
        token: OAuth token (for Google ID token)
        authorization_code: Authorization code (for Apple)
        identity_token: ID token (for Apple, optional)
        user_data: User data JSON (for Apple, optional)
        
    Returns:
        OAuthUserInfo: Verified user information
        
    Raises:
        OAuthError: If verification fails
    """
    if provider == 'google':
        if not token:
            raise OAuthError("Google OAuth requires ID token")
        return await GoogleOAuthVerifier.verify_id_token(token)
    
    elif provider == 'apple':
        if not authorization_code:
            raise OAuthError("Apple OAuth requires authorization code")
        return await AppleOAuthVerifier.verify_authorization_code(
            authorization_code, identity_token, user_data, use_ios_config=False
        )
    
    elif provider == 'apple_ios':
        if not authorization_code:
            raise OAuthError("Apple iOS OAuth requires authorization code")
        return await AppleOAuthVerifier.verify_authorization_code(
            authorization_code, identity_token, user_data, use_ios_config=True
        )
    
    else:
        raise OAuthError(f"Unsupported OAuth provider: {provider}")
