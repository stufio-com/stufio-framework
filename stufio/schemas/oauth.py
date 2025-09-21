from typing import Optional
from pydantic import BaseModel, Field, EmailStr


class GoogleOAuthRequest(BaseModel):
    """Schema for Google OAuth requests using ID token"""
    grant_type: str = Field(..., description="Must be 'google'")
    token: str = Field(..., description="Google ID token (JWT)")


class AppleOAuthRequest(BaseModel):
    """Schema for Apple OAuth requests using authorization code"""
    grant_type: str = Field(..., description="Must be 'apple_oauth'")
    authorization_code: str = Field(..., description="Apple authorization code")
    identity_token: Optional[str] = Field(None, description="Apple ID token (optional)")
    user_data: Optional[str] = Field(None, description="JSON string with user info (optional, first login only)")


class OAuthUserInfo(BaseModel):
    """Standard user information from OAuth providers"""
    provider_user_id: str = Field(..., description="Unique user ID from OAuth provider")
    email: Optional[EmailStr] = None
    full_name: Optional[str] = None
    profile_picture_url: Optional[str] = None
    provider: str = Field(..., description="OAuth provider name (google, apple)")


class GoogleUserInfo(OAuthUserInfo):
    """Google-specific user information"""
    provider: str = Field(default="google", description="OAuth provider name")
    sub: str = Field(..., description="Google user ID (same as provider_user_id)")
    picture: Optional[str] = Field(None, description="Google profile picture URL")
    
    def __init__(self, **data):
        # Map Google-specific fields to standard fields
        if 'sub' in data:
            data['provider_user_id'] = data['sub']
        if 'picture' in data:
            data['profile_picture_url'] = data['picture']
        if 'name' in data:
            data['full_name'] = data['name']
        super().__init__(**data)


class AppleUserInfo(OAuthUserInfo):
    """Apple-specific user information"""
    provider: str = Field(default="apple", description="OAuth provider name")
    
    def __init__(self, **data):
        # Apple user data comes in different formats
        if 'sub' in data:
            data['provider_user_id'] = data['sub']
        super().__init__(**data)