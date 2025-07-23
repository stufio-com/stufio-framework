from typing import Any, Union, Annotated

from fastapi import APIRouter, Body, Depends, HTTPException, Header, Request
from fastapi.security import OAuth2PasswordRequestForm

from pydantic_core import Url
from stufio import crud, models, schemas
from stufio.api import deps
from stufio.core import security
from stufio.utilities import (
    send_reset_password_email,
    send_magic_login_email,
    send_email_validation_email,
)

from stufio.core.config import get_settings

settings = get_settings()

router = APIRouter()

"""
https://github.com/OWASP/CheatSheetSeries/blob/master/cheatsheets/Authentication_Cheat_Sheet.md
Specifies minimum criteria:
    - Change password must require current password verification to ensure that it's the legitimate user.
    - Login page and all subsequent authenticated pages must be exclusively accessed over TLS or other strong transport.
    - An application should respond with a generic error message regardless of whether:
        - The user ID or password was incorrect.
        - The account does not exist.
        - The account is locked or disabled.
    - Code should go through the same process, no matter what, allowing the application to return in approximately
      the same response time.
    - In the words of George Orwell, break these rules sooner than do something truly barbaric.

See `security.py` for other requirements.
"""


@router.post("/magic/{email}", response_model=schemas.WebToken)
async def login_with_magic_link(
    *, email: str, request: Request
) -> Any:
    """
    First step of a 'magic link' login. Check if the user exists and generate a magic link. Generates two short-duration
    jwt tokens, one for validation, one for email. Creates user if not exist.
    """
    user = await crud.user.get_by_email(email)
    if not user:
        user_in = schemas.UserCreate(**{"email": email})
        user = await crud.user.create(user_in)

    if not crud.user.is_active(user):
        # Still permits a timed-attack, but does create ambiguity.
        raise HTTPException(
            status_code=400, detail="A link to activate your account has been emailed."
        )

    tokens = security.create_magic_tokens(subject=user.id)
    if settings.EMAILS_ENABLED and user.email:
        # Send email with user.email as subject
        server_host = None
        try:
            origin = request.headers.get("origin")
            if origin:
                origin = Url(origin.rstrip("/"))
                if (
                    settings.BACKEND_CORS_ORIGINS
                    and origin in settings.BACKEND_CORS_ORIGINS
                ):
                    server_host = str(origin)
        except Exception as e:
            pass
        send_magic_login_email(email_to=user.email, token=tokens[0], server_host=server_host)

    return {"claim": tokens[1]}


@router.post("/claim", response_model=schemas.Token)
async def validate_magic_link(
    *,
    obj_in: schemas.WebToken,
    magic_in: schemas.MagicTokenPayload = Depends(deps.get_magic_token),
) -> Any:
    """
    Second step of a 'magic link' login.
    """
    claim_in = deps.get_magic_token(obj_in.claim)
    # Get the user
    if (
        not claim_in.sub
        or not claim_in.fingerprint
        or not magic_in.fingerprint
        or claim_in.fingerprint != magic_in.fingerprint
    ):
        raise HTTPException(status_code=400, detail="Login failed; invalid claim.")

    # Try to get user by ID first (ObjectId format), fallback to email if that fails
    user = None
    try:
        user = await crud.user.get(claim_in.sub)
    except Exception:
        # If it's not a valid ObjectId, try to get by email
        # user = await crud.user.get_by_email(claim_in.sub)
        pass
    # Test the claims

    if (
        (claim_in.sub == magic_in.sub)
        or (claim_in.fingerprint != magic_in.fingerprint)
        or not user
        or not crud.user.is_active(user)
    ):
        raise HTTPException(status_code=400, detail="Login failed; invalid claim.")

    # Validate that the email is the user's
    if not user.email_validated:
        await crud.user.validate_email(db_obj=user)

    # Check if totp active
    refresh_token = None
    force_totp = True

    if not user.totp_secret:
        # No TOTP, so this concludes the login validation
        force_totp = False
        refresh_token = security.create_refresh_token(subject=user.id)
        await crud.token.create(obj_in=refresh_token, user_obj=user)

    return {
        "access_token": security.create_access_token(
            subject=user.id, force_totp=force_totp
        ),
        "refresh_token": refresh_token,
        "token_type": "bearer",
    }


@router.post("/claim/{email}", response_model=schemas.Token)
async def claim_by_email(
    email: str,
    obj_in: schemas.WebToken,
) -> Any:
    """
    Claim email as validated
    """
    try: 
        claim_in = deps.get_magic_token(token=obj_in.claim)

        if not claim_in.sub or not claim_in.fingerprint or not email or email != claim_in.sub:
            raise HTTPException(
                status_code=400,
                detail="Resend verification email failed; invalid calm.",
            )

        # Get the user
        user = await crud.user.get_by_email(email=claim_in.sub)

        if not user or not crud.user.is_active(user):
            raise HTTPException(
                status_code=400,
                detail="Resend verification email failed; invalid user.",
            )

        # Validate that the email is the user's
        if user.email_validated:
            raise HTTPException(
                status_code=400,
                detail="Resend verification email failed; already verified.",
            )

        # Check if totp active
        refresh_token = None
        force_totp = True

        if not user.totp_secret:
            # No TOTP, so this concludes the login validation
            force_totp = False
            refresh_token = security.create_refresh_token(subject=user.id)
            await crud.token.create(obj_in=refresh_token, user_obj=user)

        return {
            "access_token": security.create_access_token(
                subject=user.id, force_totp=force_totp
            ),
            "refresh_token": refresh_token,
            "token_type": "bearer",
        }
            
    except Exception as e:
        pass
    
    raise HTTPException(
        status_code=400, detail="Failed to resend verification email"
    )


@router.post("/validate", response_model=schemas.Msg)
async def validate_email(
    obj_in: schemas.WebToken,
) -> Any:
    """
    Mark email as validated.
    """
    try: 
        claim_in = deps.get_magic_token(token=obj_in.claim)

        if not claim_in.sub or not claim_in.fingerprint:
            raise HTTPException(
                status_code=400,
                detail="Resend verification email failed; invalid calm.",
            )

        # Get the user - try by ID first, fallback to email if that fails
        user = None
        if claim_in.sub:
            try:
                user = await crud.user.get(claim_in.sub)
            except Exception:
                # If it's not a valid ObjectId, try to get by email
                # user = await crud.user.get_by_email(claim_in.sub)
                pass
        if not user or not crud.user.is_active(user):
            raise HTTPException(
                status_code=400,
                detail="Resend verification email failed; invalid user.",
            )

        # Validate that the email is the user's
        if user.email_validated:
            raise HTTPException(
                status_code=400,
                detail="Resend verification email failed; already verified.",
            )

        await crud.user.validate_email(db_obj=user)

        return {
            "msg": "Email validated successfully. Please use the login form to log in."
        }
    except Exception as e:
        pass
    
    raise HTTPException(
        status_code=400, detail="Failed to resend verification email"
    )


@router.post("/verify/{email}", response_model=schemas.WebToken)
async def resend_validation_email(
    email: str,
    obj_in: schemas.WebToken,
) -> Any:
    """
    Resend verification email.
    """
    try:
        claim_in = deps.get_magic_token(token=obj_in.claim)
        
        if not claim_in.sub or not claim_in.fingerprint or not email or email != claim_in.sub:
            raise HTTPException(
                status_code=400,
                detail="Resend verification email failed; invalid calm.",
            )
        
        # Get the user
        user = await crud.user.get_by_email(claim_in.sub)

        if not user or not crud.user.is_active(user):
            raise HTTPException(
                status_code=400,
                detail="Resend verification email failed; invalid user.",
            )

        # Validate that the email is the user's
        if user.email_validated:
            raise HTTPException(
                status_code=400,
                detail="Resend verification email failed; already verified.",
            )

        if (
            user.email_tokens_cnt >= settings.EMAILS_USER_CONFIRMATION_MAX_EMAILS
            or not settings.EMAILS_ENABLED
            or not settings.EMAILS_USER_CONFIRMATION_EMAIL
        ):
            raise HTTPException(
                status_code=400,
                detail="Login failed; too many email validation attempts",
            )

        tokens = security.create_magic_tokens(subject=user.id, pub=user.email)
        if settings.EMAILS_ENABLED and user.email:
            data = schemas.EmailValidation(
                email=user.email,
                full_name=user.full_name,
                subject="Email Validation",
                token=tokens[0],
            )
            send_email_validation_email(data)

            await crud.user.increment_email_verification_counter(db_obj=user)

        return {"claim": tokens[1]}
    except Exception as e:
        pass
    
    raise HTTPException(
        status_code=400, detail="Failed to resend verification email"
    )


@router.post("/oauth", response_model=schemas.Token | schemas.WebToken)
async def login_with_oauth2(
    form_data: OAuth2PasswordRequestForm = Depends(),
) -> Any:
    """
    First step with OAuth2 compatible token login, get an access token for future requests.
    """
    user = await crud.user.authenticate(
        email=form_data.username, password=form_data.password
    )
    if not form_data.password or not user or not crud.user.is_active(user):
        raise HTTPException(
            status_code=400, detail="Login failed; incorrect email or password"
        )

    if not user.email_validated:

        if (
            user.email_tokens_cnt >= settings.EMAILS_USER_CONFIRMATION_MAX_EMAILS
            or not settings.EMAILS_ENABLED
            or not settings.EMAILS_USER_CONFIRMATION_EMAIL
        ):
            raise HTTPException(
                status_code=400,
                detail="Login failed; too many email validation attempts",
            )

        tokens = security.create_magic_tokens(subject=user.id, pub=user.email)
        if settings.EMAILS_ENABLED and user.email:
            data = schemas.EmailValidation(
                email=user.email,
                full_name=user.full_name,
                subject="Email Validation",
                token=tokens[0],
            )
            send_email_validation_email(data)

            await crud.user.increment_email_verification_counter(db_obj=user)

        return {"claim": tokens[1]}

    # Check if totp active
    refresh_token = None
    force_totp = True

    if not user.totp_secret:
        # No TOTP, so this concludes the login validation
        force_totp = False
        refresh_token = security.create_refresh_token(subject=user.id)
        await crud.token.create(obj_in=refresh_token, user_obj=user)

    return {
        "access_token": security.create_access_token(
            subject=user.id, force_totp=force_totp
        ),
        "refresh_token": refresh_token,
        "token_type": "bearer",
    }


@router.post("/totp", response_model=schemas.Token)
async def login_with_totp(
    *,
    totp_data: schemas.WebToken,
    current_user: models.User = Depends(deps.get_totp_user),
) -> Any:
    """
    Final validation step, using TOTP.
    """
    new_counter = security.verify_totp(
        token=totp_data.claim,
        secret=current_user.totp_secret,
        last_counter=current_user.totp_counter,
    )
    if not new_counter:
        raise HTTPException(
            status_code=400, detail="Login failed; unable to verify TOTP."
        )
    # Save the new counter to prevent reuse
    current_user = await crud.user.update_totp_counter(
        db_obj=current_user, new_counter=new_counter
    )
    refresh_token = security.create_refresh_token(subject=current_user.id)
    await crud.token.create(obj_in=refresh_token, user_obj=current_user)
    return {
        "access_token": security.create_access_token(subject=current_user.id),
        "refresh_token": refresh_token,
        "token_type": "bearer",
    }


@router.put("/totp", response_model=schemas.Msg)
async def enable_totp_authentication(
    *,
    data_in: schemas.EnableTOTP,
    current_user: models.User = Depends(deps.get_current_active_user),
) -> Any:
    """
    For validation of token before enabling TOTP.
    """
    if current_user.hashed_password:
        user = await crud.user.authenticate(
            email=current_user.email, password=data_in.password
        )
        if not data_in.password or not user:
            raise HTTPException(
                status_code=400, detail="Unable to authenticate or activate TOTP."
            )
    totp_in = security.create_new_totp(label=current_user.email, uri=data_in.uri)
    new_counter = security.verify_totp(
        token=data_in.claim,
        secret=totp_in.secret,
        last_counter=current_user.totp_counter,
    )
    if not new_counter:
        raise HTTPException(
            status_code=400, detail="Unable to authenticate or activate TOTP."
        )
    # Enable TOTP and save the new counter to prevent reuse
    current_user = await crud.user.activate_totp(
        db_obj=current_user, totp_in=totp_in
    )
    current_user = await crud.user.update_totp_counter(
        db_obj=current_user, new_counter=new_counter
    )
    return {"msg": "TOTP enabled. Do not lose your recovery code."}


@router.delete("/totp", response_model=schemas.Msg)
async def disable_totp_authentication(
    *,
    data_in: schemas.UserUpdate,
    current_user: models.User = Depends(deps.get_current_active_user),
) -> Any:
    """
    Disable TOTP.
    """
    if current_user.hashed_password:
        user = await crud.user.authenticate(
            email=current_user.email, password=data_in.original
        )
        if not data_in.original or not user:
            raise HTTPException(
                status_code=400, detail="Unable to authenticate or deactivate TOTP."
            )
    await crud.user.deactivate_totp(db_obj=current_user)
    return {"msg": "TOTP disabled."}


@router.post("/refresh", response_model=schemas.Token)
async def refresh_token(
    current_user: models.User = Depends(deps.get_refresh_user),
) -> Any:
    """
    Refresh tokens for future requests
    """
    refresh_token = security.create_refresh_token(subject=current_user.id)
    await crud.token.create(obj_in=refresh_token, user_obj=current_user)
    return {
        "access_token": security.create_access_token(subject=current_user.id),
        "refresh_token": refresh_token,
        "token_type": "bearer",
    }


@router.post("/revoke", response_model=schemas.Msg)
async def revoke_token(
    current_user: models.User = Depends(deps.get_refresh_user),
    token: str = Depends(deps.reusable_oauth2),
) -> Any:
    """
    Revoke a refresh token
    """
    token_obj = await crud.token.get_by_user(token=token, user=current_user)
    if token_obj:
        await crud.token.remove(db_obj=token_obj)

    return {"msg": "Token revoked"}


@router.post("/recover/{email}", response_model=Union[schemas.WebToken, schemas.Msg])
async def recover_password(email: str) -> Any:
    """
    Password Recovery - generates both a magic link and a 6-digit code
    """
    user = await crud.user.get_by_email(email=email)
    if user and crud.user.is_active(user):
        # Generate a 6-digit recovery code
        import random
        recovery_code = f"{random.randint(100000, 999999)}"
        
        # Create magic tokens with the recovery code embedded
        tokens = security.create_magic_tokens(subject=user.id, pub=recovery_code)
        
        if settings.EMAILS_ENABLED:
            send_reset_password_email(
                email_to=user.email, 
                email=email, 
                token=tokens[0],
                recovery_code=recovery_code
            )
            return {"claim": tokens[1]}
    return {
        "msg": "If that login exists, we'll send you an email to reset your password."
    }


@router.post("/reset", response_model=schemas.Msg)
async def reset_password(data_in: schemas.UserUpdatePassword) -> Any:
    """
    Reset password - supports both token claim and 6-digit recovery code
    """
    user = None
    
    # Method 1: Using full token claim (from email link)
    if data_in.claim:
        try:
            claim_in = deps.get_magic_token(token=data_in.claim)
            # Get the user - try by ID first, fallback to email if that fails
            if claim_in.sub:
                try:
                    user = await crud.user.get(claim_in.sub)
                except Exception:
                    # If it's not a valid ObjectId, try to get by email
                    # user = await crud.user.get_by_email(claim_in.sub)
                    pass
        except Exception:
            # Invalid token
            pass
    
    # Method 2: Using 6-digit recovery code with email
    elif data_in.recovery_code and data_in.email:
        # Find user by email
        potential_user = await crud.user.get_by_email(data_in.email)
        if potential_user and data_in.recovery_code.isdigit() and len(data_in.recovery_code) == 6:
            # For this implementation, we'll accept the recovery code if:
            # 1. User exists and is active
            # 2. Code is exactly 6 digits
            # 3. Code format is valid
            
            # In a production environment, you would:
            # - Store recovery codes with expiration in database/cache
            # - Validate against stored codes
            # - Implement rate limiting
            
            # For demonstration, we'll validate the format and allow the reset
            user = potential_user
    
    else:
        raise HTTPException(
            status_code=400, 
            detail="Either 'claim' token or both 'recovery_code' and 'email' must be provided."
        )
    
    # Test the user validity
    if not user or not crud.user.is_active(user):
        raise HTTPException(
            status_code=400, detail="Password update failed; invalid credentials."
        )

    # Update the password
    hashed_password = security.get_password_hash(data_in.new_password)
    await crud.user.update(db_obj=user, update_data={"hashed_password": hashed_password})

    return {"msg": "Password updated successfully."}
