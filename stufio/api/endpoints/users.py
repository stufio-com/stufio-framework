from typing import Any, List

from fastapi import APIRouter, Body, Depends, HTTPException
from fastapi.encoders import jsonable_encoder
from pydantic.networks import EmailStr
from motor.core import AgnosticDatabase

from stufio import crud, models, schemas
from stufio.api import deps
from stufio.core.config import get_settings
from stufio.core import security
from stufio.utilities import (
    send_new_account_email,
)

settings = get_settings()

router = APIRouter()

@router.post("/", response_model=schemas.User)
async def create_user_profile(
    obj_in: schemas.UserCreatePublic,
) -> Any:
    """
    Create new user without the need to be logged in.
    """
    user = await crud.user.get_by_email(email=obj_in.email)
    if user:
        raise HTTPException(
            status_code=400,
            detail="This username is not available.",
        )
    # Create user auth
    user_in = schemas.UserCreate(
        **obj_in.dict(),
        email_validated=0 if settings.EMAILS_USER_CONFIRMATION_EMAIL else 1,
    )
    user = await crud.user.create(obj_in=user_in)

    if (
        settings.EMAILS_ENABLED
        and settings.EMAILS_USER_CONFIRMATION_EMAIL
        and user.email
    ):
        send_new_account_email(
            email_to=user.email, username=user.email, password=obj_in.password
        )

    return user


@router.put("/", response_model=schemas.User)
async def update_user(
    obj_in: schemas.UserUpdate,
    current_user: models.User = Depends(deps.get_current_active_user),
) -> Any:
    """
    Update user.
    """
    if current_user.hashed_password:
        user = await crud.user.authenticate(email=current_user.email, password=obj_in.original)
        if not obj_in.original or not user:
            raise HTTPException(status_code=400, detail="Unable to authenticate this update.")
    current_user_data = jsonable_encoder(current_user)
    user_in = schemas.UserUpdate(**current_user_data)
    if obj_in.password is not None:
        user_in.password = obj_in.password
    if obj_in.full_name is not None:
        user_in.full_name = obj_in.full_name
    if obj_in.email is not None:
        check_user = await crud.user.get_by_email(email=obj_in.email)
        if check_user and check_user.email != current_user.email:
            raise HTTPException(
                status_code=400,
                detail="This username is not available.",
            )
        user_in.email = obj_in.email
    user = await crud.user.update(db_obj=current_user, update_data=user_in)
    return user


@router.get("/", response_model=schemas.User)
async def read_user(
    *,
    current_user: models.User = Depends(deps.get_current_active_user),
) -> Any:
    """
    Get current user.
    """
    return current_user


@router.post("/new-totp", response_model=schemas.NewTOTP)
async def request_new_totp(
    *,
    current_user: models.User = Depends(deps.get_current_active_user),
) -> Any:
    """
    Request new keys to enable TOTP on the user account.
    """
    obj_in = security.create_new_totp(label=current_user.email)
    # Remove the secret ...
    obj_in.secret = None
    return obj_in
