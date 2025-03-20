from typing import Any, List
from fastapi import Depends, HTTPException
from fastapi.routing import APIRouter
from motor.core import AgnosticDatabase

from stufio import crud, schemas
from stufio.api import deps
from stufio.core.config import get_settings
from stufio.core import security
from stufio.utilities import (
    send_new_account_email,
)

settings = get_settings()

router = APIRouter()

@router.get("/all", response_model=List[schemas.User])
async def read_all_users(
    *,
    db: AgnosticDatabase = Depends(deps.get_db),
    skip: int = 0,
    limit: int = 100,
) -> Any:
    """
    Retrieve all current users.
    """
    return await crud.user.get_multi(db=db, skip=skip, limit=limit)


@router.post("/toggle-state", response_model=schemas.Msg)
async def toggle_state(
    *,
    db: AgnosticDatabase = Depends(deps.get_db),
    user_in: schemas.UserUpdate,
) -> Any:
    """
    Toggle user state (moderator function)
    """
    response = await crud.user.toggle_user_state(db=db, obj_in=user_in)
    if not response:
        raise HTTPException(
            status_code=400,
            detail="Invalid request.",
        )
    return {"msg": "User state toggled successfully."}


@router.post("/", response_model=schemas.User)
async def create_user(
    *,
    db: AgnosticDatabase = Depends(deps.get_db),
    user_in: schemas.UserCreate,
) -> Any:
    """
    Create new user (moderator function).
    """
    user = await crud.user.get_by_email(db, email=user_in.email)
    if user:
        raise HTTPException(
            status_code=400,
            detail="The user with this username already exists in the system.",
        )
    user = await crud.user.create(db, obj_in=user_in)
    if settings.EMAILS_ENABLED and user_in.email:
        send_new_account_email(email_to=user_in.email, username=user_in.email, password=user_in.password)
    return user


@router.put("/{user_id}", response_model=schemas.User)
async def update_user(
    *,
    db: AgnosticDatabase = Depends(deps.get_db),
    user_in: schemas.UserUpdate,
) -> Any:
    """
    Update user (moderator function).
    """
    user = await crud.user.get(db, user_in.id)
    if not user:
        raise HTTPException(
            status_code=404,
            detail="User not found.",
        )
    user = await crud.user.update(db, db_obj=user, obj_in=user_in)
    return user


@router.get("/{user_id}", response_model=schemas.User)
async def read_user(
    user_id: str,
    *,
    db: AgnosticDatabase = Depends(deps.get_db),
) -> Any:
    """
    Get current user(moderator function).
    """
    user = await crud.user.get(db, user_id)
    return user
