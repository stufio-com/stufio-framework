from typing import Optional, List, Annotated
from pydantic import BaseModel, Field, EmailStr, StringConstraints, field_validator
from odmantic import ObjectId


class UserLogin(BaseModel):
    username: str
    password: str


# Shared properties
class UserBase(BaseModel):
    email: Optional[EmailStr] = None
    email_validated: Optional[bool] = False
    is_active: Optional[bool] = True
    is_superuser: Optional[bool] = False
    full_name: str = ""
    user_groups: Optional[List[ObjectId]] = []  # Add user_groups here


# Properties to receive via admin API on creation
class UserCreate(UserBase):
    email: EmailStr
    password: Optional[Annotated[str, StringConstraints(min_length=8, max_length=64)]] = None

# Properties to receive via API on creation
class UserCreatePublic(BaseModel):
    email: EmailStr
    password: str
    full_name: str = ""

# Properties to receive via API on update
class UserUpdate(UserBase):
    original: Optional[Annotated[str, StringConstraints(min_length=8, max_length=64)]] = None
    password: Optional[Annotated[str, StringConstraints(min_length=8, max_length=64)]] = None
    user_groups: Optional[List[ObjectId]] = None  # Make it optional


class UserInDBBase(UserBase):
    id: Optional[ObjectId] = None

    class Config:
        from_attributes = True


# Additional properties to return via API
class User(UserInDBBase):
    hashed_password: bool = Field(default=False, alias="password")
    totp_secret: bool = Field(default=False, alias="totp")

    class Config:
        populate_by_name = True

    @field_validator("hashed_password", mode="before")
    def evaluate_hashed_password(cls, hashed_password):
        if hashed_password:
            return True
        return False

    @field_validator("totp_secret", mode="before")
    def evaluate_totp_secret(cls, totp_secret):
        if totp_secret:
            return True
        return False


# Additional properties stored in DB
class UserInDB(UserInDBBase):
    hashed_password: Optional[str] = None
    totp_secret: Optional[str] = None
    totp_counter: Optional[int] = None
    email_tokens_cnt: Optional[int] = None
    user_groups: List[ObjectId] = []  # Include user_groups


class UserUpdatePassword(BaseModel):
    claim: Optional[str] = None  # For full token/link method
    recovery_code: Optional[str] = None  # For 6-digit code method
    email: Optional[EmailStr] = None  # Required when using recovery_code
    new_password: str
