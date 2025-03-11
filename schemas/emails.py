from pydantic import BaseModel, EmailStr


class EmailContent(BaseModel):
    email: EmailStr
    full_name: str
    subject: str
    content: str


class EmailValidation(BaseModel):
    email: EmailStr
    full_name: str
    subject: str
    token: str
