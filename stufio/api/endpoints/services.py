from typing import Any

from fastapi import APIRouter

from stufio import schemas
from stufio.utilities import send_web_contact_email
from stufio.schemas import EmailContent

router = APIRouter()

@router.post("/contact", response_model=schemas.Msg, status_code=201)
def send_email(*, data: EmailContent) -> Any:
    """
    Standard app contact us.
    """
    send_web_contact_email(data=data)
    return {"msg": "Web contact email sent"}
