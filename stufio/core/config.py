import os
import secrets
from typing import Any, Dict, List, Optional, Union

from pydantic import AnyHttpUrl, EmailStr, HttpUrl, field_validator
from pydantic_core.core_schema import ValidationInfo
from pydantic_settings import BaseSettings


class StufioSettings(BaseSettings):
    APP_NAME: str = "app"
    API_V1_STR: str = "/api/v1"
    API_ADMIN_STR: str = "/admin"
    API_DEBUG: bool = False
    API_PROFILE: bool = False
    SECRET_KEY: str = secrets.token_urlsafe(32)
    TOTP_SECRET_KEY: str = secrets.token_urlsafe(32)
    # 60 minutes * 24 hours * 8 days = 8 days
    ACCESS_TOKEN_EXPIRE_SECONDS: int = 60 * 30
    REFRESH_TOKEN_EXPIRE_SECONDS: int = 60 * 60 * 24 * 30
    JWT_ALGO: str = "HS512"
    TOTP_ALGO: str = "SHA-1"
    SERVER_NAME: str
    SERVER_HOST: AnyHttpUrl
    SERVER_BOT: str = "Symona"
    # BACKEND_CORS_ORIGINS is a JSON-formatted list of origins
    # e.g: '["http://localhost", "http://localhost:4200", "http://localhost:3000", \
    # "http://localhost:8080", "http://local.dockertoolbox.tiangolo.com"]'
    BACKEND_CORS_ORIGINS: List[AnyHttpUrl] = []

    @field_validator("BACKEND_CORS_ORIGINS", mode="before")
    def assemble_cors_origins(cls, v: Union[str, List[str]]) -> Union[List[str], str]:
        if isinstance(v, str) and not v.startswith("["):
            return [i.strip() for i in v.split(",")]
        elif isinstance(v, (list, str)):
            return v
        raise ValueError(v)

    PROJECT_NAME: str
    SENTRY_DSN: Optional[HttpUrl] = None

    @field_validator("SENTRY_DSN", mode="before")
    def sentry_dsn_can_be_blank(cls, v: str) -> Optional[str]:
        if isinstance(v, str) and len(v) == 0:
            return None
        return v

    # GENERAL SETTINGS
    MULTI_MAX: int = 20

    # COMPONENT SETTINGS
    MONGO_DATABASE: str
    MONGO_DATABASE_URI: str

    CLICKHOUSE_DSN: str

    CELERY_BROKER_URL: str

    SMTP_TLS: bool = True
    SMTP_PORT: Optional[int] = None
    SMTP_HOST: Optional[str] = None
    SMTP_USER: Optional[str] = None
    SMTP_PASSWORD: Optional[str] = None
    EMAILS_FROM_EMAIL: Optional[EmailStr] = None
    EMAILS_FROM_NAME: Optional[str] = None
    EMAILS_TO_EMAIL: Optional[EmailStr] = None

    @field_validator("EMAILS_FROM_NAME")
    def get_project_name(cls, v: Optional[str], info: ValidationInfo) -> str:
        if not v:
            return info.data["PROJECT_NAME"]
        return v

    EMAIL_RESET_TOKEN_EXPIRE_HOURS: int = 48
    EMAIL_TEMPLATES_DIR: str = "/app/app/email-templates/build"
    EMAILS_ENABLED: bool = True

    EMAILS_USER_CONFIRMATION_EMAIL: bool = True
    EMAILS_USER_CONFIRMATION_MAX_EMAILS: int = 3
    EMAILS_VERIFY_USER_EMAIL: bool = True

    @field_validator("EMAILS_ENABLED", mode="before")
    def get_emails_enabled(cls, v: bool, info: ValidationInfo) -> bool:
        return bool(info.data.get("SMTP_HOST") and info.data.get("SMTP_PORT") and info.data.get("EMAILS_FROM_EMAIL"))

    EMAIL_TEST_USER: EmailStr = "test@example.com"  # type: ignore
    FIRST_SUPERUSER: EmailStr
    FIRST_SUPERUSER_PASSWORD: str

    USERS_OPEN_REGISTRATION: bool = True

    # MODULES SETTINGS
    MODULES_DIR: Optional[str] = None
    
    STUFIO_MODULES_DIR: str = os.path.normpath(
        os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "modules"
        )
    )
    
    # Security and Rate Limiting Settings
    RATE_LIMIT_IP_MAX_REQUESTS: int = 100
    RATE_LIMIT_IP_WINDOW_SECONDS: int = 60

    RATE_LIMIT_USER_MAX_REQUESTS: int = 300
    RATE_LIMIT_USER_WINDOW_SECONDS: int = 60

    SECURITY_MAX_UNIQUE_IPS_PER_DAY: int = 5
