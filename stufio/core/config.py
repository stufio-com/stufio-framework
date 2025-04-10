import os
import secrets
from typing import Any, Dict, List, Optional, Union, ClassVar, Type
from pydantic import AnyHttpUrl, ConfigDict, EmailStr, HttpUrl, field_validator, create_model
from pydantic_core.core_schema import ValidationInfo
from pydantic_settings import BaseSettings
from .settings import BaseStufioSettings, ModuleSettings
from .setting_registry import SettingMetadata, GroupMetadata, SubgroupMetadata, SettingType, settings_registry

class StufioSettings(BaseStufioSettings):
    APP_NAME: str = "app"
    API_V1_STR: str = "/api/v1"
    API_ADMIN_STR: str = "/admin"
    API_DEBUG: bool = False
    API_PROFILE: bool = False
    SECRET_KEY: str = secrets.token_urlsafe(32)
    TOTP_SECRET_KEY: str = secrets.token_urlsafe(32)
    ACCESS_TOKEN_EXPIRE_SECONDS: int = 60 * 10 # 10 minutes
    REFRESH_TOKEN_EXPIRE_SECONDS: int = 60 * 60 * 24 * 1 # 1 day
    JWT_ALGO: str = "HS512"
    TOTP_ALGO: str = "SHA-1"
    SERVER_NAME: str
    SERVER_HOST: AnyHttpUrl
    SERVER_BOT: str = "Symona"
    # BACKEND_CORS_ORIGINS is a JSON-formatted list of origins
    # e.g: '["http://localhost", "http://localhost:4200", "http://localhost:3000", \
    # "http://localhost:8080", "http://local.dockertoolbox.tiangolo.com"]'
    BACKEND_CORS_ORIGINS: List[AnyHttpUrl] = []
    BACKEND_CORS_ALLOW_CREDENTIALS: bool = True
    BACKEND_CORS_ALLOW_METHODS: List[str] = ["*"]
    BACKEND_CORS_ALLOW_HEADERS: List[str] = ["*"]
    BACKEND_CORS_EXPOSE_HEADERS: List[str] = []
    BACKEND_MAX_AGE: int = 3600

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

    # Redis settings
    REDIS_URL: str = "redis://localhost:6379/0"
    REDIS_PREFIX: str = "stufio:"

    # Add new API security settings
    API_SECRET: str = secrets.token_urlsafe(32)
    API_INTERNAL_CLIENTS: List[str] = ["stufio-admin", "stufio-cron"]
    API_CLIENT_VALIDATION: bool = True
    
    # Add a setting for internal endpoints prefix
    API_INTERNAL_STR: str = "/internal"
    
    # Validator to handle API_INTERNAL_CLIENTS as comma-separated string
    @field_validator("API_INTERNAL_CLIENTS", mode="before")
    def assemble_api_clients(cls, v: Union[str, List[str]]) -> List[str]:
        if isinstance(v, str) and not v.startswith("["):
            return [i.strip() for i in v.split(",")]
        elif isinstance(v, list):
            return v
        return ["stufio-admin", "stufio-cron"]  # Default if invalid


# Default settings instance
settings = StufioSettings()

# Registry for the active settings instance
_active_settings = settings

def configure_settings(settings_instance):
    """
    Configure the framework to use a custom settings instance.
    This should be called early in your application's startup.
    """
    global _active_settings
    _active_settings = settings_instance
    return _active_settings


def get_settings():
    """Get the active settings, possibly from cache"""
    return _active_settings


# Register metadata for settings
settings_registry.register_group(
    GroupMetadata(id="general", label="General Settings", order=10)
)
settings_registry.register_group(
    GroupMetadata(id="api", label="API Settings", order=50)
)

settings_registry.register_subgroup(
    SubgroupMetadata(id="debugging", group_id="api", label="Debugging", order=200),
)

settings_registry.register_setting(
    SettingMetadata(
        key="API_DEBUG",
        label="Enable Debug Mode",
        description="Enable debug output in API responses",
        group="api",
        subgroup="debugging",
        type=SettingType.BOOLEAN,
        component="switch",
        order=10
    )
)

settings_registry.register_setting(
    SettingMetadata(
        key="API_PROFILE",
        label="Enable Profiling",
        description="Enable profiling of API requests",
        group="api",
        subgroup="debugging",
        type=SettingType.BOOLEAN,
        component="switch",
        order=20
    )
)
