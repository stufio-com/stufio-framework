import os
import secrets
from typing import Any, Dict, List, Optional, Union
from pydantic import AnyHttpUrl, ConfigDict, EmailStr, HttpUrl, field_validator
from pydantic_core.core_schema import ValidationInfo
from .settings import BaseStufioSettings
from .setting_registry import SettingMetadata, GroupMetadata, SubgroupMetadata, SettingType, settings_registry
from urllib.parse import urlparse  # Add this import at the top

class StufioSettings(BaseStufioSettings):
    APP_NAME: str = "app"
    API_V1_STR: str = "/api/v1"
    API_ADMIN_STR: str = "/admin"
    WS_PREFIX: str = "/ws"
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

    APP_REGION: str = ""

    MULTI_MAX: int = 20

    # Database metrics settings
    DB_METRICS_ENABLE: bool = True
    DB_METRICS_REPORT_INTERVAL_SECONDS: int = 300  # Report every 5 minutes

    # MONGO DB SETTINGS
    MONGO_DATABASE: str
    MONGO_DATABASE_URI: str

    @field_validator("MONGO_DATABASE_URI", mode="before")
    def optimize_mongo_uri_for_region(cls, v: str, info: ValidationInfo) -> str:
        """
        Optimizes MongoDB connection string by placing region-specific nodes first.
        If using replica set with readPreference=nearest and APP_REGION is defined,
        nodes ending with -${APP_REGION} will be prioritized.
        """
        if not v or not isinstance(v, str):
            return v

        # Get APP_REGION from settings
        app_region = info.data.get("APP_REGION")
        if not app_region:
            return v  # No region specified, return original

        # Check if this is a replica set connection with multiple hosts
        if not ("," in v and "replicaSet" in v and "readPreference=nearest" in v):
            return v  # Not a replica set with nearest read preference

        try:
            # Parse MongoDB URI
            protocol_part, rest = v.split("://", 1)

            # Split hosts from options
            hosts_part, options_part = rest.split("/", 1) if "/" in rest else (rest, "")

            # Get list of hosts
            hosts = hosts_part.split(",")

            # Find region-specific hosts
            region_hosts = [h for h in hosts if f"-{app_region}" in h.lower()]
            other_hosts = [h for h in hosts if f"-{app_region}" not in h.lower()]

            # If we found region-specific hosts, reorder them
            if region_hosts:
                # Reorder: put region-specific hosts first
                new_hosts = region_hosts + other_hosts

                # Reconstruct the connection string
                new_hosts_part = ",".join(new_hosts)

                # Rebuild the full connection string
                if options_part:
                    return f"{protocol_part}://{new_hosts_part}/{options_part}"
                else:
                    return f"{protocol_part}://{new_hosts_part}"

            # If no region-specific hosts found, return original string
            return v

        except Exception:
            # If any parsing error occurs, return original string
            return v

    # CLICKHOUSE SETTINGS
    CLICKHOUSE_DSN: str
    CLICKHOUSE_CLUSTER_DSN_LIST: Optional[List[str]] = None
    CLICKHOUSE_CLUSTER_NAME: Optional[str] = None
    CLICKHOUSE_CLUSTER_CONN_OPTIMIZE: bool = True
    CLICKHOUSE_CONN_OPTIMIZE_MODE: str = "adaptive"  # "adaptive", "periodic", "once"
    CLICKHOUSE_POOL_SIZE: int = 20  # Connection pool size (default 20, increase if you have many concurrent requests)
    CLICKHOUSE_POOL_TIMEOUT: int = 30  # Max seconds to wait for available connection
    CLICKHOUSE_READ_ONLY: int = 0  # 0 = read/write, 1 = read-only mode
    CLICKHOUSE_MAX_THREADS: int = 4  # Maximum threads for query execution (1-8)

    @field_validator("CLICKHOUSE_CLUSTER_NAME", mode="before")
    def validate_cluster_name(cls, v: Optional[str], info: ValidationInfo) -> Optional[str]:
        """
        Validates that CLICKHOUSE_CLUSTER_NAME is provided when CLICKHOUSE_CLUSTER_DSN_LIST is used.
        """
        cluster_dsn_list = info.data.get("CLICKHOUSE_CLUSTER_DSN_LIST")

        # If cluster DSN list is provided, cluster name is required
        if cluster_dsn_list and len(cluster_dsn_list) > 1:
            if not v or not isinstance(v, str) or not v.strip():
                raise ValueError("CLICKHOUSE_CLUSTER_NAME is required when CLICKHOUSE_CLUSTER_DSN_LIST is provided")

        return v.strip() if v else None

    @field_validator("CLICKHOUSE_CLUSTER_DSN_LIST", mode="before")
    def optimize_clickhouse_dsn_list_for_region(cls, v: Optional[List[str]], info: ValidationInfo) -> List[str]:
        """
        Sorts ClickHouse DSN list to prioritize region-specific nodes.
        If APP_REGION is defined, DSNs containing -${APP_REGION} in hostname will be placed first.
        """
        # If no DSN list provided, default to a list with just the main DSN
        if not v:
            return [info.data["CLICKHOUSE_DSN"]]

        # If APP_REGION isn't set, return the original list
        app_region = info.data.get("APP_REGION")
        if not app_region:
            return v

        try:
            # Parse each DSN to check for region in hostname
            region_dsns = []
            other_dsns = []

            for dsn in v:
                # Skip empty DSNs
                if not dsn or not isinstance(dsn, str):
                    continue

                # Parse the DSN
                parsed = urlparse(dsn)

                # Check if hostname contains region
                if f"-{app_region}" in parsed.netloc.lower():
                    region_dsns.append(dsn)
                else:
                    other_dsns.append(dsn)

            # Return region-specific DSNs first, followed by others
            return region_dsns + other_dsns

        except Exception:
            # If parsing failed, return the original list
            return v

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
    REDIS_CLUSTER_NODES: Optional[str] = None

    # Add new API security settings
    API_SECRET: str = secrets.token_urlsafe(32)
    API_INTERNAL_CLIENTS: List[str] = ["stufio-admin", "stufio-cron"]
    API_CLIENT_VALIDATION: bool = True

    # Add a setting for internal endpoints prefix
    API_INTERNAL_STR: str = "/internal"

    # OAuth Settings
    GOOGLE_CLIENT_ID: Optional[str] = None
    GOOGLE_CLIENT_SECRET: Optional[str] = None
    APPLE_CLIENT_ID: Optional[str] = None
    APPLE_TEAM_ID: Optional[str] = None
    APPLE_KEY_ID: Optional[str] = None
    APPLE_PRIVATE_KEY_PATH: Optional[str] = None
    APPLE_REDIRECT_URI: str = "http://localhost:3000/auth/callback/apple"

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
settings_registry.register_group(
    GroupMetadata(id="database", label="Database Settings", order=60)
)

settings_registry.register_subgroup(
    SubgroupMetadata(id="debugging", group_id="api", label="Debugging", order=200),
)
settings_registry.register_subgroup(
    SubgroupMetadata(id="metrics", group_id="database", label="Metrics", order=100),
)
settings_registry.register_subgroup(
    SubgroupMetadata(id="clickhouse", group_id="database", label="ClickHouse", order=200),
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

settings_registry.register_setting(
    SettingMetadata(
        key="DB_METRICS_ENABLE",
        label="Enable Database Metrics",
        description="Enable collection and reporting of database performance metrics",
        group="database",
        subgroup="metrics",
        type=SettingType.BOOLEAN,
        component="switch",
        order=10
    )
)

settings_registry.register_setting(
    SettingMetadata(
        key="DB_METRICS_REPORT_INTERVAL_SECONDS",
        label="Metrics Reporting Interval",
        description="How often (in seconds) to report database metrics in logs",
        group="database",
        subgroup="metrics", 
        type=SettingType.NUMBER,
        component="number",
        order=20
    )
)

settings_registry.register_setting(
    SettingMetadata(
        key="CLICKHOUSE_CLUSTER_NAME",
        label="ClickHouse Cluster Name",
        description="Name of the ClickHouse cluster (required when using cluster mode)",
        group="database",
        subgroup="clickhouse",
        type=SettingType.STRING,
        component="input",
        order=10
    )
)

settings_registry.register_setting(
    SettingMetadata(
        key="CLICKHOUSE_CLUSTER_CONN_OPTIMIZE",
        label="Enable Connection Optimization",
        description="Automatically optimize ClickHouse connections by periodically checking for local nodes and reconnecting when needed",
        group="database",
        subgroup="clickhouse",
        type=SettingType.BOOLEAN,
        component="switch",
        order=20
    )
)

settings_registry.register_setting(
    SettingMetadata(
        key="CLICKHOUSE_CONN_OPTIMIZE_MODE",
        label="Optimization Mode",
        description="How often to check for optimization: 'adaptive' (smart intervals), 'periodic' (fixed intervals), 'once' (single attempt only)",
        group="database",
        subgroup="clickhouse",
        type=SettingType.STRING,
        component="select",
        order=30
    )
)

settings_registry.register_setting(
    SettingMetadata(
        key="CLICKHOUSE_POOL_SIZE",
        label="Connection Pool Size",
        description="Maximum number of connections in the ClickHouse connection pool (default: 20)",
        group="database",
        subgroup="clickhouse",
        type=SettingType.NUMBER,
        component="number",
        order=40
    )
)

settings_registry.register_setting(
    SettingMetadata(
        key="CLICKHOUSE_READ_ONLY",
        label="Read-Only Mode",
        description="Set ClickHouse connection to read-only mode: 0 = read/write, 1 = read-only",
        group="database",
        subgroup="clickhouse",
        type=SettingType.NUMBER,
        component="number",
        order=50
    )
)

settings_registry.register_setting(
    SettingMetadata(
        key="CLICKHOUSE_MAX_THREADS",
        label="Max Query Threads",
        description="Maximum number of threads for ClickHouse query execution (1-8)",
        group="database",
        subgroup="clickhouse",
        type=SettingType.NUMBER,
        component="number",
        order=60
    )
)
