from typing import List
from stufio.core.config import get_settings
from starlette.middleware.cors import CORSMiddleware
from .admin_auth_middleware import AdminAuthMiddleware

def get_framework_middlewares() -> List[tuple]:
    """
    Return a list of framework-level middlewares to be applied before module middlewares.
    
    Returns:
        List of (middleware_class, args, kwargs) tuples
    """
    middlewares = []
    app_settings = get_settings()
    
    # Add CORS middleware if origins are configured
    if app_settings.BACKEND_CORS_ORIGINS:
        middlewares.append((
            CORSMiddleware,
            [],  # Empty args list
            {
                "allow_origins": [
                    str(origin).rstrip("/") for origin in app_settings.BACKEND_CORS_ORIGINS
                ],
                "allow_credentials": app_settings.BACKEND_CORS_ALLOW_CREDENTIALS,
                "allow_methods": app_settings.BACKEND_CORS_ALLOW_METHODS,
                "allow_headers": app_settings.BACKEND_CORS_ALLOW_HEADERS,
                "expose_headers": app_settings.BACKEND_CORS_EXPOSE_HEADERS,
                "max_age": app_settings.BACKEND_MAX_AGE,
            }
        ))
    
    return middlewares
