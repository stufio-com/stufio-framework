import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from .module_registry import ModuleRegistry, ModuleInterface

from stufio.core.config import get_settings
import logging


class StufioAPI(FastAPI):

    def __init__(self, *args, **kwargs):
        self.registry = ModuleRegistry()
        self._load_modules()

        # Store lifespan as instance variable instead of global
        self._user_lifespan = kwargs.pop("lifespan", None)

        # Use a method instead of a separate function
        kwargs["lifespan"] = self._create_app_lifespan()

        super().__init__(*args, **kwargs)

        # Get app_settings from kwargs - pass as explicit parameter
        self.app_settings = kwargs.get("app_settings", get_settings())
        self._init_middlewares()
        self._init_logger()

    def _create_app_lifespan(self):
        """Create the application lifespan context manager."""
        @asynccontextmanager
        async def _stufio_app_init(app):
            """Initialize the application with the module registry."""
            # Register all modules with the app
            self.registry.register_all_modules(app)
            tasks = []
            # Call on_startup for all modules
            for module_name, module in self.registry.modules.items():
                try:
                    logging.info(f"Starting module: {module_name}")
                    task = asyncio.create_task(module.on_startup(app))
                    tasks.append(task)
                except Exception as e:
                    logging.error(f"Error starting module {module_name}: {str(e)}", exc_info=True)

            # Handle user-provided lifespan
            if self._user_lifespan:
                # Call the user's lifespan context manager if provided
                async with self._user_lifespan(app):

                    # for task in tasks:
                    #     await task

                    yield
            else:
                # Default lifespan behavior
                yield

            # tasks = []
            # Call on_shutdown for all modules in reverse order
            for module_name, module in reversed(list(self.registry.modules.items())):
                try:
                    logging.info(f"Shutting down module: {module_name}")
                    task = asyncio.create_task(module.on_shutdown(app))
                    tasks.append(task)
                except Exception as e:
                    logging.error(f"Error shutting down module {module_name}: {str(e)}", exc_info=True)

            # Cleanup on shutdown
            self.registry.unregister_all_modules(app)
            
            for task in tasks:
                await task
            logging.info("All modules have been shut down.")
            # Cleanup the registry

        return _stufio_app_init

    def module_registry(self):
        """Returns the module registry instance."""
        return self.registry

    def _load_modules(self):
        """Load all modules in the registry."""
        # Discover and load all modules
        for module_name in self.registry.discover_modules():
            self.registry.load_module(module_name)

    def _init_middlewares(self):
        """Initialize middleware from all modules."""
        # Add middleware from modules - BEFORE APP STARTS
        for middleware_class, args, kwargs in self.registry.get_all_middlewares():
            self.add_middleware(middleware_class, *args, **kwargs)
            logging.info(f"Added middleware: {middleware_class.__name__}")

    def _init_logger(self):
        """Configure application logging."""
        # Configure error logging
        error_logger = logging.getLogger("uvicorn.error")
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter(
                "[%(asctime)s] [%(levelname)s] [%(name)s] [%(funcName)s():%(lineno)s] "
                f"[APP:{self.app_settings.APP_NAME} PID:%(process)d TID:%(thread)d] - %(message)s"
            )
        )
        error_logger.addHandler(handler)


__all__ = ["ModuleRegistry", "ModuleInterface", "StufioAPI"]
