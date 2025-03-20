import importlib
import inspect
import logging
import os
import pkg_resources
from typing import Dict, List, Optional, Any
from fastapi import FastAPI

# Remove the external app dependency
from stufio.core.config import get_settings
from stufio.core.migrations.manager import migration_manager
from stufio.api.admin import admin_router, internal_router
import traceback

logger = logging.getLogger(__name__)

class ModuleRegistry:
    """Registry for all modules in the application."""

    def __init__(self):
        self.modules: Dict[str, "ModuleInterface"] = {}
        self.module_paths: Dict[str, str] = {}  # Store module paths by name
        settings = get_settings()
        self.router_prefix = getattr(settings, "API_V1_STR", "/api/v1")

    def discover_modules(self) -> List[str]:
        """
        Discover available modules by checking installed packages 
        that start with "stufio.modules.".
        Returns a list of module names.
        """
        self.module_paths = {}  # Reset module paths
        discovered_modules = []
        settings = get_settings()

        try:
            # Look for installed packages that start with stufio.modules.
            logger.info("Discovering modules from installed packages...")

            module_prefix = "stufio.modules."
            for dist in pkg_resources.working_set:
                # Check both top-level packages and namespace packages
                if dist.key.startswith("stufio-modules-") or dist.key.startswith("stufio.modules."):
                    # Convert package name from stufio-modules-name to name
                    module_name = (
                        dist.key.replace("stufio-modules-", "")
                        .replace("stufio.modules.", "")
                        .replace("-", "_")
                    )
                    logger.debug(f"Found module package: {dist.key} -> {module_name}")

                    # Try to import the module to make sure it exists
                    try:
                        module_path = f"{module_prefix}{module_name}"
                        module = importlib.import_module(module_path)

                        if hasattr(module, "__file__"):
                            module_dir = os.path.dirname(module.__file__)
                            discovered_modules.append(module_name)
                            self.module_paths[module_name] = module_dir
                            logger.debug(f"Successfully imported module {module_name} from {module_dir}")
                        else:
                            logger.warning(f"Module {module_name} has no __file__ attribute")
                    except ImportError as e:
                        logger.warning(f"Could not import {module_path}: {e}")

            # Also try direct imports of modules under stufio.modules
            try:
                # Try to import stufio.modules to check for direct submodules
                import stufio.modules

                if hasattr(stufio.modules, "__path__"):
                    # Find local modules in the filesystem
                    modules_dir = getattr(settings, "STUFIO_MODULES_DIR", 
                                       os.path.join(os.path.dirname(__file__), "..", "modules"))

                    if os.path.exists(modules_dir):
                        logger.info(f"Looking for local modules in {modules_dir}")
                        for item in os.listdir(modules_dir):
                            module_path = os.path.join(modules_dir, item)
                            if (os.path.isdir(module_path) and 
                                os.path.exists(os.path.join(module_path, "__init__.py")) and
                                item != "__pycache__"):

                                # Try to import to verify it works
                                try:
                                    module = importlib.import_module(f"stufio.modules.{item}")
                                    if item not in discovered_modules:
                                        discovered_modules.append(item)
                                        self.module_paths[item] = module_path
                                        logger.debug(f"Found local module: {item} at {module_path}")
                                except ImportError:
                                    logger.warning(f"Could not import local module: stufio.modules.{item}")
            except ImportError:
                logger.warning("Could not import stufio.modules package")

        except Exception as e:
            logger.error(f"Error discovering modules: {str(e)}")
            traceback.print_exc()

        # CHECK FOR LOCAL MODULES
        module_dirs = getattr(settings, "MODULES_DIR", [])
        if module_dirs and isinstance(module_dirs, str):
            module_dirs = [module_dirs]
        for modules_dir in module_dirs if isinstance(module_dirs, list) else []:
            logger.debug(f"Discovering modules in folder: {modules_dir}")

            # Ensure modules directory exists
            if not os.path.exists(modules_dir):
                logger.error(f"Modules directory not found: {modules_dir}")
                continue

            # Find all Python packages (directories with __init__.py)
            for item in os.listdir(modules_dir):
                module_path = os.path.join(modules_dir, item)
                if (os.path.isdir(module_path) and 
                    os.path.exists(os.path.join(module_path, "__init__.py")) and
                    item != "__pycache__"):
                    discovered_modules.append(item)
                    # Store module path for later use
                    self.module_paths[item] = module_path

        # Add user-specified modules from settings if any
        user_modules = getattr(settings, "ADDITIONAL_MODULES", [])
        if user_modules and isinstance(user_modules, list):
            for full_module_name in user_modules:
                module_name = full_module_name.split(".")[-1]
                if module_name not in discovered_modules:
                    try:
                        # Try to import the module to verify it exists
                        module = importlib.import_module(full_module_name)
                        if hasattr(module, "__file__"):
                            module_dir = os.path.dirname(module.__file__)
                            discovered_modules.append(module_name)
                            self.module_paths[module_name] = module_dir
                    except ImportError:
                        logger.error(
                            f"Could not import additional module: {full_module_name}"
                        )

        if discovered_modules:
            logger.info(f"Discovered modules: {', '.join(discovered_modules)}")
        else:
            logger.warning("No modules found")

        return discovered_modules

    def load_module(self, module_name: str, discover_migrations: bool = False) -> Optional["ModuleInterface"]:
        """Load a module by name and return its ModuleInterface implementation."""
        try:
            logger.debug(f"Loading module: {module_name}")

            # Get module path from the cached paths
            if module_name not in self.module_paths:
                logger.error(f"Module path for {module_name} not found. Did you run discover_modules first?")
                return None

            module_path = self.module_paths[module_name]

            # Determine the import path dynamically based on directory structure
            # Split the path into parts and get the last 3 components (parent, parent, module_name)
            path_parts = os.path.normpath(module_path).split(os.sep)

            # Build import path based on actual directory structure
            if len(path_parts) >= 3:
                # Use the last 3 parts of the path to create the import path
                # e.g., .../stufio/modules/activity becomes stufio.modules.activity
                parent2 = path_parts[-3]
                parent1 = path_parts[-2]
                import_path = f"{parent2}.{parent1}.{module_name}"
            else:
                # Fallback to the current structure if we can't determine parent packages
                import_path = f"stufio.modules.{module_name}"

            logger.debug(f"Importing module from path: {import_path}")

            # Import the module using the dynamically generated path
            module = importlib.import_module(import_path)

            # Import module's config if it exists
            try:
                importlib.import_module(f"{import_path}.config")
            except ImportError:
                logger.debug(f"No config module found for {module_name}")
            except Exception as e:
                logger.warning(f"Error importing config for {module_name}: {e}")

            # Get module version
            version = getattr(module, "__version__", "0.0.0")
            if hasattr(module, "__version__") and isinstance(module.__version__, str):
                version = module.__version__

            # Ensure migrations path exists - create if needed
            if discover_migrations:
                # Discover migrations for this module - pass the import path
                migration_manager.discover_module_migrations(
                    module_path=module_path, 
                    module_name=module_name, 
                    module_version=version,
                    module_import_path=import_path  # Pass the already calculated import path
                )

            # Look for ModuleInterface implementation
            for name, obj in inspect.getmembers(module):
                if (inspect.isclass(obj) and 
                    name != "ModuleInterface" and 
                    hasattr(obj, "register") and
                    hasattr(obj, "get_models")):

                    # Create an instance of the module interface
                    instance = obj()
                    self.modules[module_name] = instance
                    logger.info(f"Successfully loaded module: {module_name} (v{version})")
                    return instance

            logger.warning(f"Module {module_name} does not implement ModuleInterface")
            return None

        except Exception as e:
            logger.error(f"Failed to load module {module_name}: {str(e)}")
            traceback.print_exc()
            return None

    def get_all_models(self) -> List[Any]:
        """Get all database models from all registered modules."""
        all_models = []

        for name, module in self.modules.items():
            try:
                models = module.get_models()
                all_models.extend(models)
                logger.debug(f"Loaded {len(models)} models from module: {name}")
            except Exception as e:
                logger.error(f"Failed to get models from module {name}: {str(e)}")

        return all_models

    def get_all_middlewares(self) -> List[tuple]:
        """Get all framework and module middlewares."""
        middlewares = []

        # First, add framework middlewares
        try:
            # Import framework middlewares
            from stufio.middleware.framework import get_framework_middlewares
            framework_middlewares = get_framework_middlewares()
            middlewares.extend(framework_middlewares)
            logger.info(f"Added {len(framework_middlewares)} framework middlewares")
        except ImportError:
            logger.debug("No framework middlewares found")

        # Then, add module middlewares
        for module_name, module in self.modules.items():
            try:
                module_middlewares = module.get_middlewares()
                middlewares.extend(module_middlewares)
                logger.info(f"Added {len(module_middlewares)} middlewares from module {module_name}")
            except Exception as e:
                logger.error(f"Failed to get middlewares from module {module_name}: {e}")

        return middlewares

    def register_all_modules(self, app: FastAPI) -> None:
        """Register all modules."""

        # Register stufio core routes
        from stufio.api.endpoints import api_router
        app.include_router(api_router, prefix=self.router_prefix)

        for module_name, module in self.modules.items():
            try:
                module.register(app)
                logger.info(f"Registered routes for module: {module_name}")
            except Exception as e:
                logger.error(f"Failed to register routes for module {module_name}: {e}")

        # Register admin routes
        app.include_router(admin_router, prefix=get_settings().API_V1_STR)
        # Register internal routes
        app.include_router(internal_router, prefix=get_settings().API_V1_STR)


class ModuleInterface:
    """Interface that all modules must implement for registration."""
    
    __version__ = "0.0.0"
    # Change from double underscore to single underscore to avoid name mangling
    _routes_prefix = get_settings().API_V1_STR

    def register_routes(self, app: FastAPI) -> None:
        """Register routes with the FastAPI application."""
        pass

    def get_middlewares(self) -> List[tuple]:
        """Return middleware classes that should be added to the app.
        
        Returns:
            List of (middleware_class, args, kwargs) tuples
        """
        return []

    def register(self, app: FastAPI) -> None:
        """Legacy method for backwards compatibility."""
        self.register_routes(app)

    def get_models(self) -> List[Any]:
        """Return a list of database models defined by this module."""
        return []


# Singleton instance
registry = ModuleRegistry()
