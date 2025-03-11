import importlib
import inspect
import logging
import os
from typing import Dict, List, Optional, Any
from fastapi import FastAPI

# Remove the external app dependency
from stufio.core.config import get_settings
from stufio.core.migrations.manager import migration_manager
import traceback

settings = get_settings()
logger = logging.getLogger(__name__)

class ModuleRegistry:
    """Registry for all modules in the application."""

    def __init__(self):
        self.modules: Dict[str, "ModuleInterface"] = {}
        self.module_paths: Dict[str, str] = {}  # Store module paths by name
        self.router_prefix = getattr(settings, "API_V1_STR", "/api/v1")

    def discover_modules(self) -> List[str]:
        """
        Discover available modules in the modules directory.
        Returns a list of module names.
        """
        module_dirs = []
        self.module_paths = {}  # Reset module paths

        # Use framework's default module dir
        stufio_modules_dir = getattr(settings, "STUFIO_MODULES_DIR", 
                                   os.path.join(os.path.dirname(__file__), "..", "modules"))
        module_dirs.append(stufio_modules_dir)

        # Add user-specified module directories
        user_modules_dir = getattr(settings, "MODULES_DIR", None)
        if user_modules_dir:
            if isinstance(user_modules_dir, list):
                module_dirs.extend(user_modules_dir)
            else:
                module_dirs.append(user_modules_dir)

        logger.info(f"Discovering modules in directories: {', '.join(module_dirs)}")

        discovered_modules = []

        for modules_dir in module_dirs:
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

            # Get module version
            version = getattr(module, "__version__", "0.0.0")
            if hasattr(module, "__version__") and isinstance(module.__version__, str):
                version = module.__version__

            # Ensure migrations path exists - create if needed
            if discover_migrations:
                # Discover migrations for this module
                migration_manager.discover_module_migrations(module_path, module_name, version)

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

    def register_all_modules(self, app: FastAPI) -> None:
        """
        Register all modules with the FastAPI app.
        Note: This only registers routes, NOT middleware.
        """
        for module_name, module in self.modules.items():
            try:
                # Only register routes, middleware should be added separately
                module.register_routes(app)
                logger.info(f"Registered module: {module_name}")
            except Exception as e:
                logger.error(f"Failed to register module {module_name}: {e}")

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
        """Get all middlewares from all modules."""
        middlewares = []
        for module_name, module in self.modules.items():
            try:
                module_middlewares = module.get_middlewares()
                middlewares.extend(module_middlewares)
            except Exception as e:
                logger.error(f"Failed to get middlewares from module {module_name}: {e}")
        return middlewares

    def register_module_routes(self, app: FastAPI) -> None:
        """Register routes from all modules."""
        for module_name, module in self.modules.items():
            try:
                module.register_routes(app)
                logger.info(f"Registered routes for module: {module_name}")
            except Exception as e:
                logger.error(f"Failed to register routes for module {module_name}: {e}")


class ModuleInterface:
    """Interface that all modules must implement for registration."""

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
