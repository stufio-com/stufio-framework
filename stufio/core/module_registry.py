import importlib
import inspect
import logging
import os
import pkgutil
from typing import Dict, List, Optional, Tuple
from fastapi import FastAPI
from pathlib import Path

# Remove the external app dependency
from stufio.core.config import get_settings
from stufio.core.migrations.manager import migration_manager
from stufio.api.admin import admin_router, internal_router
import traceback

logger = logging.getLogger(__name__)


class ModuleRegistry:
    """Registry for all modules in the application."""

    def __init__(self):
        self.modules: Dict[str, "ModuleInterface"] = {}  # Module instances
        self.module_infos: Dict[str, ModuleInfo] = {}  # Module information objects
        settings = get_settings()
        self.router_prefix = getattr(settings, "API_V1_STR", "/api/v1")

    def get_module_instance(self, module_name: str) -> Optional["ModuleInterface"]:
        """Get the module instance by name."""
        if module_name in self.modules:
            return self.modules[module_name]
        else:
            logger.warning(f"Module {module_name} not found in registry")
            return None

    def discovered_modules(self) -> Dict[str, str]:
        """Return the list of discovered modules with path."""
        # Use the ModuleInfo.get_filesystem_path() method
        module_dirs = {}
        for name, info in self.module_infos.items():
            module_dir = info.get_filesystem_path()
            if module_dir:
                module_dirs[name] = module_dir
        return module_dirs

    def discover_modules(self) -> List[str]:
        """
        Discover available modules using the ModuleDiscoverer.
        Returns a list of module names.
        """
        settings = get_settings()

        # Configure the module discoverer
        discoverer = ModuleDiscoverer(
            # Handle MODULES_DIR, which can be a string or list
            app_modules_dir=(settings.MODULES_DIR[0] if isinstance(settings.MODULES_DIR, list) and
                             len(settings.MODULES_DIR) > 0 else settings.MODULES_DIR or 
                             ModuleDiscoverer.DEFAULT_APP_MODULES_DIR),
            # Use app modules import path
            app_modules_base_import_path="app.modules",
            # Standard package prefix
            package_prefix="stufio.modules.",
            # Include any explicit modules from settings
            explicit_modules=getattr(settings, "ADDITIONAL_MODULES", [])
        )

        # Perform discovery using the discoverer
        self.module_infos = discoverer.discover()
        logger.info(f"Discovered {len(self.module_infos)} modules")

        # Return list of discovered module names
        discovered_modules = list(self.module_infos.keys())

        if discovered_modules:
            logger.info(f"Discovered modules: {', '.join(discovered_modules)}")
        else:
            logger.warning("No modules found")

        return discovered_modules

    def load_module(self, module_name: str, discover_migrations: bool = False) -> Optional["ModuleInterface"]:
        """Load a module by name and return its ModuleInterface implementation."""
        try:
            logger.debug(f"Loading module: {module_name}")

            # Get ModuleInfo for this module
            if module_name not in self.module_infos:
                logger.error(f"Module info for {module_name} not found. Did you run discover_modules first?")
                return None

            module_info = self.module_infos[module_name]

            # Get module directory using ModuleInfo method
            module_dir = module_info.get_filesystem_path()
            if not module_dir:
                logger.error(f"Could not determine directory for module {module_name}")
                return None

            # Get module using ModuleInfo
            try:
                module = module_info.get_module()
            except Exception as e:
                logger.error(
                    f"❌ Failed to import module {module_name} from {module_info.path}: {e}"
                )
                return None

            # Get module version
            version = getattr(module, "version", "0.0.0")
            if hasattr(module, "version") and isinstance(module.version, str):
                version = module.version

            # Discover migrations if requested
            if discover_migrations:
                migration_manager.discover_module_migrations(
                    module_path=module_dir,
                    module_name=module_name, 
                    module_version=version,
                    module_import_path=module_info.path
                )

            # Find ModuleInterface implementation
            logger.debug(f"Looking for ModuleInterface implementation in {module_name}")
            for name, obj in inspect.getmembers(module):
                if inspect.isclass(obj) and issubclass(obj, ModuleInterface) and obj != ModuleInterface:
                    # Create an instance of the module interface WITH ModuleInfo
                    instance = obj(module_info=module_info)
                    instance.name = module_name
                    instance.version = version

                    self.modules[module_name] = instance
                    logger.info(f"Successfully loaded module: {module_name} (v{version})")
                    return instance

            logger.warning(f"Module {module_name} does not implement ModuleInterface")
            return None

        except Exception as e:
            logger.error(f"❌ Failed to load module {module_name}: {str(e)}")
            traceback.print_exc()
            return None

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
        """Register all modules with better error handling."""
        try:
            from stufio.api.endpoints import api_router
            app.include_router(api_router, prefix=self.router_prefix)
            logger.info("Registered core API routes")
        except Exception as e:
            logger.error(f"Failed to register core API routes: {str(e)}", exc_info=True)

        # Register each module - no longer passing module_dir
        for module_name, module in self.modules.items():
            try:
                module.register(app)
                logger.info(f"Registered module: {module_name}")
            except Exception as e:
                logger.error(f"Failed to register module {module_name}: {str(e)}", exc_info=True)

        # Register admin/internal routes
        try:
            app.include_router(admin_router, prefix=get_settings().API_V1_STR)
            app.include_router(internal_router, prefix=get_settings().API_V1_STR)
            logger.info("Registered admin and internal routes")
        except Exception as e:
            logger.error(f"Failed to register admin/internal routes: {str(e)}", exc_info=True)

    def unregister_all_modules(self, app: FastAPI) -> None:
        """Unregister all modules."""

        for module_name, module in self.modules.items():
            try:
                module.unregister(app)
                logger.info(f"Unregistered module: {module_name}")
            except Exception as e:
                logger.error(f"Failed to unregister module {module_name}: {e}")

    def get_module_submodule(self, module_name: str, submodule_path: str):
        """Get a submodule from a registered module.
        
        Args:
            module_name: Name of the module
            submodule_path: Relative path from the module (e.g. 'consumers', 'services.event_registry')
            
        Returns:
            The imported submodule or None if not found
        """
        if module_name not in self.module_infos:
            logger.error(f"Module '{module_name}' not found in registry")
            return None

        return self.module_infos[module_name].get_submodule(submodule_path)


class ModuleInfo:
    """Holds information about a discovered module."""

    def __init__(
        self,
        name: str,
        path: str,
        source: str,
        spec: Optional[importlib.machinery.ModuleSpec] = None,
    ):
        self.name: str = name  # Short name (e.g., 'events')
        self.path: str = path  # Full import path (e.g., 'app.modules.events')
        self.source: str = source  # 'app', 'explicit', 'installed'
        self.spec: Optional[importlib.machinery.ModuleSpec] = (
            spec  # Module spec, useful for finding file location etc.
        )
        self._module = None  # Lazily loaded module object

    def get_module(self):
        """Loads and returns the actual module object."""
        if self._module is None:
            try:
                self._module = importlib.import_module(self.path)
                logger.debug(f"Successfully imported module: {self.path}")
            except ImportError as e:
                logger.error(f"❌ Failed to import module {self.path}: {e}")
                raise  # Re-raise to signal failure during loading
            except Exception as e:
                logger.error(
                    f"❌ An unexpected error occurred importing module {self.path}: {e}"
                )
                raise

        return self._module

    def get_filesystem_path(self) -> Optional[str]:
        """Get the filesystem directory path for this module."""
        if self.spec and self.spec.origin:
            return os.path.dirname(self.spec.origin)
        return None

    def get_import_path(self) -> str:
        """Get the import path for this module."""
        return self.path

    def get_submodule(self, submodule_path: str, critical: bool = False):
        """Loads and returns a submodule of this module.
        
        Args:
            submodule_path: Relative path from the base module (e.g. 'consumers', 'services.event_registry')
            
        Returns:
            The imported submodule object
        """
        if not self.path:
            logger.error(f"Cannot load submodule '{submodule_path}': parent module path is not set")
            return None

        full_path = f"{self.path}.{submodule_path}"
        try:
            submodule = importlib.import_module(full_path)
            logger.debug(f"Successfully imported submodule: {full_path}")
            return submodule
        except ImportError as e:
            if critical:
                logger.error(f"❌ Failed to import critical submodule {full_path}: {e}")
                raise
            logger.warning(f"Warning: Failed to import submodule {full_path}: {e}")
            return None
        except ModuleNotFoundError as e:
            # Handle specific case where the submodule is not found
            if critical:
                logger.error(f"❌ Critical submodule {full_path} not found: {e}")
                raise
            logger.debug(f"Failed to import submodule {full_path}: {e}")
            return None
        except Exception as e:
            logger.error(
                f"❌ An unexpected error occurred importing submodule {full_path}: {e}"
            )
            return None

    def __repr__(self):
        return f"ModuleInfo(name='{self.name}', path='{self.path}', source='{self.source}')"


class ModuleDiscoverer:
    """
    Discovers modules based on the configured sources and priority.
    - Priority 1: App Modules (from app_modules_dir)
    - Priority 2: Explicit Modules (from explicit_modules list)
    - Priority 3: Installed Packages (from package_prefix)
    """
    # Default paths and prefixes
    DEFAULT_APP_MODULES_DIR = "app/modules"  # Default app modules directory
    DEFAULT_APP_MODULES_BASE_IMPORT_PATH = "app.modules"  # Default base import path
    DEFAULT_PACKAGE_PREFIX = "stufio.modules."  # Default package prefix for installed packages
    
    def __init__(
        self,
        app_modules_dir: str = DEFAULT_APP_MODULES_DIR,
        app_modules_base_import_path: str = DEFAULT_APP_MODULES_BASE_IMPORT_PATH,
        package_prefix: str = DEFAULT_PACKAGE_PREFIX,
        explicit_modules: Optional[List[str]] = None,
    ):
        self.app_modules_path = Path(app_modules_dir).resolve()
        self.app_modules_base_import_path = app_modules_base_import_path
        self.package_prefix = package_prefix
        self.explicit_modules = explicit_modules or []
        self.discovered_modules: Dict[str, ModuleInfo] = {}

    def _get_short_name(self, module_path: str) -> str:
        """Extracts the last component as the short name."""
        if "." in module_path:
            return module_path.split(".")[-1]
        return module_path  # Should not happen for valid package paths

    def _add_module(
        self,
        name: str,
        path: str,
        source: str,
        spec: Optional[importlib.machinery.ModuleSpec],
    ):
        """Adds module info if name not already present (respecting priority)."""
        if name not in self.discovered_modules:
            self.discovered_modules[name] = ModuleInfo(
                name=name, path=path, source=source, spec=spec
            )
            logger.debug(
                f"Discovered module '{name}' from '{source}' source at path '{path}'"
            )
        else:
            logger.debug(
                f"Module '{name}' from '{source}' source at path '{path}' ignored (overridden by '{self.discovered_modules[name].source}' source)"
            )

    def discover(self) -> Dict[str, ModuleInfo]:
        """
        Performs module discovery based on configured sources and priority.
        Returns a dictionary mapping short module names to ModuleInfo objects.
        """
        logger.info("Starting module discovery...")
        self.discovered_modules = {}  # Reset previous discoveries if any

        # --- Priority 1: App Modules ---
        self._discover_app_modules()

        # --- Priority 2: Explicit Modules ---
        self._discover_explicit_modules()

        # --- Priority 3: Installed Packages ---
        self._discover_installed_packages()

        logger.info(
            f"Module discovery complete. Found {len(self.discovered_modules)} prioritized modules: {list(self.discovered_modules.keys())}"
        )
        return self.discovered_modules

    def _discover_app_modules(self):
        """Scans the predefined application modules directory."""
        logger.info(
            f"Scanning for app modules in: {self.app_modules_path} (base import: {self.app_modules_base_import_path})"
        )
        if not self.app_modules_path.is_dir():
            logger.warning(
                f"App modules directory not found or not a directory: {self.app_modules_path}"
            )
            return

        # Ensure the parent directory of the base import path is in sys.path
        # This is crucial for importlib to find the modules.
        # Example: if base is 'app.modules', ensure directory containing 'app' is in sys.path
        # This often happens naturally if running from project root. Add checks if needed.

        for item in self.app_modules_path.iterdir():
            if item.is_dir() and (item / "__init__.py").is_file():
                module_name = item.name
                full_path = f"{self.app_modules_base_import_path}.{module_name}"
                spec = None
                try:
                    # Verify it's findable before adding
                    spec = importlib.util.find_spec(full_path)
                    if spec is None:
                        logger.warning(
                            f"App module '{module_name}' found in filesystem but cannot be imported via path '{full_path}'. Check sys.path and base import path."
                        )
                        continue
                    self._add_module(
                        name=module_name, path=full_path, source="app", spec=spec
                    )
                except Exception as e:
                    logger.error(f"Error checking app module spec for {full_path}: {e}")

    def _discover_explicit_modules(self):
        """Processes the explicitly defined list of module paths."""
        logger.info(f"Checking explicit modules: {self.explicit_modules}")
        for module_path in self.explicit_modules:
            module_name = self._get_short_name(module_path)
            if not module_name:
                logger.warning(
                    f"Could not determine short name for explicit module path: {module_path}"
                )
                continue

            try:
                spec = importlib.util.find_spec(module_path)
                if spec is None:
                    logger.warning(
                        f"Explicit module specified but not found: {module_path}"
                    )
                    continue
                if spec.origin is None or not spec.origin.endswith("__init__.py"):
                    logger.warning(
                        f"Explicit module path '{module_path}' does not point to a package (__init__.py not found or namespace package)."
                    )
                    # Decide whether to allow non-package modules if needed
                    # continue # Uncomment to strictly enforce packages

                self._add_module(
                    name=module_name, path=module_path, source="explicit", spec=spec
                )
            except Exception as e:
                logger.error(f"Error checking explicit module {module_path}: {e}")

    def _discover_installed_packages(self):
        """Scans installed packages matching the defined prefix."""
        # Import these at the beginning of the function
        import sys
        # No need to re-import importlib, use the one from module scope
        
        logger.info(f"Scanning for installed modules with prefix: {self.package_prefix}")
        modules_found = 0
        
        try:
            # Method 1: Original approach with namespace packages
            base_package_path = self.package_prefix.rstrip(".")
            spec = importlib.util.find_spec(base_package_path)
            
            if spec and spec.submodule_search_locations:
                logger.debug(f"Found submodule_search_locations: {spec.submodule_search_locations}")
                # Iterate through modules within the package(s) found
                for finder, name, ispkg in pkgutil.iter_modules(
                    spec.submodule_search_locations, prefix=self.package_prefix
                ):
                    if ispkg:  # We are interested in packages (directories with __init__.py)
                        module_name = self._get_short_name(name)
                        if not module_name:
                            continue  # Skip if name invalid
                        
                        # Re-verify spec for the specific submodule found by pkgutil
                        sub_spec = importlib.util.find_spec(name)
                        self._add_module(
                            name=module_name,
                            path=name,
                            source="installed",
                            spec=sub_spec,
                        )
                        modules_found += 1
            else:
                logger.debug(f"No submodule_search_locations found for {base_package_path}")
            
            # Method 2: Check existing imports in sys.modules
            for module_name in list(sys.modules.keys()):
                if module_name.startswith(self.package_prefix) and "." not in module_name[len(self.package_prefix):]:
                    short_name = self._get_short_name(module_name)
                    try:
                        sub_spec = importlib.util.find_spec(module_name)
                        self._add_module(
                            name=short_name,
                            path=module_name,
                            source="installed",
                            spec=sub_spec,
                        )
                        modules_found += 1
                    except Exception as e:
                        logger.debug(f"Error adding module from sys.modules {module_name}: {e}")
            
            # Method 3: Scan installed package distributions
            for dist in importlib.metadata.distributions():
                # Check for both direct modules and hyphenated package names
                dist_name = dist.metadata["Name"]
                if (dist_name.startswith("stufio-modules-") or 
                    dist_name.startswith("stufio.modules.")):
                    
                    # Convert hyphenated name to dotted import path if needed
                    if "-" in dist_name:
                        # stufio-modules-events -> stufio.modules.events
                        module_path = dist_name.replace("-", ".")
                    else:
                        module_path = dist_name
                    
                    # For hyphenated packages, get just the last part (e.g., 'events')
                    if dist_name.startswith("stufio-modules-"):
                        short_name = dist_name.split("-")[-1]
                    else:
                        short_name = self._get_short_name(module_path)
                    
                    # Ensure we're using the correct import path
                    if not module_path.startswith(self.package_prefix):
                        module_path = f"{self.package_prefix}{short_name}"
                    
                    try:
                        # Try to find the spec
                        logger.debug(f"Trying to find spec for module path: {module_path}")
                        sub_spec = importlib.util.find_spec(module_path)
                        if sub_spec:
                            self._add_module(
                                name=short_name,
                                path=module_path,
                                source="installed",
                                spec=sub_spec,
                            )
                            modules_found += 1
                            logger.debug(f"Successfully found module: {module_path}")
                        else:
                            logger.debug(f"Could not find spec for path: {module_path}")
                    except Exception as e:
                        logger.debug(f"Error processing package {dist_name}: {e}")
            
            logger.info(f"Found {modules_found} installed modules with prefix {self.package_prefix}")
            
        except Exception as e:
            logger.error(
                f"Error scanning installed packages with prefix {self.package_prefix}: {e}",
                exc_info=True,
            )


class ModuleInterface:
    """Base interface for registering modules with the app."""
    
    # Module metadata
    name: str = None
    version: str = "0.0.0"
    _routes_prefix: str = None
    _module_info: Optional[ModuleInfo] = None
    
    def __init__(self, module_info: Optional[ModuleInfo] = None):
        # Store ModuleInfo if provided
        self._module_info = module_info
        
        # Auto-determine module name from class name if not provided
        if not self.name:
            if self._module_info:
                self.name = self._module_info.name
            else:
                cls_name = self.__class__.__name__
                if (cls_name.endswith("Module")):
                    self.name = cls_name[:-6].lower()
                else:
                    self.name = cls_name.lower()
    
    @property
    def module_path(self) -> Optional[str]:
        """Get the module's import path."""
        if self._module_info:
            return self._module_info.path
        return None
    
    @property
    def module_dir(self) -> Optional[str]:
        """Get the module's filesystem directory."""
        if self._module_info:
            return self._module_info.get_filesystem_path()
        return None
        
    def register(self, app: 'StufioAPI') -> None:
        """Register this module with the FastAPI app."""
        try:
            # Call register_routes by default
            self.register_routes(app)
        except NotImplementedError:
            logger.warning(f"Module {self.name} does not implement register_routes")
        except Exception as e:
            logger.error(f"Error registering routes for module {self.name}: {str(e)}", exc_info=True)
            
    def unregister(self, app: 'StufioAPI') -> None:
        """Unregister this module from the FastAPI app."""
        pass

    async def on_startup(self, app: 'StufioAPI') -> None:
        """Called when the application starts up."""
        pass

    async def on_shutdown(self, app: 'StufioAPI') -> None:
        """Called when the application shuts down."""
        pass

    def register_routes(self, app: 'StufioAPI') -> None:
        """Register this module's routes with the FastAPI app."""
        raise NotImplementedError

    def get_middlewares(self) -> List[Tuple]:
        """Return middleware classes for this module."""
        return []

    @property
    def routes_prefix(self) -> str:
        """Get the routes prefix for this module."""
        if not self._routes_prefix:
            settings = get_settings()
            api_prefix = getattr(settings, "API_V1_STR", "/api/v1")

            self._routes_prefix = api_prefix

        return self._routes_prefix

    def get_submodule(self, submodule_path: str):
        """Get a submodule from this module.
        
        Args:
            submodule_path: Relative path from the module (e.g. 'consumers', 'services.event_registry')
            
        Returns:
            The imported submodule or None if not found
        """
        if not self._module_info:
            logger.warning(f"Cannot load submodule '{submodule_path}' for module '{self.name}': no module info available")
            return None
            
        return self._module_info.get_submodule(submodule_path)


# Singleton instance
registry = ModuleRegistry()
