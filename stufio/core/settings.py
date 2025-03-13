from typing import Any, Dict, ClassVar, Type
from pydantic import ConfigDict
from pydantic_settings import BaseSettings


class ModuleSettings:
    """Base class for module-specific settings"""
    pass


class BaseStufioSettings(BaseSettings):

    # Registry of module settings classes - not an actual field
    _module_settings_registry: ClassVar[Dict[str, Type[ModuleSettings]]] = {}

    # Dynamic storage for module settings instances
    modules: Dict[str, Any] = {}

    @classmethod
    def register_module_settings(
        cls, module_name: str, settings_class: Type[ModuleSettings]
    ):
        """Register a module's settings class"""
        cls._module_settings_registry[module_name] = settings_class
        return settings_class

    def dict(self, *args, **kwargs):
        """Override dict to include module settings"""
        result = super().dict(*args, **kwargs)

        # Include module settings with their prefixes
        for module_name, module_settings in self.modules.items():
            if hasattr(module_settings, "dict"):
                module_dict = module_settings.dict(*args, **kwargs)
                for key, value in module_dict.items():
                    result[f"{module_name}_{key}"] = value

        return result

    def __getattr__(self, name: str):
        """
        Allow accessing module settings directly with {module}_{SETTING} pattern
        Example: settings.activity_RATE_LIMIT_IP_MAX_REQUESTS

        Also handles lazy-loading of modules registered after initialization
        """
        # First check if attribute exists directly in object's dictionary
        if name in self.__dict__:
            return self.__dict__[name]
            
        # Or check if it's a @property or method defined on the class
        if hasattr(self.__class__, name):
            return getattr(self.__class__, name).__get__(self, self.__class__)
            
        # Check if this might be a module setting
        if "_" in name:
            parts = name.split("_", 1)
            if len(parts) == 2:
                module_name, setting_name = parts

                # Initialize module settings if registered but not yet initialized
                if module_name in self._module_settings_registry and (
                    not hasattr(self, "modules")
                    or self.modules is None
                    or module_name not in self.modules
                ):
                    if not hasattr(self, "modules") or self.modules is None:
                        self.modules = {}

                    # Extract module settings with prefix
                    module_settings_dict = {}
                    for field_name, field_value in self.__dict__.items():
                        module_prefix = f"{module_name}_"
                        if field_name.startswith(module_prefix):
                            setting_name_in_dict = field_name[len(module_prefix) :]
                            module_settings_dict[setting_name_in_dict] = field_value

                    # Initialize and store module settings
                    self.modules[module_name] = self._module_settings_registry[
                        module_name
                    ](**module_settings_dict)

                # Check if this module exists in modules dict
                if (
                    hasattr(self, "modules")
                    and self.modules
                    and module_name in self.modules
                ):
                    module_settings = self.modules[module_name]

                    # Try to access the setting from the module
                    if hasattr(module_settings, setting_name):
                        return getattr(module_settings, setting_name)

        # Fallback to normal attribute error
        raise AttributeError(f"'StufioSettings' object has no attribute '{name}'")

    # Allow extra attributes in this model
    model_config = ConfigDict(extra="allow")
