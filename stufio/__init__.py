"""
Stufio - A modular FastAPI framework for building scalable applications
"""

__version__ = "0.1.0"

from stufio.core.module_registry import ModuleRegistry, ModuleInterface
from . import crud, models, schemas

# Export commonly used classes
registry = ModuleRegistry()

__all__ = ["ModuleRegistry", "ModuleInterface", "registry", "crud", "models", "schemas"]