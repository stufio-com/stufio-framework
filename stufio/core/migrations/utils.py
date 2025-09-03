import os
import datetime

def get_current_version_dir():
    """
    Get the current date-based version directory name (v20250308 format).
    """
    today = datetime.datetime.now(datetime.timezone.utc)
    return f"v{today.strftime('%Y%m%d')}"

def ensure_migration_dir(base_path, module_name=None):
    """
    Ensure migration directory exists for a module or the core app.
    
    Args:
        base_path: Base path where migrations should be created
        module_name: Name of the module, or None for core app
        
    Returns:
        Tuple of (migrations_dir, version_dir_path)
    """
    if module_name:
        # Module migration
        migrations_dir = os.path.join(base_path, "modules", module_name, "migrations")
    else:
        # Core app migration
        migrations_dir = os.path.join(base_path, "app", "migrations")
        
    # Create migrations directory if it doesn't exist
    os.makedirs(migrations_dir, exist_ok=True)
    
    # Create __init__.py if it doesn't exist
    init_py = os.path.join(migrations_dir, "__init__.py")
    if not os.path.exists(init_py):
        with open(init_py, "w") as f:
            f.write("# Auto-generated migrations package\n")
    
    # Create version directory with current date
    version_dir = get_current_version_dir()
    version_dir_path = os.path.join(migrations_dir, version_dir)
    os.makedirs(version_dir_path, exist_ok=True)
    
    # Create __init__.py in version directory
    version_init_py = os.path.join(version_dir_path, "__init__.py")
    if not os.path.exists(version_init_py):
        with open(version_init_py, "w") as f:
            f.write(f"# Migrations created on {datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d')}\n")
            
    return migrations_dir, version_dir_path

def create_migration_file(base_path, name, template, module_name=None):
    """
    Create a new migration file with the given name.
    
    Args:
        base_path: Base path where migrations should be created
        name: Name of the migration (will be prefixed with order number)
        template: Template string for the migration
        module_name: Name of the module, or None for core app
        
    Returns:
        Path to the created migration file
    """
    # Ensure directories exist
    _, version_dir_path = ensure_migration_dir(base_path, module_name)
    
    # Determine next available order number
    existing_files = [f for f in os.listdir(version_dir_path) if f.endswith('.py') and not f.startswith('__')]
    
    # Find the highest existing order number
    highest_order = 0
    for file in existing_files:
        try:
            order = int(file.split('_')[0])
            highest_order = max(highest_order, order)
        except (ValueError, IndexError):
            pass
    
    # Create new migration with incremented order number
    new_order = highest_order + 1
    file_name = f"{new_order:02d}_{name}.py"
    file_path = os.path.join(version_dir_path, file_name)
    
    with open(file_path, "w") as f:
        f.write(template)
        
    return file_path