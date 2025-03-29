## Usage Examples for Different Setting Types

Here are examples of how to register different types of settings in your Python settings registry:

### 1. String Setting
```python
settings_registry.register_setting(
    SettingMetadata(
        key="core_SITE_NAME",
        label="Site Name",
        description="The name of your site",
        group="general",
        subgroup="site",
        type=SettingType.STRING,
        placeholder="My Awesome Site",
        order=10,
        module="core"
    )
)
```

### 2. Number Setting
```python
settings_registry.register_setting(
    SettingMetadata(
        key="core_PAGINATION_LIMIT",
        label="Pagination Limit",
        description="Number of items per page",
        group="general",
        subgroup="content",
        type=SettingType.NUMBER,
        min=5,
        max=100,
        order=20,
        module="core"
    )
)
```

### 3. Select/Dropdown Setting
```python
settings_registry.register_setting(
    SettingMetadata(
        key="core_DEFAULT_THEME",
        label="Default Theme",
        description="Choose the default theme for your site",
        group="appearance",
        subgroup="theming",
        type=SettingType.SELECT,
        options=[
            {"value": "light", "label": "Light Theme"},
            {"value": "dark", "label": "Dark Theme"},
            {"value": "system", "label": "System Default"}
        ],
        order=10,
        module="core"
    )
)
```

### 4. Switch/Boolean Setting
```python
settings_registry.register_setting(
    SettingMetadata(
        key="security_ENABLE_2FA",
        label="Enable Two-Factor Authentication",
        description="Require two-factor authentication for all users",
        group="security",
        subgroup="authentication",
        type=SettingType.SWITCH,
        order=10,
        module="security"
    )
)
```

### 5. Radio Group Setting
```python
settings_registry.register_setting(
    SettingMetadata(
        key="content_IMAGE_QUALITY",
        label="Image Quality",
        description="Quality setting for uploaded images",
        group="content",
        subgroup="media",
        type=SettingType.RADIO,
        options=[
            {"value": "low", "label": "Low (faster loading)"},
            {"value": "medium", "label": "Medium (balanced)"},
            {"value": "high", "label": "High (best quality)"}
        ],
        order=30,
        module="content"
    )
)
```

### 6. Date Setting
```python
settings_registry.register_setting(
    SettingMetadata(
        key="system_MAINTENANCE_DATE",
        label="Scheduled Maintenance Date",
        description="Date of the next scheduled maintenance",
        group="system",
        subgroup="maintenance",
        type=SettingType.DATE,
        order=40,
        module="system"
    )
)
```

### 7. Color Setting
```python
settings_registry.register_setting(
    SettingMetadata(
        key="appearance_PRIMARY_COLOR",
        label="Primary Color",
        description="The primary brand color for your site",
        group="appearance",
        subgroup="colors",
        type=SettingType.COLOR,
        order=10,
        module="appearance"
    )
)
```
