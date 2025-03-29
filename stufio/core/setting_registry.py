from typing import Dict, Any, Optional, List, Union, Type
from enum import Enum
from pydantic import BaseModel, Field
from ..schemas.setting import (
    SettingType,
    GroupMetadataSchema,
    SubgroupMetadataSchema,
    SettingMetadataSchema
)

# Type aliases for clarity
GroupMetadata = GroupMetadataSchema
SubgroupMetadata = SubgroupMetadataSchema
SettingMetadata = SettingMetadataSchema


# Registry for settings metadata
class SettingRegistry:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SettingRegistry, cls).__new__(cls)
            cls._instance._settings = {}
            cls._instance._groups = {}
            cls._instance._subgroups = {}
        return cls._instance

    def register_setting(self, setting: SettingMetadata):
        """Register a setting metadata"""
        self._settings[f"{setting.module}.{setting.key}"] = setting

    def register_group(self, group: GroupMetadata):
        """Register a group metadata"""
        self._groups[group.id] = group

    def register_subgroup(
        self, subgroup: SubgroupMetadata
    ):
        """Register a subgroup metadata"""
        _group = self._groups.get(subgroup.group_id)
        if not _group:
            raise ValueError(f"Group {subgroup.group_id} not found for subgroup {subgroup.id}")

        self._subgroups[f"{subgroup.group_id}.{subgroup.id}"] = subgroup

    def get_settings(self, module: str = None) -> List[SettingMetadata]:
        """Get all settings, optionally filtered by module"""
        if module:
            return [s for k, s in self._settings.items() if s.module == module]
        return list(self._settings.values())

    def get_groups(self) -> List[GroupMetadata]:
        """Get all groups, optionally filtered by module"""
        return sorted(list(self._groups.values()), key=lambda g: g.order)

    def get_subgroups(
        self, group_id: str
    ) -> List[SubgroupMetadata]:
        """Get all subgroups for a group"""
        return sorted(
            [
                sg
                for k, sg in self._subgroups.items()
                if k.startswith(f"{group_id}.")
            ],
            key=lambda sg: sg.order,
        )


# Singleton instance
settings_registry = SettingRegistry()


def get_setting_registry() -> SettingRegistry:
    """Get the setting registry singleton"""
    return SettingRegistry()
