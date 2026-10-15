import os
from typing import List, Tuple, Type

import yaml
from pydantic import BaseModel, Extra
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)

from meilisync.enums import ProgressType, SourceType
from meilisync.plugin import load_plugin


class DictSettingsSource(PydanticBaseSettingsSource):
    def __init__(self, settings_cls: Type[BaseSettings], data: dict):
        super().__init__(settings_cls)
        self.data = data or {}

    def get_field_value(self, field, field_name: str):
        return self.data.get(field_name), field_name, False

    def __call__(self) -> dict:
        return self.data


class Source(BaseModel):
    type: SourceType
    database: str

    class Config:
        extra = Extra.allow


class MeiliSearch(BaseModel):
    api_url: str
    api_key: str | None = None
    insert_size: int | None = None
    insert_interval: int | None = None


class BasePlugin(BaseModel):
    plugins: List[str] = []

    def plugins_cls(self):
        plugins = []
        for plugin in self.plugins or []:
            p = load_plugin(plugin)
            if p.is_global:
                plugins.append(p())
            else:
                plugins.append(p)
        return plugins


class Sync(BasePlugin):
    table: str
    pk: str = "id"
    full: bool = False
    index: str | None = None
    fields: dict | None = None

    @property
    def index_name(self):
        return self.index or self.table

    def __hash__(self):
        return hash(self.table)


class Progress(BaseModel):
    type: ProgressType

    class Config:
        extra = Extra.allow


class Sentry(BaseModel):
    dsn: str
    environment: str = "production"


class Settings(BaseSettings, BasePlugin):
    progress: Progress
    debug: bool = False
    source: Source
    meilisearch: MeiliSearch
    sync: List[Sync]
    sentry: Sentry | None = None

    model_config = SettingsConfigDict(
        env_nested_delimiter="__",
        extra="ignore",
    )

    @classmethod
    def load(cls, config_path: str):
        data = {}
        if os.path.exists(config_path):
            with open(config_path) as f:
                data = yaml.safe_load(f) or {}

        class DynamicSettings(cls):
            @classmethod
            def settings_customise_sources(
                cls,
                settings_cls,
                init_settings: PydanticBaseSettingsSource,
                env_settings: PydanticBaseSettingsSource,
                dotenv_settings: PydanticBaseSettingsSource,
                file_secret_settings: PydanticBaseSettingsSource,
            ) -> Tuple[PydanticBaseSettingsSource, ...]:
                return (init_settings, env_settings, DictSettingsSource(settings_cls, data))

        return DynamicSettings()

    @property
    def tables(self):
        return [sync.table for sync in self.sync]

    def get_sync(self, table: str):
        for sync in self.sync:
            if sync.table == table:
                return sync
