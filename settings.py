import os
import yaml
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseBackupConfig(BaseModel):
    compress: int = 0
    keep: int = 0


class BackupConfig(BaseModel):
    default_compress: int = 0
    default_keep: int = 0
    databases: dict[str, DatabaseBackupConfig] = {}


class RestoreConfig(BaseModel):
    backup_dirs: list[str] = ["backup", "restory"]


class SchedulerConfig(BaseModel):
    enabled: bool = False
    cron: str = "0 2 * * *"


class WebUIConfig(BaseModel):
    enabled: bool = False
    host: str = "0.0.0.0"
    port: int = 8080


class ServerProfile(BaseModel):
    name: str = ""
    host: str = ""
    user: str = "root"
    port: int = 22
    pg_port: int = 5432


class RemoteServer(BaseModel):
    host: str = ""
    user: str = ""
    port: int = 22
    pg_port: int = 5432


class YamlConfig(BaseModel):
    connection: dict[str, str] = {"host": "localhost", "user": "postgres"}
    backup: BackupConfig = BackupConfig()
    restore: RestoreConfig = RestoreConfig()
    scheduler: SchedulerConfig = SchedulerConfig()
    web: WebUIConfig = WebUIConfig()
    remote: RemoteServer = RemoteServer()
    servers: list[ServerProfile] = []


class EnvSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")
    DB_PASSWORD: str = ""
    DB_HOST: str | None = None
    DB_USER: str | None = None


def load_yaml(path="settings.yaml"):
    if not os.path.exists(path):
        return YamlConfig()
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return YamlConfig(**data) if data else YamlConfig()


def get_settings():
    yaml_cfg = load_yaml()
    env_cfg = EnvSettings()

    host = env_cfg.DB_HOST or yaml_cfg.connection.get("host", "localhost")
    user = env_cfg.DB_USER or yaml_cfg.connection.get("user", "postgres")
    password = env_cfg.DB_PASSWORD

    return host, user, password, yaml_cfg


def get_db_backup_config(db_name, yaml_cfg):
    if db_name in yaml_cfg.backup.databases:
        return yaml_cfg.backup.databases[db_name]
    return DatabaseBackupConfig()


def save_remote_config(host, user, port, pg_port, path="settings.yaml"):
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    data.setdefault("remote", {})
    data["remote"]["host"] = host
    data["remote"]["user"] = user
    data["remote"]["port"] = port
    data["remote"]["pg_port"] = pg_port
    with open(path, "w", encoding="utf-8") as f:
        yaml.dump(data, f, allow_unicode=True, default_flow_style=False, sort_keys=False)
