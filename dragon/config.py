from __future__ import annotations

import os
import secrets
from dataclasses import dataclass

from .env import bootstrap_environment, load_local_env
from .paths import DOTENV_PATH


bootstrap_environment(DOTENV_PATH)
LOCAL_ENV = load_local_env(DOTENV_PATH)


def config_value(name: str, default: str = "") -> str:
    env_value = os.environ.get(name)
    if env_value not in (None, ""):
        return env_value
    file_value = LOCAL_ENV.get(name)
    if file_value not in (None, ""):
        return file_value
    return default


def config_flag(name: str, default: bool = False) -> bool:
    raw_value = config_value(name, "1" if default else "0")
    return str(raw_value or "").strip().lower() not in {"", "0", "false", "no", "off"}


def config_int(name: str, default: int = 0, minimum: int | None = None, maximum: int | None = None) -> int:
    raw_value = config_value(name, str(default))
    try:
        value = int(str(raw_value or "").strip())
    except Exception:
        value = int(default)
    if minimum is not None:
        value = max(int(minimum), value)
    if maximum is not None:
        value = min(int(maximum), value)
    return value


@dataclass(frozen=True)
class DragonRuntimeConfig:
    flask_env_name: str
    is_production: bool
    flask_secret_key: str


def build_runtime_config() -> DragonRuntimeConfig:
    flask_env_name = str(config_value("FLASK_ENV", "") or "").strip().lower()
    is_production = flask_env_name == "production" or config_flag("RENDER", False)
    flask_secret_key = config_value("FLASK_SECRET_KEY", "")
    if not flask_secret_key:
        if is_production:
            raise RuntimeError("Missing FLASK_SECRET_KEY. Set it in the environment before running Dragon online.")
        flask_secret_key = secrets.token_urlsafe(32)
    return DragonRuntimeConfig(
        flask_env_name=flask_env_name,
        is_production=is_production,
        flask_secret_key=flask_secret_key,
    )


def emit_environment_diagnostics() -> None:
    print(f"[env] .env path: {DOTENV_PATH} | exists: {DOTENV_PATH.exists()}")
    print(f"[env] NOTION_TOKEN detected: {bool(os.environ.get('NOTION_TOKEN') or LOCAL_ENV.get('NOTION_TOKEN'))}")
    print(f"[env] NOTION_DATABASE_ID detected: {bool(os.environ.get('NOTION_DATABASE_ID') or LOCAL_ENV.get('NOTION_DATABASE_ID'))}")
    print(f"[env] NOTION_BOOKS_DATABASE_ID detected: {bool(os.environ.get('NOTION_BOOKS_DATABASE_ID') or LOCAL_ENV.get('NOTION_BOOKS_DATABASE_ID'))}")
    print(f"[env] NOTION_BOOK_QUOTES_DATABASE_ID detected: {bool(os.environ.get('NOTION_BOOK_QUOTES_DATABASE_ID') or LOCAL_ENV.get('NOTION_BOOK_QUOTES_DATABASE_ID'))}")
    print(f"[env] NOTION_BOOK_QUOTES_SOURCE_PAGE_ID detected: {bool(os.environ.get('NOTION_BOOK_QUOTES_SOURCE_PAGE_ID') or LOCAL_ENV.get('NOTION_BOOK_QUOTES_SOURCE_PAGE_ID'))}")

