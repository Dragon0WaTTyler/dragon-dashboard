from __future__ import annotations

from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None


def bootstrap_environment(dotenv_path: Path) -> None:
    if load_dotenv:
        load_dotenv(dotenv_path=str(dotenv_path), override=False, encoding="utf-8")


def load_local_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    try:
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip("'\"")
            if key:
                values[key] = value
    except Exception:
        return {}
    return values

