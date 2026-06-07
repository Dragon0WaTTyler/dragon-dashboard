from __future__ import annotations

import os
from pathlib import Path

from .env import load_local_env


BASE_DIR = Path(__file__).resolve().parent.parent
DOTENV_PATH = BASE_DIR / ".env"
LOCAL_ENV = load_local_env(DOTENV_PATH)


def _resolve_path_from_env(env_var_name: str, default_path: Path) -> Path:
    raw_value = str(os.getenv(env_var_name, "") or LOCAL_ENV.get(env_var_name, "") or "").strip()
    if not raw_value:
        return default_path
    configured_path = Path(raw_value).expanduser()
    if not configured_path.is_absolute():
        configured_path = BASE_DIR / configured_path
    return configured_path.resolve()

CACHE_DIR = BASE_DIR / "cache"
BACKUPS_DIR = BASE_DIR / "backups"
READING_BACKUPS_DIR = BACKUPS_DIR / "reading"
READING_TTS_CACHE_DIR = CACHE_DIR / "reading_tts"
READING_RECIPE_OF_DAY_PATH = CACHE_DIR / "reading_recipe_of_day.json"
READING_FULLTEXT_CACHE_DIR = _resolve_path_from_env("DRAGON_READING_FULLTEXT_CACHE_DIR", CACHE_DIR / "articles" / "full_text")
YOUTUBE_LATEST_SNAPSHOT_PATH = CACHE_DIR / "youtube_latest_snapshot.json"
YOUTUBE_LATEST_SYNC_STATUS_PATH = CACHE_DIR / "youtube_latest_sync_status.json"
BOOKS_SNAPSHOT_PATH = CACHE_DIR / "books_snapshot.json"
BOOK_QUOTES_SNAPSHOT_PATH = CACHE_DIR / "quotes_snapshot.json"

YTS_TORRENTS_CACHE_PATH = BASE_DIR / "yts_torrents_cache.json"
DURATION_CACHE_PATH = BASE_DIR / "youtube_duration_cache.json"
PLAYLISTS_PATH = BASE_DIR / "playlists.json"
ADMIN_DATA_PATH = BASE_DIR / "admin_data.json"
DELETED_HISTORY_PATH = BASE_DIR / "deleted_history.json"
READING_DATA_PATH = _resolve_path_from_env("DRAGON_READING_DATA_PATH", BASE_DIR / "reading_data.json")
CHESS_DATA_PATH = BASE_DIR / "chess_data.json"
CHESS_COURSES_PATH = BASE_DIR / "chess_courses.json"
LICHESS_PUZZLE_SAMPLE_PATH = BASE_DIR / "lichess_puzzles_sample.csv"
LICHESS_PUZZLE_SAMPLE_DATA_PATH = BASE_DIR / "data" / "lichess_puzzles_sample.csv"
YOUTUBE_TOKEN_PATH = BASE_DIR / "youtube_token.json"
CACHE_DATA_PATH = BASE_DIR / "cache_data.json"
MOVIE_WATCH_PROGRESS_PATH = CACHE_DIR / "movie_watch_progress.json"
CHAT_HISTORY_DB_PATH = BASE_DIR / "chat_history.db"
CSV_CORRECTIONS_DIR = BASE_DIR / "csv_corrections"
CORRECTION_REPORTS_DIR = BASE_DIR / "correction_reports"
EXPORTS_DIR = BASE_DIR / "exports"
MISMATCH_CSV_PATH = Path(r"C:\Users\walid\Downloads\movie_metadata_mismatches.csv")


def discover_client_secrets_file(base_dir: Path = BASE_DIR) -> str:
    possible_names = [
        "client_secret.json",
        "client_secrets.json",
        "client_secret.json.json",
        "client_secret",
    ]
    candidates = (
        [base_dir / filename for filename in possible_names]
        + sorted(base_dir.glob("client_secret*"))
        + sorted(base_dir.glob("client_secrets*"))
    )
    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            return str(candidate)
    return str(base_dir / "client_secret.json")


CLIENT_SECRETS_FILE = discover_client_secrets_file()
YOUTUBE_CLIENT_SECRET_PATH = Path(CLIENT_SECRETS_FILE)
