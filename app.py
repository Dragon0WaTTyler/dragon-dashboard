import os
import json
import csv
import asyncio
import hashlib
import shutil
import requests
import urllib.parse
import math
import re
import random
import secrets
import sqlite3
import time
import threading
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from datetime import datetime
from html import escape, unescape
from html.parser import HTMLParser
from pathlib import Path
from flask import Flask, Response, jsonify, redirect, render_template, request, session, send_file, url_for
from markupsafe import Markup
from werkzeug.middleware.proxy_fix import ProxyFix
try:
    import edge_tts
except ImportError:
    edge_tts = None
try:
    import google.generativeai as genai
except ImportError:
    genai = None
try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

try:
    from google.auth.transport.requests import Request as GoogleAuthRequest
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import Flow
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
except ImportError:
    GoogleAuthRequest = None
    Credentials = None
    Flow = None
    build = None
    HttpError = Exception

BASE_DIR = Path(__file__).resolve().parent
DOTENV_PATH = BASE_DIR / ".env"
if load_dotenv:
    load_dotenv(dotenv_path=str(DOTENV_PATH), override=False, encoding="utf-8")


def load_local_env(path):
    values = {}
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


LOCAL_ENV = load_local_env(BASE_DIR / ".env")


def config_value(name, default=""):
    env_value = os.environ.get(name)
    if env_value not in (None, ""):
        return env_value
    file_value = LOCAL_ENV.get(name)
    if file_value not in (None, ""):
        return file_value
    return default


def config_flag(name, default=False):
    raw_value = config_value(name, "1" if default else "0")
    return str(raw_value or "").strip().lower() not in {"", "0", "false", "no", "off"}


print(f"[env] .env path: {DOTENV_PATH} | exists: {DOTENV_PATH.exists()}")
print(f"[env] NOTION_TOKEN detected: {bool(os.environ.get('NOTION_TOKEN') or LOCAL_ENV.get('NOTION_TOKEN'))}")
print(f"[env] NOTION_DATABASE_ID detected: {bool(os.environ.get('NOTION_DATABASE_ID') or LOCAL_ENV.get('NOTION_DATABASE_ID'))}")
print(f"[env] NOTION_BOOKS_DATABASE_ID detected: {bool(os.environ.get('NOTION_BOOKS_DATABASE_ID') or LOCAL_ENV.get('NOTION_BOOKS_DATABASE_ID'))}")
print(f"[env] NOTION_BOOK_QUOTES_DATABASE_ID detected: {bool(os.environ.get('NOTION_BOOK_QUOTES_DATABASE_ID') or LOCAL_ENV.get('NOTION_BOOK_QUOTES_DATABASE_ID'))}")
print(f"[env] NOTION_BOOK_QUOTES_SOURCE_PAGE_ID detected: {bool(os.environ.get('NOTION_BOOK_QUOTES_SOURCE_PAGE_ID') or LOCAL_ENV.get('NOTION_BOOK_QUOTES_SOURCE_PAGE_ID'))}")


app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)

FLASK_ENV_NAME = str(config_value("FLASK_ENV", "") or "").strip().lower()
IS_PRODUCTION = FLASK_ENV_NAME == "production" or config_flag("RENDER", False)
FLASK_SECRET_KEY = config_value("FLASK_SECRET_KEY", "")
if not FLASK_SECRET_KEY:
    if IS_PRODUCTION:
        raise RuntimeError("Missing FLASK_SECRET_KEY. Set it in the environment before running Dragon online.")
    FLASK_SECRET_KEY = secrets.token_urlsafe(32)
app.secret_key = FLASK_SECRET_KEY
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=IS_PRODUCTION,
    PREFERRED_URL_SCHEME="https" if IS_PRODUCTION else "http",
)

# Allow OAuth over http://localhost during local development only.
if not IS_PRODUCTION:
    os.environ.setdefault("OAUTHLIB_INSECURE_TRANSPORT", "1")

# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  CONFIGURATION
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
YOUTUBE_API_KEY      = config_value("YOUTUBE_API_KEY", "")
GEMINI_API_KEY       = config_value("GEMINI_API_KEY", "")
GEMINI_PROJECT_NAME  = config_value("GEMINI_PROJECT_NAME", "projects/947897749170")
GEMINI_PROJECT_NUMBER = config_value("GEMINI_PROJECT_NUMBER", "947897749170")
NOTION_TOKEN         = config_value("NOTION_TOKEN", "")
NOTION_DATABASE_ID   = config_value("NOTION_DATABASE_ID", "")
NOTION_BOOKS_DATABASE_ID = config_value("NOTION_BOOKS_DATABASE_ID", "")
NOTION_BOOK_QUOTES_DATABASE_ID = config_value("NOTION_BOOK_QUOTES_DATABASE_ID", "")
NOTION_BOOK_QUOTES_SOURCE_PAGE_ID = config_value("NOTION_BOOK_QUOTES_SOURCE_PAGE_ID", "")
NOTION_BOOK_QUOTES_SOURCE_PAGE_TITLE = config_value("NOTION_BOOK_QUOTES_SOURCE_PAGE_TITLE", "مقولات من كتبي")
TMDB_API_KEY         = config_value("TMDB_API_KEY", "")
NOTION_DIRECTORS_DATABASE_ID = config_value("NOTION_DIRECTORS_DATABASE_ID", "")
NOTION_DIRECTORS_PARENT_PAGE_ID = config_value("NOTION_DIRECTORS_PARENT_PAGE_ID", "")
NOTION_GENRES_DATABASE_ID = config_value("NOTION_GENRES_DATABASE_ID", "")
MOVIE_WANT_TO_UNION_FETCH_ENABLED = config_value("MOVIE_WANT_TO_UNION_FETCH_ENABLED", "1")
NOTEBOOKLM_URL       = config_value("NOTEBOOKLM_URL", "https://notebooklm.google.com/")
DRAGON_ADMIN_USERNAME = config_value("DRAGON_ADMIN_USERNAME", "")
DRAGON_ADMIN_PASSWORD = config_value("DRAGON_ADMIN_PASSWORD", "")
DRAGON_PROTECT_WHOLE_SITE = config_flag("DRAGON_PROTECT_WHOLE_SITE", IS_PRODUCTION)
MOVIE_WANT_TO_UNION_FETCH_FLAG_NAME = "MOVIE_WANT_TO_UNION_FETCH_ENABLED"
DEFAULT_MOVIE_FETCH_EXPERIMENT_UI_COUNT = 506
MOVIE_FETCH_EXPERIMENT_ANCHOR_TITLES = (
    "Decalogue IV",
    "Mon Oncle",
    "The World of Apu",
    "Red River",
    "The Bridge on the River Kwai",
)
MOVIE_WANT_TO_UNION_COMPARE_STRATEGIES = (
    "production_unfiltered_no_sort",
    "created_time_ascending",
    "created_time_descending",
    "last_edited_time_ascending",
    "last_edited_time_descending",
    "title_ascending",
    "title_descending",
    "year_ascending",
    "year_descending",
    "status_equals_iwantto",
    "status_equals_finished",
    "source_equals_ebertslibrary",
    "source_equals_mylibrary",
    "source_equals_mylibraryandeberts",
    "category_equals_movie",
    "category_equals_tvshow",
    "category_equals_anime",
    "category_equals_shortmovie",
)
DURATION_CACHE_PATH  = Path(__file__).resolve().parent / "youtube_duration_cache.json"
PLAYLISTS_PATH       = BASE_DIR / "playlists.json"
ADMIN_DATA_PATH      = BASE_DIR / "admin_data.json"
DELETED_HISTORY_PATH = BASE_DIR / "deleted_history.json"
READING_DATA_PATH    = BASE_DIR / "reading_data.json"
READING_BACKUPS_DIR  = BASE_DIR / "backups" / "reading"
READING_TTS_CACHE_DIR = BASE_DIR / "cache" / "reading_tts"
YOUTUBE_TOKEN_PATH   = BASE_DIR / "youtube_token.json"
CACHE_DATA_PATH      = BASE_DIR / "cache_data.json"
CHAT_HISTORY_DB_PATH = BASE_DIR / "chat_history.db"
CACHE_MAX_AGE_SECONDS = 24 * 60 * 60
READING_BACKUP_KEEP_COUNT = 8
READING_TTS_MAX_CHARS = 12000
READING_TTS_MIN_CHARS = 120
READING_TTS_DEFAULT_VOICES = {
    "ar": "ar-EG-SalmaNeural",
    "en": "en-US-AriaNeural",
}
READING_TTS_GENERATION_LOCK = threading.Lock()
READING_TTS_SYNC_LEAD_SECONDS = 0.08
READING_TTS_TIMINGS_VERSION = 3
READING_TTS_MAX_AUDIO_START_OFFSET_SECONDS = 0.24
BOOK_COVER_CACHE = {}
BOOKS_ENTRIES_CACHE = {"entries": None, "error": "", "updated_at": 0}
BOOK_QUOTES_IMPORT_CACHE = {"books": None, "updated_at": 0}
BOOK_QUOTES_ENTRIES_CACHE = {"entries": None, "error": "", "updated_at": 0}
possible_names = [
    "client_secret.json",
    "client_secrets.json",
    "client_secret.json.json",
    "client_secret",
]
CLIENT_SECRETS_FILE = next(
    (
        str(candidate)
        for candidate in (
            [BASE_DIR / filename for filename in possible_names]
            + sorted(BASE_DIR.glob("client_secret*"))
            + sorted(BASE_DIR.glob("client_secrets*"))
        )
        if candidate.exists() and candidate.is_file()
    ),
    os.path.join(os.path.dirname(__file__), "client_secret.json")
)
YOUTUBE_CLIENT_SECRET_PATH = Path(CLIENT_SECRETS_FILE)
YOUTUBE_OAUTH_SCOPES = ["https://www.googleapis.com/auth/youtube.force-ssl"]

if IS_PRODUCTION and (not DRAGON_ADMIN_USERNAME or not DRAGON_ADMIN_PASSWORD):
    raise RuntimeError(
        "Missing DRAGON_ADMIN_USERNAME or DRAGON_ADMIN_PASSWORD. "
        "Set both before deploying Dragon online."
    )

# Score mappings (unchanged)
SCORE_ORDER = {
    "god mode": 9, "close to god mode": 8, "masterpiece": 7,
    "Sweet": 6, "good": 5, "acceptable": 4, "naah": 3,
    "i don't like it": 1, "": 0
}
SCORE_DISPLAY = {
    "god mode": "God Mode", "close to god mode": "Close to God Mode",
    "masterpiece": "Masterpiece", "Sweet": "Sweet", "good": "Good",
    "acceptable": "Acceptable", "naah": "Naah",
    "i don't like it": "Didn't Like", "": "Not Rated"
}
SCORE_COLOR = {
    "god mode": "#e8c96a", "close to god mode": "#c9a84c",
    "masterpiece": "#c9a84c", "Sweet": "#8a9e7a", "good": "#7a8e6a",
    "acceptable": "#6a7a8a", "naah": "#6a6460", "i don't like it": "#5a4444",
    "": "#3a3a3a"
}
VALID_MOVIE_CATEGORIES = {
    "movie",
    "tv show",
    "anime",
    "short movie",
    "documentary",
    "theatre",
}
NON_MOVIE_CATEGORY_MARKERS = (
    "youtube video",
    "youtube",
    "music video",
    "video essay",
    "video clip",
    "clip",
    "trailer",
    "podcast",
    "episode",
    "livestream",
    "live stream",
)
SOURCE_FILTER_OPTIONS = [
    "All sources",
    "My library",
    "Ebert's library",
    "My library and Ebert's",
]
MOVIE_REVIEW_IMPORTANT_FIELDS = ("poster", "year", "director", "genres", "overview")
CURATION_METADATA_FIELDS = ("poster", "year", "director", "genres", "runtime", "overview")
PRIMARY_WATCH_NEXT_STATUSES = {
    "iwantto",
    "wanttowatch",
    "plantowatch",
    "upnext",
    "queued",
    "queue",
    "prioritywatch",
}
WATCHED_STATUS_ALIASES = {
    "finished",
    "watched",
    "completed",
    "done",
    "seen",
    "alreadywatched",
    "finishedwatching",
}
TITLE_NOISE_TOKENS = (
    "1080p", "720p", "2160p", "bluray", "brrip", "webrip", "web-dl",
    "hdrip", "x264", "x265", "yify", "dvdrip"
)
CSV_CORRECTIONS_DIR = BASE_DIR / "csv_corrections"
CORRECTION_REPORTS_DIR = BASE_DIR / "correction_reports"
EXPORTS_DIR = BASE_DIR / "exports"
MISMATCH_CSV_PATH = Path(r"C:\Users\walid\Downloads\movie_metadata_mismatches.csv")
CSV_CORRECTION_SCHEMA = ("original_title", "corrected_title", "director", "year", "notes")
MOVIE_EXPORT_FIELDS = (
    "name",
    "category",
    "status",
    "score",
    "watch_date",
    "finish_date",
    "rewatch",
    "trailer",
    "poster",
    "year",
    "director",
    "genres",
    "runtime",
    "overview",
)
CSV_CORRECTION_HEADER_ALIASES = {
    "originalentry": "original_title",
    "originaltitle": "original_title",
    "titlefromlist": "original_title",
    "confirmedtitle": "corrected_title",
    "correctedtitle": "corrected_title",
    "canonicaltitle": "corrected_title",
    "verifiedtitle": "corrected_title",
    "title": "corrected_title",
    "directorcreator": "director",
    "directororcreator": "director",
    "director": "director",
    "creator": "director",
    "firstreleaseyear": "year",
    "releaseyear": "year",
    "year": "year",
    "notes": "notes",
}
TARGETED_MOVIE_TITLES = (
    "Black Knight",
    "Breaking Bad Season 1",
    "Breaking Bad Season 3",
    "The Mummy",
    "Bad Boys II",
    "Boyka: Undisputed IV",
    "The Boy",
    "Breaking Bad Season 4",
    "Spirited Away",
    "Small Pleasures",
    "La Chinoise",
    "Masculin Feminin",
    "Your Name",
    "Monster",
    "\u041a\u043e\u043d\u0444\u043b\u0438\u043a\u0442",
    "My One And Only Love",
    "Cry, the Beloved Country",
    "Taxi T\u00e9h\u00e9ran",
    "metropolis",
    "Cry, the Beloved Country 1995",
)
TARGETED_TMDB_OVERRIDES = {
    "Black Knight": {"kind": "movie", "tmdb_id": 11469, "confidence": "high", "fields": ("poster", "genres", "rating")},
    "Breaking Bad Season 1": {"kind": "tv_season", "tmdb_id": 1396, "season_number": 1, "confidence": "high", "fields": ("genres", "rating", "overview")},
    "Breaking Bad Season 3": {"kind": "tv_season", "tmdb_id": 1396, "season_number": 3, "confidence": "high", "fields": ("year", "poster", "genres", "rating", "overview")},
    "The Mummy": {"kind": "movie", "tmdb_id": 564, "confidence": "high", "fields": ("rating", "overview")},
    "Bad Boys II": {"kind": "movie", "tmdb_id": 8961, "confidence": "high", "fields": ("genres", "rating", "overview")},
    "Boyka: Undisputed IV": {"kind": "movie", "tmdb_id": 348893, "confidence": "high", "fields": ("genres", "overview")},
    "The Boy": {"kind": "movie", "tmdb_id": 321258, "confidence": "high", "fields": ()},
    "Breaking Bad Season 4": {"kind": "tv_season", "tmdb_id": 1396, "season_number": 4, "confidence": "high", "fields": ("year", "poster", "genres", "rating", "overview")},
    "Spirited Away": {"kind": "movie", "tmdb_id": 129, "confidence": "high", "fields": ("overview",)},
    "Small Pleasures": {"kind": "movie", "tmdb_id": 391727, "confidence": "high", "fields": ("overview",)},
    "La Chinoise": {"kind": "movie", "tmdb_id": 1629, "confidence": "high", "fields": ("rating", "overview")},
    "Masculin Feminin": {"kind": "movie", "tmdb_id": 4710, "confidence": "high", "fields": ("poster", "genres")},
    "Your Name": {"kind": "movie", "tmdb_id": 372058, "confidence": "high", "fields": ()},
    "Monster": {"kind": "movie", "tmdb_id": 1050035, "confidence": "high", "fields": ("genres",)},
    "\u041a\u043e\u043d\u0444\u043b\u0438\u043a\u0442": {"kind": "movie", "tmdb_id": 553881, "confidence": "low", "fields": ()},
    "My One And Only Love": {"kind": "movie", "tmdb_id": 476351, "confidence": "high", "fields": ("poster", "genres", "rating", "overview")},
    "Cry, the Beloved Country": {"kind": "movie", "tmdb_id": 173893, "confidence": "high", "fields": ()},
    "Taxi T\u00e9h\u00e9ran": {"kind": "movie", "tmdb_id": 320006, "confidence": "high", "fields": ()},
    "metropolis": {"kind": "movie", "tmdb_id": 19, "confidence": "high", "fields": ()},
    "Cry, the Beloved Country 1995": {"kind": "movie", "tmdb_id": 34615, "confidence": "high", "fields": ("poster", "rating")},
}
DIRECTORS_DATABASE_TITLE = "Cinema Prive Directors"
DIRECTORS_PARENT_PAGE_TITLE = "Cinema Prive Metadata"
DIRECTOR_RELATION_PROPERTY = "Director Entry"
DIRECTOR_KEY_PROPERTY = "Director Key"
DIRECTOR_ALIASES_PROPERTY = "Aliases"
DIRECTOR_IMAGE_PROPERTY = "Profile Image"
DIRECTOR_TMDB_PERSON_ID_PROPERTY = "TMDb Person ID"
DIRECTOR_MIGRATION_REPORT_PREFIX = "director-migration"
GENRES_DATABASE_TITLE = "Cinema Prive Genres"
GENRE_RELATION_PROPERTY = "Genre Entry"
GENRE_KEY_PROPERTY = "Genre Key"
GENRE_ALIASES_PROPERTY = "Aliases"

YOUTUBE_DURATION_CACHE = {}
RUNTIME_CACHE_LOCK = threading.Lock()
CACHE_DATA_LOCK = threading.Lock()
RUNTIME_CACHE = {
    "initialized": False,
    "films": None,
    "library_films": {},
    "want_to_union_films": None,
    "youtube_playlists": {},
    "youtube_channel_debug": {},
    "youtube_section_picks": {},
    "refreshing": {}
}
GEMINI_MODEL_NAME_CACHE = None
TMDB_LOOKUP_CACHE = {}
TMDB_PERSON_LOOKUP_CACHE = {}
TMDB_COUNTRY_NAME_CACHE = None
TMDB_COUNTRY_DISPLAY_ALIASES = {
    "United States of America": "United States",
    "United Kingdom of Great Britain and Northern Ireland": "United Kingdom",
    "Korea (the Republic of)": "South Korea",
}
FILM_DISCUSSION_PROMPT = """â•”â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•—
â•‘        ðŸŽž  FILM DISCUSSION PROMPT        â•‘
â•šâ•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

You are my personal film discussion partner â€” a close friend
who loves cinema as much as I do. I just watched a film and
I want to unpack it with you.

Your tone: warm, curious, direct. Like two friends who take
films seriously but never make it feel like a lecture.
You have opinions â€” share them freely. But always make space
for mine too. This is a conversation, not a review.

â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”
  1. MY MOVIE LIST & RATINGS
â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”

  You have access to my full watched list and ratings below.
  Use it to make comparisons personal and specific â€” always
  connect the film we're discussing to films I've actually
  seen and rated.

  RATING MEANINGS:
  â€¢ god mode          â†’ changed my perspective on life
  â€¢ close to god mode â†’ deeply loved, nearly life-changing
  â€¢ masterpiece       â†’ excellent, loved it
  â€¢ Sweet / good      â†’ liked it
  â€¢ acceptable        â†’ neutral
  â€¢ naah / i don't like it â†’ disliked

â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”
  2. HOW TO START A DISCUSSION
â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”

  I will open with one of these:

  DISCUSS: [movie name]
  â†’ Start the discussion. Ask me one opening question
    to understand how the film landed for me before
    diving in. Then go deep.

  DISCUSS: [movie name] | [my initial reaction]
  â†’ I give you my first feeling. Respond to it directly,
    then open up the deeper conversation.

â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”
        return parts.join(' | ')
â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”

  Every discussion should naturally move through these:

  THEMES & PHILOSOPHY
  â†’ What is the film really about beneath the surface?
    What questions does it ask? What does it say about
    life, humanity, morality, existence?
    Connect to my taste â€” especially god mode films.

  DIRECTOR'S STYLE & TECHNIQUE
  â†’ How did the director tell this story visually?
    Camera, light, pace, silence, structure.
    What choices made it feel the way it felt?
    Compare to other directors whose work I've seen.

  Don't rush through both at once. Let the conversation
  breathe â€” go deep on one thread before moving to the next.

â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”
  4. OUTPUT FORMAT
â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”

  Each response has two parts:

  â”€ Ø§Ù„ÙÙŠÙ„Ù… â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
  [Arabic â€” your main discussion point or reaction.
   Casual, warm, like a friend texting you.
   3â€“5 sentences. No spoiler warnings needed â€”
   we both watched it.]

  â”€ Going deeper â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
  [English â€” the analytical layer.
   Themes, technique, comparisons to my watched list.
   Be specific: name the scene, name the shot, name
   the film from my list you're comparing to.
   End with ONE question to keep the conversation going.
   Never ask more than one question at a time.]

  â”€ What's on your mind? â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
  [Always end with this small command reminder:]

  AGREE Â· DISAGREE Â· GO DEEPER Â· NEXT TOPIC
  or just reply freely in any language

â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”
  5. ARABIC LANGUAGE RULES
â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”

  â€¢ Pure Arabic â€” no English words mid-sentence
  â€¢ Use Arabic punctuation (ØŒ not ,)
  â€¢ If English must appear, put it at the END only

  Handling names:
  â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
  â”‚ Film titles      â”‚ Keep in English as-is         â”‚
  â”‚                  â”‚ e.g. "Ø´Ø¨ÙŠÙ‡ Ø¨Ù€ The Godfather"  â”‚
  â”œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”¼â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”¤
  â”‚ Director names   â”‚ Arabic transliteration        â”‚
  â”‚                  â”‚ e.g. "ÙƒÙˆØ¨Ø±ÙŠÙƒ"                 â”‚
  â”œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”¼â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”¤
  â”‚ Actor names      â”‚ Arabic transliteration        â”‚
  â”‚                  â”‚ e.g. "Ø¯ÙŠ Ù†ÙŠØ±Ùˆ"                â”‚
  â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”´â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜

â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”
  6. COMMANDS
â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”

  DISCUSS: [movie name]
     â†’ start a new discussion

  DISCUSS: [movie name] | [my reaction]
     â†’ start with my first feeling

  AGREE
     â†’ you agree, build on it together

  DISAGREE
     â†’ you push back â€” defend your read of the film

  GO DEEPER
     â†’ stay on this thread, go further

  NEXT TOPIC
     â†’ move to the next discussion pillar

  COMPARE: [another film from my list]
     â†’ draw a direct comparison between the two films

  RATE IT
     â†’ after discussion, I'll tell you my rating â€”
       you react to it honestly

â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”
  7. RULES
â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”

  â€¢ Never summarize the plot â€” we both watched it
  â€¢ Never be generic â€” always tie back to specific scenes,
    shots, moments, or films from my list
  â€¢ Never ask more than one question per response
  â€¢ Share your opinion freely â€” don't hedge everything
  â€¢ If I disagree, engage seriously â€” don't just agree
    with me to be polite
  â€¢ Keep the energy of two friends who love cinema,
    not a professor and a student

â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”â”

â•”â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•—
â•‘       PASTE YOUR MOVIE LIST BELOW        â•‘
â•šâ•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•"""


def load_json_file(path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def save_json_file(path, payload):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.tmp")
    temp_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    temp_path.replace(path)


def backup_reading_data_file(reason="save"):
    if not READING_DATA_PATH.exists() or not READING_DATA_PATH.is_file():
        return ""
    READING_BACKUPS_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    safe_reason = re.sub(r"[^A-Za-z0-9_-]+", "-", str(reason or "save").strip().lower()).strip("-") or "save"
    backup_path = READING_BACKUPS_DIR / f"reading-data-{stamp}-{safe_reason}.json"
    try:
        shutil.copy2(READING_DATA_PATH, backup_path)
    except Exception:
        return ""
    backups = []
    for path in sorted(READING_BACKUPS_DIR.glob("reading-data-*.json"), key=lambda item: item.stat().st_mtime, reverse=True):
        backups.append(path)
    for old_path in backups[READING_BACKUP_KEEP_COUNT:]:
        try:
            old_path.unlink()
        except Exception:
            pass
    return str(backup_path)


def list_reading_backup_files(limit=READING_BACKUP_KEEP_COUNT):
    if not READING_BACKUPS_DIR.exists():
        return []
    paths = sorted(READING_BACKUPS_DIR.glob("reading-data-*.json"), key=lambda item: item.stat().st_mtime, reverse=True)
    items = []
    for path in paths[: max(int(limit or 0), 0)]:
        try:
            modified = datetime.fromtimestamp(path.stat().st_mtime).astimezone()
        except Exception:
            modified = None
        items.append({
            "path": str(path),
            "name": path.name,
            "label": format_timestamp_label(modified.isoformat() if modified else "", default=path.name) if modified else path.name,
            "size": path.stat().st_size,
        })
    return items


def load_reading_backup_payload():
    for backup in list_reading_backup_files(limit=READING_BACKUP_KEEP_COUNT):
        try:
            payload = json.loads(Path(backup["path"]).read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(payload, dict):
            return payload
    return None


def collect_reading_recovery_trace_hits(limit=50):
    candidate_paths = []
    for path in (READING_DATA_PATH,):
        if path.exists() and path.is_file():
            candidate_paths.append(path)
    for directory in (READING_BACKUPS_DIR,):
        if directory.exists():
            candidate_paths.extend(sorted(directory.glob("*.json")))
    for pattern in ("*reading*.json", "*rss*.json", "*feed*.json"):
        candidate_paths.extend(sorted(BASE_DIR.glob(pattern)))

    unique_paths = []
    seen_paths = set()
    for path in candidate_paths:
        try:
            resolved = str(path.resolve())
        except Exception:
            continue
        if resolved in seen_paths:
            continue
        seen_paths.add(resolved)
        unique_paths.append(path)

    hits = []
    seen_hits = set()
    for path in unique_paths:
        if len(hits) >= max(int(limit or 0), 0):
            break
        try:
            if path.stat().st_size > 8 * 1024 * 1024:
                continue
        except Exception:
            continue
        data = load_json_file(path, None)
        source_groups = []
        if isinstance(data, dict):
            for key in ("sources", "reading_sources", "reading_backup_files", "reading_source_list"):
                value = data.get(key, [])
                if isinstance(value, list):
                    source_groups.append((key, value))
        elif isinstance(data, list):
            source_groups.append(("items", data))

        for group_key, items in source_groups:
            for item in items:
                if not isinstance(item, dict):
                    continue
                name = str(item.get("name", "") or item.get("source", "") or "").strip()
                url = str(item.get("url", "") or item.get("feed_url", "") or "").strip()
                if not name and not url:
                    continue
                hit_key = (name.lower(), normalize_reading_url(url).lower(), str(path))
                if hit_key in seen_hits:
                    continue
                seen_hits.add(hit_key)
                hits.append({
                    "name": name,
                    "url": normalize_reading_url(url),
                    "file": path.name,
                    "path": str(path),
                    "group": group_key,
                })
                if len(hits) >= max(int(limit or 0), 0):
                    break
            if len(hits) >= max(int(limit or 0), 0):
                break
    return hits


def init_db():
    connection = sqlite3.connect(str(CHAT_HISTORY_DB_PATH))
    try:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS chat_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category TEXT NOT NULL,
                role TEXT NOT NULL,
                message TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        connection.commit()
    finally:
        connection.close()


def save_message(category, role, message):
    if not category or not role or not str(message or "").strip():
        return
    connection = sqlite3.connect(str(CHAT_HISTORY_DB_PATH))
    try:
        connection.execute(
            "INSERT INTO chat_history (category, role, message, created_at) VALUES (?, ?, ?, ?)",
            (category, role, str(message).strip(), current_timestamp())
        )
        connection.commit()
    finally:
        connection.close()


def get_recent_messages(category, limit=30):
    safe_limit = max(int(limit), 1)
    connection = sqlite3.connect(str(CHAT_HISTORY_DB_PATH))
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """
            SELECT id, category, role, message, created_at
            FROM chat_history
            WHERE category = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (category, safe_limit)
        ).fetchall()
    finally:
        connection.close()
    results = []
    for row in reversed(rows):
        results.append({
            "id": row["id"],
            "category": row["category"],
            "role": row["role"],
            "message": row["message"],
            "created_at": row["created_at"],
        })
    return results


init_db()


def current_timestamp():
    return datetime.now().astimezone().isoformat(timespec="seconds")


def parse_timestamp(value):
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = f"{raw[:-1]}+00:00"
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        pass
    try:
        return parsedate_to_datetime(raw)
    except Exception:
        return None


def normalize_timestamp_value(value):
    timestamp = parse_timestamp(value)
    if not timestamp:
        return str(value or "").strip()
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=datetime.now().astimezone().tzinfo)
    return timestamp.astimezone().isoformat(timespec="seconds")


def format_timestamp_label(value, default=""):
    timestamp = parse_timestamp(value)
    if not timestamp:
        return default
    try:
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=datetime.now().astimezone().tzinfo)
        return timestamp.astimezone().strftime("%b %d, %Y %H:%M")
    except Exception:
        return default


READING_STATUSES = ("unread", "reading", "finished", "archived")
READING_CATEGORIES = ("news", "culture", "opinion")
READING_DEMO_SOURCE_URLS = {
    "https://www.themarginalian.org/feed/",
    "https://www.quantamagazine.org/feed/",
}
READING_DEMO_ENTRY_URLS = {
    "https://example.com/attention-practice",
    "https://example.com/math-ideas",
    "https://example.com/saved-article",
}
READING_CATEGORY_LABELS = {
    "news": "News",
    "culture": "Culture",
    "opinion": "Opinion",
}
READING_CONTENT_CACHE_STATUSES = {"ok", "partial", "feed", "failed"}


def normalize_reading_category(value):
    normalized = str(value or "").strip().lower()
    if normalized in READING_CATEGORIES:
        return normalized
    return "news"


def reading_category_label(value):
    return READING_CATEGORY_LABELS.get(normalize_reading_category(value), "News")


def normalize_reading_topic(value, category=""):
    raw_topic = str(value or "").strip()
    if not raw_topic:
        return ""
    compact_topic = re.sub(r"[^a-z0-9]+", "", raw_topic.lower())
    normalized_category = normalize_reading_category(category)
    category_aliases = {
        "news": {"news", "new", "latest", "updates", "update", "stories", "topstories", "headline", "headlines"},
        "culture": {"culture", "arts", "art", "music", "books", "book", "film", "films", "media"},
        "opinion": {"opinion", "opinions", "editorial", "editorials", "commentary", "analysis", "views"},
    }
    if compact_topic in category_aliases.get(normalized_category, set()):
        return ""
    if compact_topic in {"general", "misc", "other", "inbox", "default", "topic"}:
        return ""
    if compact_topic == normalized_category:
        return ""
    return raw_topic


def reading_visible_topic_label(topic, category=""):
    raw_topic = str(topic or "").strip()
    if not raw_topic:
        return ""
    normalized_topic = normalize_reading_topic(raw_topic, category).lower()
    normalized_category = normalize_reading_category(category)
    if normalized_topic in READING_CATEGORIES:
        return ""
    if normalized_topic == normalized_category:
        return ""
    return raw_topic


class ReadingHTMLExtractor(HTMLParser):
    def __init__(self, base_url=""):
        super().__init__(convert_charrefs=True)
        self.base_url = str(base_url or "").strip()
        self.skip_stack = []
        self.current_tag = ""
        self.current_link = ""
        self.in_paragraph = False
        self.images = []
        self.image_candidates = []
        self.author_images = []
        self.author_image_candidates = []
        self.meta = {}
        self.paragraphs = []
        self._paragraph_parts = []

    def handle_starttag(self, tag, attrs):
        tag = (tag or "").lower()
        attrs_dict = {str(key).lower(): str(value or "").strip() for key, value in attrs}
        self.current_tag = tag
        if tag in {"script", "style", "noscript", "svg", "nav", "footer", "header", "aside"}:
            self.skip_stack.append(tag)
            return
        if tag == "meta":
            key = attrs_dict.get("property") or attrs_dict.get("name")
            content = attrs_dict.get("content", "")
            if key and content:
                self.meta[key.lower()] = content
        style_urls = reading_extract_css_image_urls(attrs_dict.get("style", ""))
        if style_urls:
            for style_url in style_urls:
                if style_url:
                    self.image_candidates.append({
                        "src": style_url,
                        "alt": attrs_dict.get("alt", ""),
                        "title": attrs_dict.get("title", ""),
                        "class": attrs_dict.get("class", ""),
                        "id": attrs_dict.get("id", ""),
                        "width": attrs_dict.get("width", ""),
                        "height": attrs_dict.get("height", ""),
                        "itemprop": attrs_dict.get("itemprop", ""),
                        "data-testid": attrs_dict.get("data-testid", ""),
                        "loading": attrs_dict.get("loading", ""),
                    })
        if tag == "source":
            src = reading_best_image_url_from_attrs(attrs_dict, base_url=self.base_url)
            if src:
                self.image_candidates.append({
                    "src": src,
                    "alt": attrs_dict.get("alt", ""),
                    "title": attrs_dict.get("title", ""),
                    "class": attrs_dict.get("class", ""),
                    "id": attrs_dict.get("id", ""),
                    "width": attrs_dict.get("width", ""),
                    "height": attrs_dict.get("height", ""),
                    "itemprop": attrs_dict.get("itemprop", ""),
                    "data-testid": attrs_dict.get("data-testid", ""),
                    "loading": attrs_dict.get("loading", ""),
                })
        if tag == "img":
            src = reading_best_image_url_from_attrs(attrs_dict, base_url=self.base_url)
            if src:
                self.images.append(src)
                self.image_candidates.append({
                    "src": src,
                    "alt": attrs_dict.get("alt", ""),
                    "title": attrs_dict.get("title", ""),
                    "class": attrs_dict.get("class", ""),
                    "id": attrs_dict.get("id", ""),
                    "width": attrs_dict.get("width", ""),
                    "height": attrs_dict.get("height", ""),
                    "itemprop": attrs_dict.get("itemprop", ""),
                    "data-testid": attrs_dict.get("data-testid", ""),
                    "loading": attrs_dict.get("loading", ""),
                })
                attrs_blob = " ".join(
                    str(attrs_dict.get(key, "") or "")
                    for key in ("class", "id", "alt", "title", "aria-label", "itemprop", "data-testid")
                ).lower()
                if any(token in attrs_blob for token in ("author", "avatar", "byline", "profile", "person", "user")):
                    self.author_images.append(src)
                    self.author_image_candidates.append({
                        "src": src,
                        "alt": attrs_dict.get("alt", ""),
                        "title": attrs_dict.get("title", ""),
                        "class": attrs_dict.get("class", ""),
                        "id": attrs_dict.get("id", ""),
                        "width": attrs_dict.get("width", ""),
                        "height": attrs_dict.get("height", ""),
                        "itemprop": attrs_dict.get("itemprop", ""),
                        "data-testid": attrs_dict.get("data-testid", ""),
                    })
        if tag == "source":
            src = reading_best_image_url_from_attrs(attrs_dict, base_url=self.base_url)
            if src and not reading_is_bad_image_candidate(src, attrs=attrs_dict, article_url=self.base_url):
                self.image_candidates.append({
                    "src": src,
                    "alt": attrs_dict.get("alt", ""),
                    "title": attrs_dict.get("title", ""),
                    "class": attrs_dict.get("class", ""),
                    "id": attrs_dict.get("id", ""),
                    "width": attrs_dict.get("width", ""),
                    "height": attrs_dict.get("height", ""),
                    "itemprop": attrs_dict.get("itemprop", ""),
                    "data-testid": attrs_dict.get("data-testid", ""),
                    "loading": attrs_dict.get("loading", ""),
                })
        if tag == "a":
            self.current_link = attrs_dict.get("href", "")
        if tag == "p":
            self.in_paragraph = True
            self._paragraph_parts = []

    def handle_endtag(self, tag):
        tag = (tag or "").lower()
        if self.skip_stack and self.skip_stack[-1] == tag:
            self.skip_stack.pop()
            return
        if tag == "p":
            paragraph = normalize_reading_space(" ".join(self._paragraph_parts))
            if len(paragraph) >= 35:
                self.paragraphs.append(paragraph)
            self._paragraph_parts = []
            self.in_paragraph = False
        if tag == "a":
            self.current_link = ""
        self.current_tag = ""

    def handle_data(self, data):
        if self.skip_stack:
            return
        text = normalize_reading_space(data)
        if not text:
            return
        if self.in_paragraph:
            self._paragraph_parts.append(text)


class ReadingArticleFragmentExtractor(HTMLParser):
    article_tokens = (
        "article", "article-body", "article-content", "body-content", "content-body",
        "post-content", "entry-content", "story-content", "main-content", "wysiwyg",
        "wysiwyg-content", "article__body", "article-body__content", "field--name-body",
    )

    def __init__(self):
        super().__init__(convert_charrefs=False)
        self.fragments = []
        self.active = []

    def _attrs_dict(self, attrs):
        return {str(key or "").lower(): str(value or "") for key, value in attrs or []}

    def _attrs_html(self, attrs):
        rendered = []
        for key, value in attrs or []:
            key = str(key or "").strip()
            if key:
                rendered.append(f' {key}="{html_escape(value, quote=True)}"')
        return "".join(rendered)

    def _is_candidate_start(self, tag, attrs):
        tag = str(tag or "").lower()
        attrs_dict = self._attrs_dict(attrs)
        blob = " ".join(
            str(attrs_dict.get(key, "") or "")
            for key in ("class", "id", "itemprop", "role", "data-testid")
        ).lower()
        if tag in {"article", "main"}:
            return True
        return tag in {"section", "div"} and any(token in blob for token in self.article_tokens)

    def _append_to_active(self, markup):
        for fragment in self.active:
            fragment["parts"].append(markup)

    def handle_starttag(self, tag, attrs):
        tag = str(tag or "").lower()
        markup = f"<{tag}{self._attrs_html(attrs)}>"
        self._append_to_active(markup)
        for fragment in self.active:
            fragment["depth"] += 1
        if self._is_candidate_start(tag, attrs):
            self.active.append({"depth": 1, "parts": [markup]})

    def handle_startendtag(self, tag, attrs):
        tag = str(tag or "").lower()
        markup = f"<{tag}{self._attrs_html(attrs)}>"
        self._append_to_active(markup)
        if self._is_candidate_start(tag, attrs):
            self.fragments.append(markup)

    def handle_endtag(self, tag):
        tag = str(tag or "").lower()
        markup = f"</{tag}>"
        self._append_to_active(markup)
        finished = []
        for fragment in self.active:
            fragment["depth"] -= 1
            if fragment["depth"] <= 0:
                finished.append(fragment)
        if not finished:
            return
        self.active = [fragment for fragment in self.active if fragment not in finished]
        for fragment in finished:
            value = "".join(fragment.get("parts", [])).strip()
            if value:
                self.fragments.append(value)

    def handle_data(self, data):
        self._append_to_active(html_escape(data))

    def handle_entityref(self, name):
        self._append_to_active(f"&{name};")

    def handle_charref(self, name):
        self._append_to_active(f"&#{name};")

    def close(self):
        super().close()
        for fragment in self.active:
            value = "".join(fragment.get("parts", [])).strip()
            if value:
                self.fragments.append(value)
        self.active = []


class ReadingArticleHTMLSanitizer(HTMLParser):
    allowed_inline = {"a", "strong", "em", "b", "i", "code", "br"}
    allowed_blocks = {"p", "div", "section", "article", "main", "h1", "h2", "h3", "h4", "h5", "h6", "blockquote", "ul", "ol", "li", "figure", "figcaption"}
    media_hosts = ("player.vimeo.com", "vimeo.com")

    def __init__(self, base_url="", hero_image="", author_image=""):
        super().__init__(convert_charrefs=True)
        self.base_url = str(base_url or "").strip()
        self.hero_image = normalize_reading_url(hero_image)
        self.author_image = normalize_reading_url(author_image)
        self.output = []
        self.skip_stack = []
        self.open_tags = []
        self.seen_images = set()
        self.seen_image_variants = set()
        self.seen_media = set()
        self.seen_youtube_ids = set()
        self.pending_youtube_fallbacks = {}
        self.pending_embedded_placeholder = False
        self.visible_media_emitted = False
        if self.hero_image:
            self.seen_images.add(normalize_reading_image_identity(self.hero_image))
            self.seen_image_variants.add(normalize_reading_image_variant_key(self.hero_image))
        if self.author_image:
            self.seen_images.add(normalize_reading_image_identity(self.author_image))
            self.seen_image_variants.add(normalize_reading_image_variant_key(self.author_image))

    def _append_attrs(self, attrs):
        rendered = []
        for key, value in attrs:
            value = str(value or "").strip()
            if value:
                rendered.append(f' {key}="{html_escape(value, quote=True)}"')
        return "".join(rendered)

    def _start(self, tag, attrs=None):
        attrs = attrs or []
        self.output.append(f"<{tag}{self._append_attrs(attrs)}>")
        if tag != "br":
            self.open_tags.append(tag)

    def _media_label(self, url):
        host = urllib.parse.urlsplit(str(url or "")).netloc.lower()
        if reading_extract_youtube_media_id(url):
            return "YouTube video"
        if reading_host_matches(host, self.media_hosts):
            return "Embedded video"
        return "Embedded media"

    def _youtube_id(self, url):
        return reading_extract_youtube_media_id(url)

    def _youtube_thumbnail_url(self, video_id):
        video_id = re.sub(r"[^A-Za-z0-9_-]", "", str(video_id or ""))
        if not video_id:
            return ""
        return f"https://img.youtube.com/vi/{html_escape(video_id, quote=True)}/hqdefault.jpg"

    def _queue_youtube_fallback(self, url):
        media_url = absolutize_reading_url(url, self.base_url)
        video_id = self._youtube_id(media_url)
        if not video_id:
            return False
        if video_id in self.seen_youtube_ids:
            return True
        if video_id not in self.pending_youtube_fallbacks:
            self.pending_youtube_fallbacks[video_id] = {
                "video_id": video_id,
                "watch_url": f"https://www.youtube.com/watch?v={html_escape(video_id, quote=True)}",
                "thumbnail_url": self._youtube_thumbnail_url(video_id),
            }
        return True

    def _tag_is_junk(self, tag, attrs_dict):
        tag = str(tag or "").lower()
        attrs_blob = " ".join(
            str(attrs_dict.get(key, "") or "")
            for key in ("class", "id", "role", "aria-label", "title", "data-testid", "data-track-label")
        ).lower()
        junk_tokens = (
            "share", "save", "bookmark", "follow", "subscribe", "newsletter", "print", "comment", "comments",
            "toolbar", "sticky", "floating", "action", "actions", "menu", "modal", "popup", "overlay",
            "related", "recommend", "read-more", "continue-reading", "more-link", "social", "cookie", "consent",
            "post-bottom-meta", "post-bottom-tags", "tagcloud",
        )
        tag_tokens = ("share", "save", "bookmark", "follow", "subscribe", "print", "comment", "toolbar", "sticky", "floating", "menu")
        if any(token in attrs_blob for token in junk_tokens):
            return True
        if tag == "a":
            href = str(attrs_dict.get("href", "") or "").strip().lower()
            if any(token in attrs_blob for token in ("more-link", "read-more", "continue-reading", "button")):
                return True
            if href.startswith(("javascript:", "#")):
                return True
        return False

    def _text_is_junk(self, text):
        value = normalize_reading_space(text).lower()
        if not value:
            return True
        if len(value) <= 1 and value not in {"...", "»", "›"}:
            return True
        junk_exact = {
            "save", "share", "bookmark", "follow", "subscribe", "print", "comments", "comment", "more",
            "read more", "continue reading", "open media", "embedded media", "open on youtube",
            "أكمل القراءة", "اقرأ المزيد", "شارك", "حفظ", "تعليقات", "المزيد", "تابع", "اشترك",
        }
        if value in junk_exact:
            return True
        junk_phrases = (
            "منشورة على", "published on", "posted on", "article published on", "shared on", "follow us", "sign up",
        )
        return any(phrase in value for phrase in junk_phrases)

    def _render_youtube_block(self, url):
        video_id = self._youtube_id(url)
        if not video_id or video_id in self.seen_youtube_ids:
            return False
        self.seen_youtube_ids.add(video_id)
        self.pending_youtube_fallbacks.pop(video_id, None)
        self.seen_media.add(video_id)
        self.pending_embedded_placeholder = False
        self.visible_media_emitted = True
        embed_url = f"https://www.youtube-nocookie.com/embed/{html_escape(video_id, quote=True)}"
        self.output.append(
            '<div class="reading-youtube-embed">'
            f'<iframe src="{embed_url}" loading="lazy" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" allowfullscreen referrerpolicy="strict-origin-when-cross-origin"></iframe>'
            '</div>'
        )
        return True

    def _render_media_placeholder(self, url):
        media_url = absolutize_reading_url(url, self.base_url)
        if not media_url:
            return False
        media_key = normalize_reading_url(media_url)
        if media_key in self.seen_media:
            return True
        self.seen_media.add(media_key)
        label = self._media_label(media_url)
        if label == "YouTube video":
            if self._queue_youtube_fallback(media_url):
                return True
        self.visible_media_emitted = True
        if label != "YouTube video":
            self.output.append(
                f'<div class="reading-media-placeholder"><span>{html_escape(label)}</span><a href="{html_escape(media_url, quote=True)}" target="_blank" rel="noopener">Open media</a></div>'
            )
            return True
        return True

    def _render_inline_image(self, image_url, attrs_dict=None):
        attrs_dict = attrs_dict if isinstance(attrs_dict, dict) else {}
        image_url = absolutize_reading_url(image_url, self.base_url)
        if reading_is_bad_image_candidate(image_url, attrs=attrs_dict, article_url=self.base_url):
            return False
        image_key = normalize_reading_image_identity(image_url)
        image_variant_key = normalize_reading_image_variant_key(image_url)
        youtube_id = self._youtube_id(image_url)
        if youtube_id:
            if youtube_id in self.seen_youtube_ids:
                return False
            if self._queue_youtube_fallback(image_url):
                return False
        if not image_url or image_key in self.seen_images or image_variant_key in self.seen_image_variants:
            return False
        self.seen_images.add(image_key)
        self.seen_image_variants.add(image_variant_key)
        alt = attrs_dict.get("alt", "")
        image_markup = f'<img src="{html_escape(image_url, quote=True)}" alt="{html_escape(alt, quote=True)}" loading="lazy" referrerpolicy="no-referrer">'
        if "figure" in self.open_tags:
            self.output.append(image_markup)
        else:
            self.output.append(f'<figure class="reading-inline-media">{image_markup}</figure>')
        return True

    def handle_starttag(self, tag, attrs):
        tag = (tag or "").lower()
        attrs_dict = {str(key).lower(): str(value or "").strip() for key, value in attrs}
        if self.skip_stack:
            self.skip_stack.append(tag)
            return
        if tag in {"script", "style", "noscript", "svg", "form", "button", "input", "select", "textarea", "nav", "header", "footer", "aside"}:
            self.skip_stack.append(tag)
            return
        if self._tag_is_junk(tag, attrs_dict):
            self.skip_stack.append(tag)
            return
        if tag in self.allowed_blocks:
            self._start(tag, [("dir", "auto")])
            return
        if tag == "a":
            href = absolutize_reading_url(attrs_dict.get("href", ""), self.base_url)
            if href and href.startswith(("http://", "https://")):
                self._start("a", [("href", href), ("target", "_blank"), ("rel", "noopener")])
                return
            self._start("span", [])
            return
        if tag in {"strong", "em", "b", "i", "code"}:
            self._start(tag)
            return
        if tag == "br":
            self.output.append("<br>")
            return
        if tag == "source":
            src = reading_best_image_url_from_attrs(attrs_dict, base_url=self.base_url)
            if self._render_inline_image(src, attrs_dict):
                return
        for style_url in reading_extract_css_image_urls(attrs_dict.get("style", "")):
            if self._render_inline_image(style_url, attrs_dict):
                return
        if tag == "img":
            src = reading_best_image_url_from_attrs(attrs_dict, base_url=self.base_url)
            self._render_inline_image(src, attrs_dict)
            return
        if tag in {"iframe", "embed", "video", "audio"}:
            src = absolutize_reading_url(attrs_dict.get("src", ""), self.base_url)
            host = urllib.parse.urlsplit(src).netloc.lower()
            if self._render_youtube_block(src):
                return
            if src and tag == "iframe" and reading_host_matches(host, self.media_hosts):
                self.output.append(
                    f'<div class="reading-embed"><iframe src="{html_escape(src, quote=True)}" loading="lazy" allowfullscreen referrerpolicy="no-referrer"></iframe></div>'
                )
            elif src:
                self._render_media_placeholder(src)

    def handle_startendtag(self, tag, attrs):
        self.handle_starttag(tag, attrs)
        if tag.lower() in self.open_tags:
            self.handle_endtag(tag)

    def handle_endtag(self, tag):
        tag = (tag or "").lower()
        if self.skip_stack:
            self.skip_stack.pop()
            return
        close_tag = "span" if tag == "a" and self.open_tags and self.open_tags[-1] == "span" else tag
        if close_tag in self.open_tags:
            while self.open_tags:
                current = self.open_tags.pop()
                self.output.append(f"</{current}>")
                if current == close_tag:
                    break

    def handle_data(self, data):
        if self.skip_stack:
            return
        text = str(data or "")
        if text.strip():
            normalized = normalize_reading_space(text)
            if normalized.lower() in {"[embedded content]", "embedded content"}:
                self.pending_embedded_placeholder = True
            else:
                if self._text_is_junk(text):
                    return
                self.output.append(html_escape(text))

    def html(self):
        for video_id, fallback in list(self.pending_youtube_fallbacks.items()):
            if video_id in self.seen_youtube_ids:
                continue
            thumbnail_url = fallback.get("thumbnail_url", "")
            watch_url = fallback.get("watch_url", "")
            self.output.append(
                '<div class="reading-media-card">'
                + (f'<img src="{thumbnail_url}" alt="" loading="lazy" referrerpolicy="no-referrer">' if thumbnail_url else "")
                + '<div><span>YouTube video</span>'
                f'<a href="{html_escape(watch_url, quote=True)}" target="_blank" rel="noopener">Open on YouTube</a></div>'
                '</div>'
            )
            self.visible_media_emitted = True
        self.pending_youtube_fallbacks = {
            video_id: fallback for video_id, fallback in self.pending_youtube_fallbacks.items()
            if video_id not in self.seen_youtube_ids
        }
        if self.pending_embedded_placeholder and not self.visible_media_emitted:
            self.output.append('<div class="reading-media-placeholder"><span>Embedded media</span><span>Open original to play this media</span></div>')
        while self.open_tags:
            self.output.append(f"</{self.open_tags.pop()}>")
        return "".join(self.output).strip()


def normalize_reading_space(value):
    return re.sub(r"\s+", " ", unescape(str(value or ""))).strip()


def _coerce_int(value, default=0):
    try:
        return int(str(value or "").strip() or default)
    except Exception:
        return int(default)


def html_escape(value, quote=False):
    return (
        str(value or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;" if quote else '"')
    )


def strip_reading_html(value):
    parser = ReadingHTMLExtractor()
    try:
        parser.feed(str(value or ""))
        parser.close()
    except Exception:
        return normalize_reading_space(re.sub(r"<[^>]+>", " ", str(value or "")))
    if parser.paragraphs:
        return "\n\n".join(parser.paragraphs)
    return normalize_reading_space(re.sub(r"<[^>]+>", " ", str(value or "")))


def absolutize_reading_url(url, base_url=""):
    raw = str(url or "").strip()
    if not raw:
        return ""
    return urllib.parse.urljoin(str(base_url or "").strip(), raw)


def normalize_reading_image_identity(value):
    normalized = normalize_reading_url(unwrap_reading_image_proxy_url(value))
    if not normalized:
        return ""
    parsed = urllib.parse.urlsplit(normalized)
    trivial_params = {
        "auto", "crop", "fit", "format", "h", "height", "ixid", "ixlib", "quality", "resize", "ssl",
        "w", "width",
    }
    query_pairs = [
        (key, val)
        for key, val in urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
        if key.lower() not in trivial_params and not key.lower().startswith(("utm_", "fbclid", "gclid"))
    ]
    return urllib.parse.urlunsplit((
        parsed.scheme,
        parsed.netloc,
        parsed.path,
        urllib.parse.urlencode(query_pairs, doseq=True),
        "",
    ))


def normalize_reading_image_variant_key(value):
    identity = normalize_reading_image_identity(value)
    if not identity:
        return ""
    parsed = urllib.parse.urlsplit(identity)
    host = parsed.netloc.lower()
    path = urllib.parse.unquote(parsed.path or "").lower()
    if reading_host_matches(host, ("ychef.files.bbci.co.uk", "ichef.bbci.co.uk")):
        bbc_image_id = re.search(r"/(p[0-9a-z]{6,})(?:\.[a-z0-9]+)+(?:\.webp)?$", path)
        if bbc_image_id:
            return f"bbc-image:{bbc_image_id.group(1)}"
    path = re.sub(r'(?i)-\d{2,5}x\d{2,5}(?=\.[a-z0-9]{2,5}$)', "", path)
    path = re.sub(r'(?i)@[12]x(?=\.[a-z0-9]{2,5}$)', "", path)
    path = re.sub(r'(?i)/\d{2,5}x\d{2,5}(?=/)', "/", path)
    path = re.sub(r'(?i)/(?:ic|ace|live)/\d{2,5}x(?:\d{2,5}|n)(?=/)', "/", path)
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))


def reading_image_dimension_hint(url, attrs=None):
    attrs = attrs if isinstance(attrs, dict) else {}
    width = _coerce_int(attrs.get("width", 0))
    height = _coerce_int(attrs.get("height", 0))
    parsed = urllib.parse.urlsplit(str(url or ""))
    query = urllib.parse.parse_qs(parsed.query)
    for key in ("w", "width"):
        if query.get(key):
            width = max(width, _coerce_int(query[key][0]))
    for key in ("h", "height"):
        if query.get(key):
            height = max(height, _coerce_int(query[key][0]))
    resize_values = query.get("resize", []) + query.get("fit", [])
    for value in resize_values:
        nums = [_coerce_int(part) for part in re.findall(r"\d{2,5}", str(value or ""))]
        if len(nums) >= 2:
            width = max(width, nums[0])
            height = max(height, nums[1])
    path_nums = re.findall(r"(?i)(?:-|_)(\d{2,5})x(\d{2,5})(?=\.[a-z0-9]{2,5}|[?&]|$)", urllib.parse.unquote(parsed.path or ""))
    for raw_width, raw_height in path_nums:
        width = max(width, _coerce_int(raw_width))
        height = max(height, _coerce_int(raw_height))
    return width, height


def reading_image_variant_quality(url, attrs=None, article_url="", source_url="", source_name="", kind=""):
    image_url = absolutize_reading_url(unwrap_reading_image_proxy_url(url), article_url)
    if reading_is_bad_image_candidate(image_url, attrs=attrs, article_url=article_url, source_url=source_url, source_name=source_name):
        return -10_000
    parsed = urllib.parse.urlsplit(image_url)
    path = f"{parsed.path} {parsed.query}".lower()
    width, height = reading_image_dimension_hint(image_url, attrs=attrs)
    score = 100
    if width and height:
        score += min(width * height / 1200, 900)
        score += min(max(width, height) / 10, 140)
    if kind in {"explicit", "feed_cover", "current_hero"}:
        score += 30
    if any(token in path for token in ("blur", "blurred", "placeholder", "lowres", "low-res")):
        score -= 260
    if any(token in path for token in ("thumb", "thumbnail", "small")):
        score -= 65
    query_values = urllib.parse.parse_qs(parsed.query)
    quality_values = query_values.get("quality", []) + query_values.get("q", [])
    if quality_values:
        score += min(_coerce_int(quality_values[0]), 100) / 2
    return score


def unwrap_reading_image_proxy_url(value):
    raw = str(value or "").strip()
    if not raw:
        return ""
    parsed = urllib.parse.urlsplit(raw)
    host = parsed.netloc.lower()
    query = urllib.parse.parse_qs(parsed.query)
    for key in ("url", "u", "image", "img", "src", "source"):
        if query.get(key):
            candidate = str(query[key][0] or "").strip()
            if candidate.startswith(("http://", "https://")):
                return candidate
    decoded_path = urllib.parse.unquote(parsed.path or "")
    embedded = re.search(r"https?://.+", decoded_path)
    if embedded and (
        "proxy" in host
        or "shortpixel" in host
        or "image" in host
        or "cdn" in host
        or "/http" in decoded_path
    ):
        return embedded.group(0)
    return raw


def reading_candidate_image_sources(attrs):
    attrs = attrs if isinstance(attrs, dict) else {}
    candidates = []
    for key in ("data-src", "data-lazy-src", "data-original", "data-image", "data-url", "data-echo", "data-large-file", "data-full-url", "src"):
        value = str(attrs.get(key, "") or "").strip()
        if value:
            candidates.append(value)
    for key in ("data-srcset", "srcset"):
        value = str(attrs.get(key, "") or "").strip()
        candidates.extend(reading_parse_srcset_candidates(value))
    return list(dict.fromkeys(candidates))


def reading_parse_srcset_candidates(value):
    raw = str(value or "").strip()
    if not raw:
        return []
    parsed_candidates = []
    for item in raw.split(","):
        part = item.strip()
        if not part:
            continue
        bits = part.split()
        candidate = bits[0].strip()
        descriptor = bits[1].strip().lower() if len(bits) > 1 else ""
        score = 0
        if descriptor.endswith("w"):
            score = _coerce_int(descriptor[:-1])
        elif descriptor.endswith("x"):
            try:
                score = int(float(descriptor[:-1]) * 1000) if descriptor[:-1] else 0
            except Exception:
                score = 0
        parsed_candidates.append((score, candidate))
    parsed_candidates.sort(key=lambda item: item[0], reverse=True)
    return [candidate for _, candidate in parsed_candidates if candidate]


def reading_extract_css_image_urls(style_value):
    style = str(style_value or "").strip()
    if not style:
        return []
    urls = []
    for match in re.finditer(r'url\((["\']?)(.*?)\1\)', style, flags=re.IGNORECASE):
        raw = str(match.group(2) or "").strip()
        if not raw or raw.lower() in {"none", "unset", "initial"}:
            continue
        urls.append(raw)
    return list(dict.fromkeys(urls))


def reading_best_image_url_from_attrs(attrs, base_url="", article_url="", source_url="", source_name=""):
    attrs = attrs if isinstance(attrs, dict) else {}
    fallback = ""
    for candidate in reading_candidate_image_sources(attrs):
        image_url = absolutize_reading_url(candidate, base_url or article_url)
        if not fallback:
            fallback = image_url
        if not reading_is_bad_image_candidate(
            image_url,
            attrs=attrs,
            article_url=article_url or base_url,
            source_url=source_url,
            source_name=source_name,
        ):
            return image_url
    return fallback


def reading_host_matches(host, allowed_hosts):
    host = str(host or "").lower().strip()
    for allowed in allowed_hosts:
        allowed = str(allowed or "").lower().strip()
        if host == allowed or host.endswith(f".{allowed}"):
            return True
    return False


def reading_extract_youtube_id(url):
    raw = str(url or "").strip()
    if not raw:
        return ""
    parsed = urllib.parse.urlsplit(raw)
    host = parsed.netloc.lower()
    path_parts = [part for part in parsed.path.split("/") if part]
    query = urllib.parse.parse_qs(parsed.query)
    candidate = ""
    if reading_host_matches(host, ("youtu.be",)):
        candidate = path_parts[0] if path_parts else ""
    elif reading_host_matches(host, ("youtube.com", "youtube-nocookie.com")):
        if "v" in query and query["v"]:
            candidate = query["v"][0]
        elif path_parts and path_parts[0] in {"embed", "shorts", "live", "v"} and len(path_parts) > 1:
            candidate = path_parts[1]
    candidate = re.sub(r"[^A-Za-z0-9_-]", "", str(candidate or ""))
    return candidate if 6 <= len(candidate) <= 32 else ""


def reading_extract_youtube_thumbnail_id(url):
    raw = str(url or "").strip()
    if not raw:
        return ""
    parsed = urllib.parse.urlsplit(raw)
    host = parsed.netloc.lower()
    path_parts = [part for part in parsed.path.split("/") if part]
    candidate = ""
    if reading_host_matches(host, ("img.youtube.com", "i.ytimg.com")) and len(path_parts) >= 3 and path_parts[0] in {"vi", "vi_webp"}:
        candidate = path_parts[1]
    candidate = re.sub(r"[^A-Za-z0-9_-]", "", str(candidate or ""))
    return candidate if 6 <= len(candidate) <= 32 else ""


def reading_extract_youtube_media_id(url):
    return reading_extract_youtube_id(url) or reading_extract_youtube_thumbnail_id(url)


def reading_extract_youtube_urls_from_text(value):
    text = str(value or "")
    return re.findall(r"https?://(?:www\.)?(?:youtube\.com|youtu\.be|youtube-nocookie\.com)/[^\s<>'\")]+", text, flags=re.IGNORECASE)


def reading_source_thumbnail_host(url="", article_url="", source_name=""):
    article_host = str(urllib.parse.urlsplit(str(article_url or "").strip()).netloc or "").lower().strip()
    source_host = str(urllib.parse.urlsplit(str(url or "").strip()).netloc or "").lower().strip()
    if article_host:
        return article_host
    if source_host:
        return source_host
    fallback = str(source_name or "").strip().lower()
    return fallback


def reading_image_domain_hint(host):
    host = str(host or "").lower().strip()
    host = host[4:] if host.startswith("www.") else host
    if host.endswith("aljazeera.net") or host.endswith("aljazeera.com"):
        return {
            "prefer": ("wp-content/uploads", "/uploads/", "/media/", "/images/"),
            "reject": ("logo", "avatar", "icon", "favicon", "sprite", "placeholder"),
        }
    if host.endswith("moroccoworldnews.com"):
        return {
            "prefer": ("wp-content/uploads", "/uploads/", "/images/"),
            "reject": ("logo", "avatar", "icon", "favicon", "sprite", "placeholder"),
        }
    if host.endswith("howiyapress.com"):
        return {
            "prefer": ("wp-content/uploads", "/uploads/", "/images/"),
            "reject": ("logo", "avatar", "icon", "favicon", "sprite", "placeholder"),
        }
    if host.endswith("assabah.ma") or host.endswith("alousboue.ma"):
        return {
            "prefer": ("wp-content/uploads", "/uploads/", "/images/"),
            "reject": ("logo", "avatar", "icon", "favicon", "sprite", "placeholder"),
        }
    if host.endswith("hespress.com"):
        return {
            "prefer": ("wp-content/uploads", "/uploads/", "/images/"),
            "reject": ("logo", "avatar", "icon", "favicon", "sprite", "placeholder"),
        }
    return {
        "prefer": ("wp-content/uploads", "/uploads/", "/images/", "/media/", "wp-content"),
        "reject": ("logo", "avatar", "icon", "favicon", "sprite", "placeholder", "tracking", "pixel", "spacer", "default", "blank"),
    }


def reading_is_bad_image_candidate(url, attrs=None, article_url="", source_url="", source_name=""):
    raw = unwrap_reading_image_proxy_url(url)
    if not raw:
        return True
    parsed = urllib.parse.urlsplit(raw)
    host = parsed.netloc.lower().strip()
    path = f"{parsed.path} {parsed.query}".lower()
    attrs = attrs if isinstance(attrs, dict) else {}
    blob = " ".join(str(attrs.get(key, "") or "") for key in ("alt", "title", "class", "id", "itemprop", "data-testid")).lower()
    if not host or host.endswith("favicon.ico"):
        return True
    if host == "s.w.org" and "/emoji/" in path:
        return True
    if "grey-placeholder" in path or "gray-placeholder" in path:
        return True
    if raw.startswith("data:image/"):
        return True
    if "placeholder" in host or "blank" in host:
        return True
    reject_tokens = (
        "logo", "avatar", "icon", "favicon", "sprite", "placeholder", "tracking", "pixel", "spacer", "default", "blank",
        "button", "badge", "share", "social", "author", "profile", "user", "cover-small", "thumb-small",
        "/emoji/", "emoji/", "wp-smiley", "smiley", "loading", "lazyload-placeholder",
    )
    if any(token in path for token in reject_tokens):
        return True
    if any(token in blob for token in ("logo", "avatar", "icon", "favicon", "profile", "author", "user")):
        return True
    width = _coerce_int(attrs.get("width", 0))
    height = _coerce_int(attrs.get("height", 0))
    if width and height and min(width, height) < 90:
        return True
    if width and height and max(width, height) / max(min(width, height), 1) > 2.8 and not any(token in path for token in ("panorama", "wide", "landscape")):
        return True
    if width and height and width * height < 120 * 90 and not any(token in path for token in ("image", "img", "photo", "picture", "media", "content", "upload")):
        return True
    source_hint = reading_image_domain_hint(reading_source_thumbnail_host(source_url, article_url, source_name))
    if any(token in path for token in source_hint.get("reject", ())):
        return True
    return False


def reading_is_valid_author_avatar(url, attrs=None, article_url="", author_name="", hero_image="", entry_image="", source=""):
    image_url = absolutize_reading_url(url, article_url)
    if not image_url or not image_url.startswith(("http://", "https://")):
        return False
    image_key = normalize_reading_image_identity(image_url)
    if image_key and image_key in {
        normalize_reading_image_identity(hero_image),
        normalize_reading_image_identity(entry_image),
    }:
        return False
    parsed = urllib.parse.urlsplit(image_url)
    path = f"{parsed.netloc} {parsed.path} {parsed.query}".lower()
    attrs = attrs if isinstance(attrs, dict) else {}
    blob = " ".join(str(attrs.get(key, "") or "") for key in ("alt", "title", "class", "id", "itemprop", "data-testid")).lower()
    if reading_extract_youtube_media_id(image_url):
        return False
    if any(token in path for token in ("logo", "favicon", "sprite", "placeholder", "blank", "default", "tracking", "pixel")):
        return False
    if any(token in path for token in ("wp-content/uploads", "/uploads/", "/media/", "/images/")) and not any(token in path + " " + blob for token in ("author", "avatar", "profile", "byline", "person", "user")):
        return False
    width = _coerce_int(attrs.get("width", 0))
    height = _coerce_int(attrs.get("height", 0))
    if width and height:
        if min(width, height) < 32:
            return False
        ratio = max(width, height) / max(min(width, height), 1)
        if ratio > 2.4:
            return False
        if width * height > 900 * 900 and not any(token in path + " " + blob for token in ("avatar", "profile", "author")):
            return False
    strong_avatar_signal = any(token in path + " " + blob for token in ("author", "avatar", "profile", "byline", "person", "user"))
    explicit_author_meta = str(source or "").lower() == "meta"
    return bool(str(author_name or "").strip()) and (strong_avatar_signal or explicit_author_meta)


def reading_score_image_candidate(url, attrs=None, article_url="", source_url="", source_name="", kind=""):
    raw = unwrap_reading_image_proxy_url(url)
    if not raw:
        return -10_000
    if reading_is_bad_image_candidate(raw, attrs=attrs, article_url=article_url, source_url=source_url, source_name=source_name):
        return -10_000
    attrs = attrs if isinstance(attrs, dict) else {}
    parsed = urllib.parse.urlsplit(raw)
    host = parsed.netloc.lower().strip()
    path = f"{parsed.path} {parsed.query}".lower()
    blob = " ".join(str(attrs.get(key, "") or "") for key in ("alt", "title", "class", "id", "itemprop", "data-testid")).lower()
    score = 0
    if kind == "explicit":
        score += 320
    elif kind == "feed_cover":
        score += 300
    elif kind == "body":
        score += 180
    else:
        score += 100
    if any(token in path for token in ("wp-content/uploads", "/uploads/", "/images/", "/media/", "article", "story", "news", "post", "content")):
        score += 90
    if any(token in blob for token in ("featured", "hero", "lead", "cover", "main", "article", "story", "content")):
        score += 65
    if any(token in path for token in ("logo", "avatar", "icon", "favicon", "sprite", "placeholder")):
        score -= 150
    if any(token in path for token in ("thumb", "thumbnail", "preview")):
        score -= 20
    width = _coerce_int(attrs.get("width", 0))
    height = _coerce_int(attrs.get("height", 0))
    if width and height:
        area = width * height
        if area >= 800 * 450:
            score += 75
        elif area >= 400 * 225:
            score += 55
        elif area >= 240 * 160:
            score += 30
        else:
            score -= 60
    source_hint = reading_image_domain_hint(reading_source_thumbnail_host(source_url, article_url, source_name))
    if any(token in path for token in source_hint.get("prefer", ())):
        score += 35
    if host.endswith("aljazeera.net") or host.endswith("aljazeera.com"):
        if "resize=" in path:
            score += 25
    return score


def reading_pick_best_image_candidate(candidates, article_url="", source_url="", source_name=""):
    best = {"url": "", "kind": "", "score": -10_000, "attrs": {}}
    for candidate in candidates or []:
        if not isinstance(candidate, dict):
            continue
        url = absolutize_reading_url(candidate.get("url", "") or candidate.get("src", ""), article_url)
        score = reading_score_image_candidate(
            url,
            attrs=candidate.get("attrs", {}),
            article_url=article_url,
            source_url=source_url,
            source_name=source_name,
            kind=candidate.get("kind", ""),
        )
        if score > best["score"]:
            best = {
                "url": url,
                "kind": str(candidate.get("kind", "") or ""),
                "score": score,
                "attrs": candidate.get("attrs", {}) if isinstance(candidate.get("attrs", {}), dict) else {},
            }
    return best if best["score"] > -10_000 else {"url": "", "kind": "", "score": -10_000, "attrs": {}}


def extract_reading_image_from_html(value, base_url="", source_url="", source_name=""):
    parser = ReadingHTMLExtractor(base_url=base_url)
    try:
        parser.feed(str(value or ""))
        parser.close()
    except Exception:
        return ""
    candidates = []
    lead_image = extract_reading_lead_image_from_meta(parser.meta, base_url)
    if lead_image:
        candidates.append({"src": lead_image, "kind": "explicit", "attrs": {}})
    for candidate in parser.image_candidates:
        candidates.append({"src": candidate.get("src", ""), "kind": "body", "attrs": candidate})
    best = reading_pick_best_image_candidate(
        candidates,
        article_url=base_url,
        source_url=source_url,
        source_name=source_name,
    )
    return best.get("url", "")


def reading_extract_html_image_candidates(value, base_url=""):
    parser = ReadingHTMLExtractor(base_url=base_url)
    try:
        parser.feed(str(value or ""))
        parser.close()
    except Exception:
        return []
    return [
        {
            "url": absolutize_reading_url(candidate.get("src", ""), base_url),
            "kind": "body",
            "attrs": candidate,
        }
        for candidate in parser.image_candidates
        if candidate.get("src")
    ]


def reading_choose_article_hero_image(preferred_image="", lead_image="", content_html="", article_url="", source_url="", source_name=""):
    hero_candidates = []
    preferred_image = absolutize_reading_url(preferred_image, article_url)
    lead_image = absolutize_reading_url(lead_image, article_url)
    if preferred_image:
        hero_candidates.append({"url": preferred_image, "kind": "current_hero", "attrs": {}})
    if lead_image:
        hero_candidates.append({"url": lead_image, "kind": "explicit", "attrs": {}})
    body_candidates = reading_extract_html_image_candidates(content_html, article_url)
    if not hero_candidates:
        return ""
    current = reading_pick_best_image_candidate(hero_candidates, article_url=article_url, source_url=source_url, source_name=source_name)
    current_url = current.get("url", "")
    if not current_url:
        return ""
    current_key = normalize_reading_image_variant_key(current_url)
    current_quality = reading_image_variant_quality(
        current_url,
        attrs=current.get("attrs", {}),
        article_url=article_url,
        source_url=source_url,
        source_name=source_name,
        kind=current.get("kind", "current_hero"),
    )
    best_url = current_url
    best_quality = current_quality
    for candidate in body_candidates:
        candidate_url = candidate.get("url", "")
        if not candidate_url or normalize_reading_image_variant_key(candidate_url) != current_key:
            continue
        candidate_quality = reading_image_variant_quality(
            candidate_url,
            attrs=candidate.get("attrs", {}),
            article_url=article_url,
            source_url=source_url,
            source_name=source_name,
            kind="body",
        )
        if candidate_quality > best_quality + 25:
            best_url = candidate_url
            best_quality = candidate_quality
    return best_url if best_quality > -10_000 else ""


def extract_reading_lead_image_from_meta(meta, base_url=""):
    meta = meta if isinstance(meta, dict) else {}
    for key in (
        "og:image",
        "og:image:url",
        "og:image:secure_url",
        "twitter:image",
        "twitter:image:src",
        "parsely-image-url",
        "article:featured_image",
        "article:hero_image",
        "feature-image",
        "featured_image",
        "hero-image",
        "hero_image",
        "lead-image",
        "lead_image",
        "image",
        "image_url",
        "thumbnail",
        "thumbnail_url",
        "thumb",
    ):
        image = str(meta.get(key, "") or "").strip()
        if image:
            return absolutize_reading_url(image, base_url)
    return ""


def extract_reading_lead_image_from_html(value, base_url=""):
    parser = ReadingHTMLExtractor(base_url=base_url)
    try:
        parser.feed(str(value or ""))
        parser.close()
    except Exception:
        return ""
    return extract_reading_lead_image_from_meta(parser.meta, base_url)


def reading_is_explicit_lead_image_meta(meta):
    meta = meta if isinstance(meta, dict) else {}
    return any(
        str(meta.get(key, "") or "").strip()
        for key in (
            "og:image",
            "og:image:url",
            "og:image:secure_url",
            "twitter:image",
            "twitter:image:src",
            "parsely-image-url",
            "article:featured_image",
            "article:hero_image",
            "feature-image",
            "featured_image",
            "hero-image",
            "hero_image",
            "lead-image",
            "lead_image",
            "image",
            "image_url",
            "thumbnail",
            "thumbnail_url",
            "thumb",
        )
    )


def extract_reading_author_info_from_html(value, base_url=""):
    parser = ReadingHTMLExtractor(base_url=base_url)
    try:
        parser.feed(str(value or ""))
        parser.close()
    except Exception:
        return {"author": "", "author_image_url": ""}
    author_name = ""
    for key in (
        "author",
        "article:author",
        "twitter:creator",
        "parsely-author",
        "dc.creator",
        "dc:creator",
        "byline",
    ):
        author_name = normalize_reading_space(parser.meta.get(key, ""))
        if author_name:
            break
    if author_name and len(author_name) > 80:
        author_name = author_name[:80].strip()
    author_image_url = ""
    for key in (
        "author:image",
        "author_image",
        "article:author:image",
        "twitter:creator:image",
        "profile:image",
        "profile_image",
        "author:avatar",
        "author_avatar",
    ):
        candidate = absolutize_reading_url(parser.meta.get(key, ""), base_url)
        if reading_is_valid_author_avatar(candidate, article_url=base_url, author_name=author_name, source="meta"):
            author_image_url = candidate
            break
    if not author_image_url:
        for candidate in parser.author_image_candidates:
            candidate_url = absolutize_reading_url(candidate.get("src", ""), base_url)
            if reading_is_valid_author_avatar(candidate_url, attrs=candidate, article_url=base_url, author_name=author_name, source="dom"):
                author_image_url = candidate_url
                break
    return {
        "author": author_name,
        "author_image_url": author_image_url,
    }


def detect_reading_direction(*values):
    text = " ".join(str(value or "") for value in values)
    rtl_count = len(re.findall(r"[\u0590-\u08ff\ufb50-\ufdff\ufe70-\ufeff]", text))
    latin_count = len(re.findall(r"[A-Za-z]", text))
    if rtl_count >= 24 and rtl_count >= latin_count:
        return {"dir": "rtl", "lang": "ar"}
    return {"dir": "ltr", "lang": "en"}


def reading_short_text_direction(value):
    text = str(value or "").strip()
    if not text:
        return "auto"
    rtl_count = sum(
        1
        for ch in text
        if (
            0x0590 <= ord(ch) <= 0x08FF
            or 0xFB50 <= ord(ch) <= 0xFDFF
            or 0xFE70 <= ord(ch) <= 0xFEFF
        )
    )
    latin_count = sum(1 for ch in text if ("A" <= ch <= "Z") or ("a" <= ch <= "z"))
    if rtl_count and rtl_count >= latin_count:
        return "rtl"
    if latin_count:
        return "ltr"
    return "auto"


def reading_title_direction(title):
    direction = detect_reading_direction(title or "")
    return direction["dir"]


def reading_html_to_text(value):
    text = str(value or "")
    if not text:
        return ""
    text = re.sub(r"(?is)<(script|style|noscript|svg|form|button|input|select|textarea)[^>]*>.*?</\1>", " ", text)
    text = re.sub(r"(?is)</(p|div|section|article|main|h[1-6]|blockquote|li|figure|figcaption|ul|ol|tr|br)>", "\n\n", text)
    text = re.sub(r"(?is)<br\s*/?>", "\n", text)
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    text = unescape(text).replace("\xa0", " ")
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    return text.strip()


def reading_html_structure_score(value):
    html = str(value or "")
    if not html:
        return 0
    text = reading_html_to_text(html)
    if not text:
        return 0
    block_count = len(re.findall(r"(?is)</(p|div|section|article|main|h[1-6]|blockquote|li|figure|figcaption|ul|ol|tr)>", html))
    heading_count = len(re.findall(r"(?is)<h[1-6]\b", html))
    image_count = len(re.findall(r"(?is)<img\b", html))
    media_count = len(re.findall(r"(?is)<(iframe|video|embed|object)\b", html))
    media_count += sum(1 for marker in ("reading-youtube-embed", "reading-embed", "reading-media-placeholder") if marker in html)
    return min(len(text), 3000) + block_count * 110 + heading_count * 50 + image_count * 35 + media_count * 90


def reading_select_article_fragment(html):
    raw = str(html or "").strip()
    if not raw:
        return ""
    candidates = [(raw, False)]
    fragment_parser = ReadingArticleFragmentExtractor()
    try:
        fragment_parser.feed(raw)
        fragment_parser.close()
    except Exception:
        fragment_parser = None
    patterns = (
        r'(?is)<article\b[^>]*>.*?</article>',
        r'(?is)<main\b[^>]*>.*?</main>',
        r'(?is)<section\b[^>]*(?:class|id)=["\'][^"\']*(?:article|post|entry|story|body|reader|article-body|article-content|post-content|entry-content|story-content|main-content|wysiwyg)[^"\']*["\'][^>]*>.*?</section>',
        r'(?is)<div\b[^>]*(?:class|id)=["\'][^"\']*(?:article|post|entry|story|body|reader|article-body|article-content|post-content|entry-content|story-content|main-content|wysiwyg)[^"\']*["\'][^>]*>.*?</div>',
    )
    seen = {raw}
    if fragment_parser is not None:
        for fragment in fragment_parser.fragments:
            fragment = str(fragment or "").strip()
            if fragment and fragment not in seen:
                seen.add(fragment)
                candidates.append((fragment, True))
    for pattern in patterns:
        for match in re.finditer(pattern, raw):
            fragment = str(match.group(0) or "").strip()
            if fragment and fragment not in seen:
                seen.add(fragment)
                candidates.append((fragment, True))
    best = raw
    best_score = reading_html_structure_score(raw)
    best_text_len = len(reading_html_to_text(raw))
    for candidate, structured in candidates[1:]:
        candidate_text = reading_html_to_text(candidate)
        candidate_score = reading_html_structure_score(candidate) + (260 if structured else 0)
        candidate_len = len(candidate_text)
        if candidate_len < 200 and candidate_score < best_score + 150:
            continue
        if candidate_score > best_score + 80 or (structured and candidate_score > best_score - 40 and candidate_len >= 200) or (candidate_score > best_score and candidate_len > max(best_text_len * 1.15, best_text_len + 180)):
            best = candidate
            best_score = candidate_score
            best_text_len = candidate_len
    return best


def reading_select_source_article_fragment(html, article_url=""):
    raw = str(html or "").strip()
    if not raw:
        return ""
    host = urllib.parse.urlsplit(str(article_url or "")).netloc.lower()
    if host.endswith("alousboue.ma"):
        fragment_parser = ReadingArticleFragmentExtractor()
        try:
            fragment_parser.feed(raw)
            fragment_parser.close()
        except Exception:
            return ""
        candidates = []
        for fragment in fragment_parser.fragments:
            marker = fragment.lower()
            if "entry-content" not in marker:
                continue
            text_len = len(reading_html_to_text(fragment))
            score = reading_html_structure_score(fragment)
            if text_len >= 500:
                candidates.append((score, text_len, fragment))
        if candidates:
            candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
            return candidates[0][2]
    return ""


def reading_entry_content_score(entry):
    entry = entry if isinstance(entry, dict) else {}
    html_score = reading_html_structure_score(entry.get("content_html", ""))
    if html_score:
        return html_score
    text = reading_html_to_text(entry.get("content_text", "")) or normalize_reading_space(entry.get("excerpt", ""))
    score = len(text)
    if entry.get("image_url"):
        score += 35
    if entry.get("lead_image_url"):
        score += 35
    if entry.get("author_image_url"):
        score += 10
    if entry.get("author"):
        score += 10
    return score


def reading_entry_needs_content_upgrade(entry, force_refresh=False):
    entry = entry if isinstance(entry, dict) else {}
    if force_refresh:
        return True
    source_url = normalize_reading_url(entry.get("original_url") or entry.get("url"))
    if not source_url:
        return False
    status = str(entry.get("extraction_status", "") or "").strip().lower()
    score = reading_entry_content_score(entry)
    text = reading_html_to_text(entry.get("content_html", "")) or normalize_reading_space(entry.get("content_text", "")) or normalize_reading_space(entry.get("excerpt", ""))
    lowered = text.lower()
    teaser_markers = (
        "read more", "continue reading", "open original", "only an excerpt", "excerpt is available",
        "embedded content", "[embedded content]", "click to read more",
    )
    if any(marker in lowered for marker in teaser_markers):
        return True
    if reading_content_needs_media_enrichment(entry.get("content_html", "")):
        return True
    if status == "ok":
        return score < 1800 or len(text) < 900
    if status == "partial":
        return score < 1800 or len(text) < 1200
    if status == "feed":
        return score < 900 or len(text) < 700
    return score < 900 or len(text) < 700


def sanitize_reading_article_html(value, base_url="", hero_image="", author_image=""):
    html = str(value or "").strip()
    if not html:
        return ""
    parser = ReadingArticleHTMLSanitizer(base_url=base_url, hero_image=hero_image, author_image=author_image)
    try:
        parser.feed(html)
        parser.close()
        sanitized = parser.html()
    except Exception:
        return ""
    sanitized = reading_cleanup_article_markup(sanitized)
    text = normalize_reading_space(re.sub(r"<[^>]+>", " ", sanitized))
    has_media = any(marker in sanitized for marker in ("reading-youtube-embed", "reading-embed", "reading-media-placeholder"))
    return sanitized if text or has_media else ""


def reading_cleanup_article_markup(html):
    cleaned = str(html or "")
    if not cleaned:
        return ""
    junk_link_re = re.compile(
        r'(?is)<a\b[^>]*>\s*(?:أكمل القراءة|اقرأ المزيد|read more|continue reading|more|share|save|bookmark|follow|subscribe|print|comments?)\s*(?:[»›]|&raquo;)?\s*</a>'
    )
    junk_block_re = re.compile(
        r'(?is)<(p|div|span|li|blockquote)[^>]*>\s*(?:'
        r'(?:save|share|bookmark|follow|subscribe|print|comments?|more|read more|continue reading|أكمل القراءة|اقرأ المزيد)\s*(?:[|/·•]\s*)?)+'
        r'</\1>'
    )
    attribution_re = re.compile(r'(?is)<p[^>]*>\s*(?:المقالة|article)\b.*?</p>')
    attribution_re_2 = re.compile(
        r'(?is)<(p|div|span|li|blockquote)[^>]*>.*?(?:منشورة على|published on|posted on|article published on|shared on|follow us|sign up).*?</\1>'
    )
    junk_link_container_re = re.compile(
        r'(?is)<(p|div|section|article)[^>]*>.*?<a\b[^>]*(?:more-link|read-more|continue-reading|button)[^>]*>.*?</a>.*?</\1>'
    )
    image_href_re = re.compile(
        r'(?is)<a\b[^>]*\bhref=(["\'])(?:https?:)?//[^"\']+\.(?:jpe?g|png|webp|gif)(?:\?[^"\']*)?\1[^>]*>(.*?)</a>'
    )
    image_href_figure_re = re.compile(
        r'(?is)<figure\b[^>]*>\s*'
        r'<a\b[^>]*\bhref=(["\'])(?:https?:)?//[^"\']+\.(?:jpe?g|png|webp|gif)(?:\?[^"\']*)?\1[^>]*>(.*?)</a>'
        r'\s*</figure>'
    )
    cleaned = junk_link_re.sub("", cleaned)
    cleaned = junk_block_re.sub("", cleaned)
    cleaned = attribution_re.sub("", cleaned)
    cleaned = attribution_re_2.sub("", cleaned)
    cleaned = junk_link_container_re.sub("", cleaned)
    cleaned = image_href_figure_re.sub(r"\2", cleaned)
    cleaned = image_href_re.sub(r"\2", cleaned)
    cleaned = re.sub(r'(?is)(<p[^>]*>\s*)?(?:save|share|bookmark|follow|subscribe|print|comments?|more)\s*(?:[|/·•]\s*)?(</p>)?', "", cleaned)
    cleaned = re.sub(r'(?is)<figure\b[^>]*>\s*</figure>', "", cleaned)
    cleaned = re.sub(r'(?is)<(p|div|section|article|span|li|blockquote)[^>]*>\s*</\1>', "", cleaned)
    cleaned = re.sub(r'(?is)\s{3,}', " ", cleaned)
    cleaned = re.sub(r'(?is)(<p[^>]*>\s*)?(?:\s*&nbsp;\s*)+(</p>)?', "", cleaned)
    cleaned = reading_trim_junk_tail_blocks(cleaned)
    return reading_wrap_leading_text(cleaned.strip())


def reading_wrap_leading_text(html):
    cleaned = str(html or "").strip()
    if not cleaned:
        return ""
    if cleaned.startswith("<"):
        return cleaned
    first_tag = cleaned.find("<")
    if first_tag <= 0:
        return f'<p dir="auto">{html_escape(cleaned)}</p>'
    leading = cleaned[:first_tag].strip()
    if not leading:
        return cleaned[first_tag:].lstrip()
    return f'<p dir="auto">{html_escape(leading)}</p>{cleaned[first_tag:]}'


def reading_trim_junk_tail_blocks(html):
    cleaned = str(html or "").strip()
    if not cleaned:
        return ""
    block_re = re.compile(r'(?is)<(p|div|blockquote|li|figure|figcaption)[^>]*>.*?</\1>')
    junk_phrase_markers = (
        "save", "share", "bookmark", "follow", "subscribe", "print", "comment", "comments",
        "read more", "continue reading", "أكمل القراءة", "اقرأ المزيد", "منشورة على", "published on",
        "posted on", "article published on", "shared on", "follow us", "sign up", "المقالة",
    )
    while True:
        blocks = list(block_re.finditer(cleaned))
        if not blocks:
            return cleaned
        last = blocks[-1]
        block_html = last.group(0)
        block_text = normalize_reading_space(re.sub(r"<[^>]+>", " ", block_html)).lower()
        anchor_count = block_html.lower().count("<a")
        if not block_text:
            if re.search(r'(?is)<(?:img|iframe|video|audio|embed)\b', block_html):
                return cleaned
            cleaned = cleaned[:last.start()] + cleaned[last.end():]
            continue
        looks_like_junk = any(marker in block_text for marker in junk_phrase_markers)
        looks_like_footer = anchor_count >= 2 and len(block_text) < 320
        if looks_like_junk or looks_like_footer:
            cleaned = cleaned[:last.start()] + cleaned[last.end():]
            continue
        return cleaned


def reading_content_needs_media_enrichment(value):
    content = str(value or "").lower()
    if not content:
        return False
    stripped_markers = ("[embedded content]", "embedded content", "data-oembed", "wp-embedded-content")
    if any(marker in content for marker in stripped_markers):
        return True
    return False


def default_reading_data():
    return {
        "version": 1,
        "sources": [
            {
                "name": "The Marginalian",
                "url": "https://www.themarginalian.org/feed/",
                "topic": "Essays",
                "active": True,
            },
            {
                "name": "Quanta Magazine",
                "url": "https://www.quantamagazine.org/feed/",
                "topic": "Science",
                "active": True,
            },
            {
                "name": "Readwise Reader Import",
                "url": "",
                "topic": "Inbox",
                "active": False,
            },
        ],
        "entries": [
            {
                "source": "The Marginalian",
                "title": "How attention becomes a practice",
                "url": "https://example.com/attention-practice",
                "published_at": "2026-04-18T09:00:00+01:00",
                "added_at": "2026-04-22T08:30:00+01:00",
                "status": "unread",
                "starred": True,
                "topic": "Essays",
            },
            {
                "source": "Quanta Magazine",
                "title": "A quiet tour through new mathematical ideas",
                "url": "https://example.com/math-ideas",
                "published_at": "2026-04-15T14:20:00+01:00",
                "added_at": "2026-04-22T08:35:00+01:00",
                "status": "reading",
                "starred": False,
                "topic": "Science",
            },
            {
                "source": "Readwise Reader Import",
                "title": "Saved article waiting for RSS wiring",
                "url": "https://example.com/saved-article",
                "published_at": "",
                "added_at": "2026-04-22T08:40:00+01:00",
                "status": "unread",
                "starred": False,
                "topic": "Inbox",
            },
        ],
    }


def normalize_reading_status(value):
    normalized = str(value or "").strip().lower()
    return normalized if normalized in READING_STATUSES else "unread"


def normalize_reading_bool(value, default=True):
    if isinstance(value, bool):
        return value
    if value is None:
        return bool(default)
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on", "active", "enabled"}:
        return True
    if normalized in {"0", "false", "no", "off", "paused", "disabled", "inactive"}:
        return False
    return bool(default)


def normalize_reading_url(value):
    raw = str(value or "").strip()
    if not raw:
        return ""
    parsed = urllib.parse.urlsplit(raw)
    if not parsed.scheme or not parsed.netloc:
        return raw.rstrip("/")
    path = parsed.path.rstrip("/") or parsed.path or "/"
    return urllib.parse.urlunsplit((parsed.scheme.lower(), parsed.netloc.lower(), path, parsed.query, ""))


def normalize_reading_dedupe_url(value):
    normalized = normalize_reading_url(value)
    if not normalized:
        return ""
    parsed = urllib.parse.urlsplit(normalized)
    query_pairs = [
        (key, val)
        for key, val in urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
        if not key.lower().startswith(("utm_", "fbclid", "gclid"))
    ]
    return urllib.parse.urlunsplit((
        parsed.scheme,
        parsed.netloc[4:] if parsed.netloc.startswith("www.") else parsed.netloc,
        parsed.path.rstrip("/") or parsed.path or "/",
        urllib.parse.urlencode(query_pairs, doseq=True),
        "",
    ))


def reading_entry_dedupe_keys(entry):
    entry = entry if isinstance(entry, dict) else {}
    keys = set()
    source_id = str(entry.get("source_id", "") or "").strip()
    for field in ("url", "original_url", "canonical_url"):
        key_url = normalize_reading_dedupe_url(entry.get(field, ""))
        if key_url:
            keys.add(f"url:{key_url}")
    for field in ("external_id", "guid", "entry_guid", "feed_entry_id"):
        value = str(entry.get(field, "") or "").strip()
        if value:
            keys.add(f"id:{source_id}:{value.lower()}" if source_id else f"id:{value.lower()}")
    return keys


def reading_entry_sort_key(entry):
    entry = entry if isinstance(entry, dict) else {}
    published = parse_timestamp(entry.get("published_at", ""))
    added = parse_timestamp(entry.get("added_at", "") or entry.get("imported_at", ""))
    fallback = added or published
    return (
        published.timestamp() if published else (fallback.timestamp() if fallback else 0),
        added.timestamp() if added else 0,
        str(entry.get("id", "") or ""),
    )


def reading_has_real_sources(data):
    sources = data.get("sources", []) if isinstance(data, dict) else []
    demo_source_urls = {normalize_reading_url(url) for url in READING_DEMO_SOURCE_URLS}
    for source in sources:
        if not isinstance(source, dict):
            continue
        source_url = normalize_reading_url(source.get("url", ""))
        if source_url and source_url not in demo_source_urls:
            return True
    return False


def is_reading_demo_entry(entry):
    if not isinstance(entry, dict):
        return False
    demo_entry_urls = {normalize_reading_url(url) for url in READING_DEMO_ENTRY_URLS}
    return normalize_reading_url(entry.get("url", "")) in demo_entry_urls


def strip_reading_demo_entries(data):
    if not isinstance(data, dict) or not reading_has_real_sources(data):
        return False
    entries = data.get("entries", []) or []
    filtered = [entry for entry in entries if not is_reading_demo_entry(entry)]
    if len(filtered) == len(entries):
        return False
    data["entries"] = filtered
    return True


def reading_hash_key(value):
    return hashlib.sha1(str(value or "").encode("utf-8")).hexdigest()[:12]


def normalize_reading_source(source, index=0):
    item = source if isinstance(source, dict) else {}
    name = str(item.get("name", "") or "").strip() or f"Source {index + 1}"
    url = str(item.get("url", "") or "").strip()
    category = normalize_reading_category(item.get("category", "") or "")
    topic = normalize_reading_topic(item.get("topic", "") or "", category)
    topic_display = reading_visible_topic_label(topic, category)
    source_id = str(item.get("id", "") or item.get("source_id", "") or "").strip()
    if not source_id:
        source_id = f"reading-src-{reading_hash_key('|'.join([name.lower(), normalize_reading_url(url).lower(), topic.lower()]))}"
    added_at = str(item.get("added_at", "") or "").strip() or current_timestamp()
    updated_at = str(item.get("updated_at", "") or "").strip() or added_at
    last_sync_status = str(item.get("last_sync_status", "") or "").strip()
    last_sync_error = str(item.get("last_sync_error", "") or "").strip()
    last_sync_message = str(item.get("last_sync_message", "") or "").strip()
    last_sync_already_had_count = int(item.get("last_sync_already_had_count", 0) or item.get("last_sync_existing_count", 0) or 0)
    if not last_sync_already_had_count and last_sync_message:
        already_had_match = re.search(r"already had\s+(\d+)", last_sync_message, flags=re.IGNORECASE)
        if already_had_match:
            last_sync_already_had_count = int(already_had_match.group(1) or 0)
    last_sync_missing_key_count = int(item.get("last_sync_missing_key_count", 0) or 0)
    if not last_sync_missing_key_count and last_sync_message:
        missing_key_match = re.search(r"Skipped\s+(\d+)\s+item\(s\)\s+with no stable URL or id", last_sync_message, flags=re.IGNORECASE)
        if missing_key_match:
            last_sync_missing_key_count = int(missing_key_match.group(1) or 0)
    zero_import_streak = int(item.get("last_sync_zero_import_streak", 0) or 0)
    if last_sync_status.lower().startswith("error:"):
        if not last_sync_error:
            last_sync_error = last_sync_status.split(":", 1)[1].strip()
        last_sync_status = "error"
    return {
        "id": source_id,
        "name": name,
        "url": url,
        "topic": topic,
        "topic_display": topic_display,
        "category": category,
        "active": normalize_reading_bool(item.get("active", True), default=True),
        "added_at": added_at,
        "updated_at": updated_at,
        "last_synced_at": str(item.get("last_synced_at", "") or "").strip(),
        "last_sync_count": int(item.get("last_sync_count", 0) or 0),
        "last_sync_raw_count": int(item.get("last_sync_raw_count", 0) or 0),
        "last_sync_normalized_count": int(item.get("last_sync_normalized_count", 0) or 0),
        "last_sync_imported_count": int(item.get("last_sync_imported_count", 0) or 0),
        "last_sync_already_had_count": last_sync_already_had_count,
        "last_sync_missing_key_count": last_sync_missing_key_count,
        "last_sync_zero_import_streak": zero_import_streak,
        "last_sync_status_code": int(item.get("last_sync_status_code", 0) or 0),
        "last_sync_content_type": str(item.get("last_sync_content_type", "") or "").strip(),
        "last_sync_feed_kind": str(item.get("last_sync_feed_kind", "") or "").strip(),
        "last_sync_reason": str(item.get("last_sync_reason", "") or "").strip(),
        "last_sync_message": last_sync_message,
        "last_sync_status": last_sync_status,
        "last_sync_error": last_sync_error,
    }


def reading_source_sync_reason(source):
    source = source if isinstance(source, dict) else {}
    status = str(source.get("last_sync_status", "") or "").strip().lower()
    error = str(source.get("last_sync_error", "") or "").strip()
    feed_kind = str(source.get("last_sync_feed_kind", "") or "").strip().lower()
    content_type = str(source.get("last_sync_content_type", "") or "").strip().lower()
    raw_count = int(source.get("last_sync_raw_count", 0) or source.get("last_sync_count", 0) or 0)
    normalized_count = int(source.get("last_sync_normalized_count", 0) or 0)
    imported_count = int(source.get("last_sync_imported_count", 0) or 0)
    already_had_count = int(source.get("last_sync_already_had_count", 0) or 0)
    missing_key_count = int(source.get("last_sync_missing_key_count", 0) or 0)
    zero_import_streak = int(source.get("last_sync_zero_import_streak", 0) or 0)
    if not source.get("active", True):
        return "Source paused"
    if not str(source.get("url", "") or "").strip():
        return "Missing feed URL"
    if zero_import_streak >= 3 and status == "error":
        return "Repeated zero-import failures"
    if status == "error":
        return f"Fetch failed: {error}" if error else "Fetch failed"
    if not source.get("last_synced_at"):
        return "Not synced yet"
    if raw_count == 0:
        if "html" in content_type or feed_kind == "unknown":
            return "No RSS/Atom items found"
        return "Feed empty"
    if normalized_count == 0:
        return "Parser/normalization issue"
    if imported_count == 0 and already_had_count >= normalized_count:
        return "Already had all items"
    if missing_key_count:
        return f"Skipped {missing_key_count} item(s) with no stable URL or id"
    return "New items imported" if imported_count else "No new items"


def reading_source_health(source, known_entries=0):
    source = source if isinstance(source, dict) else {}
    status = str(source.get("last_sync_status", "") or "").strip().lower()
    feed_kind = str(source.get("last_sync_feed_kind", "") or "").strip().lower()
    content_type = str(source.get("last_sync_content_type", "") or "").strip().lower()
    raw_count = int(source.get("last_sync_raw_count", 0) or source.get("last_sync_count", 0) or 0)
    normalized_count = int(source.get("last_sync_normalized_count", 0) or 0)
    missing_key_count = int(source.get("last_sync_missing_key_count", 0) or 0)
    zero_import_streak = int(source.get("last_sync_zero_import_streak", 0) or 0)
    if not source.get("active", True):
        return "paused"
    if not str(source.get("url", "") or "").strip() or not source.get("last_synced_at"):
        return "warning"
    if status == "error":
        return "failing"
    if zero_import_streak >= 3:
        return "warning"
    if raw_count == 0:
        return "warning" if int(known_entries or 0) else "empty"
    if normalized_count == 0 or feed_kind == "unknown" or "html" in content_type:
        return "warning"
    if missing_key_count:
        return "warning"
    return "healthy"


def reading_source_health_label(source, known_entries=0):
    health = reading_source_health(source, known_entries=known_entries)
    return {
        "healthy": "OK",
        "warning": "Warning",
        "failing": "Error",
        "empty": "Empty",
        "paused": "Inactive",
    }.get(health, health.title() if isinstance(health, str) else "Warning")


def reading_is_rss_source(source):
    source = source if isinstance(source, dict) else {}
    url = str(source.get("url", "") or "").strip()
    name = str(source.get("name", "") or "").strip().lower()
    if not url:
        return False
    if "readwise" in name:
        return False
    return True


def normalize_reading_entry(entry, index=0, source_lookup=None, source_category_lookup=None):
    item = entry if isinstance(entry, dict) else {}
    published_at = str(item.get("published_at", "") or "").strip()
    added_at = str(item.get("added_at", "") or "").strip()
    source_name = str(item.get("source", "") or "Unknown Source").strip()
    source_id = str(item.get("source_id", "") or "").strip()
    source_lookup = source_lookup if isinstance(source_lookup, dict) else {}
    source_category_lookup = source_category_lookup if isinstance(source_category_lookup, dict) else {}
    if not source_id:
        source_id = source_lookup.get(source_name.lower(), "")
    url = normalize_reading_url(item.get("url", ""))
    title = str(item.get("title", "") or "").strip() or "Untitled article"
    original_url = normalize_reading_url(item.get("original_url", "")) or url
    entry_seed = url or "|".join([source_name.lower(), title.lower(), published_at.lower()])
    entry_id = str(item.get("id", "") or item.get("entry_id", "") or "").strip()
    if not entry_id:
        entry_id = f"reading-{reading_hash_key(entry_seed or str(index))}"
    topic = str(item.get("topic", "") or "").strip()
    excerpt = normalize_reading_space(item.get("excerpt", ""))
    content_text = str(item.get("content_text", "") or "").strip()
    content_html = str(item.get("content_html", "") or "").strip()
    content_score = int(item.get("content_score", 0) or 0) or reading_entry_content_score({
        "content_html": content_html,
        "content_text": content_text,
        "excerpt": excerpt,
        "image_url": item.get("image_url", ""),
        "lead_image_url": item.get("lead_image_url", ""),
        "author_image_url": item.get("author_image_url", ""),
        "author": item.get("author", ""),
    })
    image_url = absolutize_reading_url(item.get("image_url", ""), original_url or url)
    lead_image_url = absolutize_reading_url(item.get("lead_image_url", ""), original_url or url)
    image_source = str(item.get("image_source", "") or "").strip().lower()
    lead_image_kind = str(item.get("lead_image_kind", "") or "").strip().lower()
    author_name = str(item.get("author", "") or "").strip()
    author_image_url = absolutize_reading_url(item.get("author_image_url", ""), original_url or url)
    feed_source_url = str(item.get("feed_url", "") or "").strip()
    image_candidates = []
    if lead_image_url:
        image_candidates.append({
            "url": lead_image_url,
            "kind": "explicit" if lead_image_kind == "explicit" else "feed_cover",
            "attrs": {},
        })
    if image_url:
        image_candidates.append({
            "url": image_url,
            "kind": "feed_cover" if image_source == "feed_cover" else "body",
            "attrs": {},
        })
    body_image_url = ""
    if content_html:
        body_image_url = extract_reading_image_from_html(content_html, original_url or url, source_url=feed_source_url, source_name=source_name)
    if body_image_url:
        image_candidates.append({"url": body_image_url, "kind": "body", "attrs": {}})
    best_image = reading_pick_best_image_candidate(image_candidates, article_url=original_url or url, source_url=feed_source_url, source_name=source_name)
    if best_image.get("url"):
        image_url = best_image.get("url", "")
        if best_image.get("kind") in {"explicit", "feed_cover"}:
            lead_image_url = best_image.get("url", "")
            lead_image_kind = best_image.get("kind", "")
        else:
            lead_image_url = ""
            lead_image_kind = ""
    else:
        if reading_is_bad_image_candidate(image_url, article_url=original_url or url, source_url=feed_source_url, source_name=source_name):
            image_url = ""
        if reading_is_bad_image_candidate(lead_image_url, article_url=original_url or url, source_url=feed_source_url, source_name=source_name):
            lead_image_url = ""
            lead_image_kind = ""
    if author_image_url and not reading_is_valid_author_avatar(
        author_image_url,
        article_url=original_url or url,
        author_name=author_name,
        hero_image=lead_image_url,
        entry_image=image_url,
    ):
        author_image_url = ""
    source_category = source_category_lookup.get(source_id, "") or source_category_lookup.get(source_name.lower(), "")
    category = normalize_reading_category(source_category or item.get("category", ""))
    topic_display = reading_visible_topic_label(topic, category)
    extraction_status = str(item.get("extraction_status", "") or "").strip()
    if extraction_status and extraction_status not in READING_CONTENT_CACHE_STATUSES:
        extraction_status = ""
    return {
        "id": entry_id,
        "source_id": source_id,
        "source": source_name,
        "source_dir": reading_short_text_direction(source_name),
        "title": title,
        "title_dir": reading_title_direction(title),
        "url": url,
        "original_url": original_url,
        "external_id": str(item.get("external_id", "") or item.get("guid", "") or item.get("entry_guid", "") or item.get("feed_entry_id", "") or "").strip(),
        "published_at": published_at,
        "published_display": str(item.get("published_display", "") or "").strip() or format_timestamp_label(published_at, default=""),
        "added_at": added_at,
        "added_display": str(item.get("added_display", "") or "").strip() or format_timestamp_label(added_at, default=""),
        "imported_at": str(item.get("imported_at", "") or added_at).strip(),
        "status": normalize_reading_status(item.get("status", "")),
        "starred": bool(item.get("starred", False)),
        "topic": topic,
        "topic_display": topic_display,
        "category": category,
        "image_url": image_url,
        "lead_image_url": lead_image_url,
        "image_source": image_source,
        "lead_image_kind": lead_image_kind if lead_image_kind in {"explicit", "feed_cover"} else "",
        "author": author_name,
        "author_image_url": author_image_url,
        "excerpt": excerpt,
        "content_html": content_html,
        "content_text": content_text,
        "content_score": content_score,
        "extraction_status": extraction_status,
        "extraction_error": str(item.get("extraction_error", "") or "").strip(),
        "content_cached_at": str(item.get("content_cached_at", "") or "").strip(),
        "origin": str(item.get("origin", "") or "manual").strip() or "manual",
        "imported_from": str(item.get("imported_from", "") or "").strip(),
        "feed_url": str(item.get("feed_url", "") or "").strip(),
        "canonical_url": url,
    }


def normalize_reading_data(payload):
    data = payload if isinstance(payload, dict) else default_reading_data()
    data.setdefault("version", 1)
    data.setdefault("sources", [])
    data.setdefault("entries", [])
    changed = not isinstance(payload, dict)
    normalized_sources = []
    source_lookup = {}
    source_category_lookup = {}
    for index, source in enumerate(data.get("sources", []) or []):
        normalized_source = normalize_reading_source(source, index)
        if not isinstance(source, dict) or source != normalized_source:
            changed = True
        normalized_sources.append(normalized_source)
        source_lookup[normalized_source["name"].lower()] = normalized_source["id"]
        source_lookup[normalized_source["id"]] = normalized_source["id"]
        if normalized_source.get("url"):
            source_lookup[normalize_reading_url(normalized_source["url"]).lower()] = normalized_source["id"]
        source_category_lookup[normalized_source["id"]] = normalized_source.get("category", "news")
        source_category_lookup[normalized_source["name"].lower()] = normalized_source.get("category", "news")
    normalized_entries = []
    for index, entry in enumerate(data.get("entries", []) or []):
        normalized_entry = normalize_reading_entry(entry, index, source_lookup=source_lookup, source_category_lookup=source_category_lookup)
        if not isinstance(entry, dict) or entry != normalized_entry:
            changed = True
        normalized_entries.append(normalized_entry)
    data["sources"] = normalized_sources
    data["entries"] = normalized_entries
    if strip_reading_demo_entries(data):
        changed = True
    return data, changed


def load_reading_data():
    data = load_json_file(READING_DATA_PATH, None)
    read_failed = READING_DATA_PATH.exists() and data is None
    if read_failed:
        backup_payload = load_reading_backup_payload()
        if backup_payload is not None:
            data = backup_payload
    normalized, changed = normalize_reading_data(data)
    if changed and not read_failed:
        backup_reading_data_file("normalize")
        save_json_file(READING_DATA_PATH, normalized)
    return normalized


def save_reading_data(data):
    normalized, _ = normalize_reading_data(data)
    backup_reading_data_file("save")
    save_json_file(READING_DATA_PATH, normalized)
    return normalized


def reading_extract_text(node, names, default=""):
    for name in names:
        found = node.find(f".//{{*}}{name}")
        if found is None:
            continue
        text = unescape("".join(found.itertext()).strip())
        if text:
            return text
    return default


def reading_extract_link(node):
    links = list(node.findall(".//{*}link"))
    preferred_links = []
    fallback_links = []
    for link in links:
        href = str(link.attrib.get("href", "") or "").strip()
        text = unescape("".join(link.itertext()).strip())
        rel = str(link.attrib.get("rel", "") or "").strip().lower()
        if href and rel in {"alternate", "related", ""}:
            preferred_links.append(href)
            continue
        if href:
            fallback_links.append(href)
            continue
        if text:
            fallback_links.append(text)
    if preferred_links:
        return preferred_links[0]
    if fallback_links:
        return fallback_links[0]
    guid = node.find(".//{*}guid")
    if guid is not None:
        guid_text = unescape("".join(guid.itertext()).strip())
        if guid_text:
            return guid_text
    return ""


def reading_extract_categories(node):
    categories = []
    for category in node.findall(".//{*}category"):
        value = str(category.attrib.get("term", "") or category.attrib.get("label", "") or "").strip()
        if not value:
            value = unescape("".join(category.itertext()).strip())
        if value:
            categories.append(value)
    return categories


def reading_extract_entry_identifier(node):
    for name in ("guid", "id"):
        found = node.find(f".//{{*}}{name}")
        if found is None:
            continue
        text = unescape("".join(found.itertext()).strip())
        if text:
            return text
    return ""


def reading_node_local_name(node):
    return str(getattr(node, "tag", "") or "").split("}", 1)[-1].lower()


def reading_extract_feed_content(node):
    for child in list(node):
        local_name = reading_node_local_name(child)
        if local_name in {"encoded", "content"}:
            text = unescape("".join(child.itertext()).strip())
            if text:
                return text
    return reading_extract_text(node, ["description", "summary", "subtitle"], default="")


def reading_extract_feed_image_details(node, content_html="", article_url="", source_name="", source_url=""):
    candidates = []
    for child in node.iter():
        local_name = reading_node_local_name(child)
        attrs = getattr(child, "attrib", {}) or {}
        if local_name in {"thumbnail", "content", "image"}:
            candidate = attrs.get("url") or attrs.get("href") or attrs.get("src")
            media_type = str(attrs.get("type", "") or "").lower()
            medium = str(attrs.get("medium", "") or "").lower()
            if candidate and (local_name != "content" or "image" in media_type or medium == "image"):
                candidates.append({
                    "url": candidate,
                    "kind": "feed_cover",
                    "attrs": {
                        "type": media_type,
                        "medium": medium,
                        "width": attrs.get("width", ""),
                        "height": attrs.get("height", ""),
                        "title": attrs.get("title", ""),
                        "label": attrs.get("label", ""),
                    },
                })
        if local_name == "enclosure":
            candidate = attrs.get("url", "")
            media_type = str(attrs.get("type", "") or "").lower()
            if candidate and media_type.startswith("image/"):
                candidates.append({
                    "url": candidate,
                    "kind": "feed_cover",
                    "attrs": {
                        "type": media_type,
                        "width": attrs.get("width", ""),
                        "height": attrs.get("height", ""),
                    },
                })
    lead_image_url = extract_reading_lead_image_from_html(content_html, article_url)
    if lead_image_url:
        candidates.append({"url": lead_image_url, "kind": "explicit", "attrs": {}})
    body_image_url = extract_reading_image_from_html(content_html, article_url, source_url=source_url, source_name=source_name)
    if body_image_url:
        candidates.append({"url": body_image_url, "kind": "body", "attrs": {}})
    best = reading_pick_best_image_candidate(candidates, article_url=article_url, source_url=source_url, source_name=source_name)
    if not best.get("url"):
        return "", "", ""
    if best.get("kind") == "body":
        return best.get("url", ""), "", "body"
    return best.get("url", ""), best.get("url", ""), best.get("kind", "feed_cover") or "feed_cover"


def reading_extract_feed_image(node, content_html="", article_url="", source_name="", source_url=""):
    image_url, _, _ = reading_extract_feed_image_details(node, content_html=content_html, article_url=article_url, source_name=source_name, source_url=source_url)
    return image_url


def build_reading_import_item(source, feed_url, node, source_topic=""):
    title = reading_extract_text(node, ["title"], default="Untitled article")
    url = normalize_reading_url(reading_extract_link(node))
    external_id = reading_extract_entry_identifier(node)
    published_at = normalize_timestamp_value(reading_extract_text(node, ["pubDate", "published", "updated", "date"], default=""))
    imported_at = current_timestamp()
    topic = reading_extract_categories(node)
    content_html = reading_extract_feed_content(node)
    content_text = strip_reading_html(content_html)
    excerpt = content_text[:420].strip()
    image_url, lead_image_url, lead_image_kind = reading_extract_feed_image_details(
        node,
        content_html=content_html,
        article_url=url,
        source_name=source.get("name", ""),
        source_url=feed_url,
    )
    author_info = extract_reading_author_info_from_html(content_html, url)
    author_name = reading_extract_text(node, ["author", "creator", "dc:creator"], default="") or author_info.get("author", "")
    content_score = reading_entry_content_score({
        "content_html": content_html,
        "content_text": content_text,
        "excerpt": excerpt,
        "image_url": image_url,
        "lead_image_url": lead_image_url,
        "author_image_url": author_info.get("author_image_url", ""),
        "author": author_name,
    })
    return {
        "source": source.get("name", "Unknown Source"),
        "source_id": source.get("id", ""),
        "title": title,
        "url": url,
        "original_url": url,
        "external_id": external_id,
        "published_at": published_at,
        "added_at": imported_at,
        "imported_at": imported_at,
        "status": "unread",
        "starred": False,
        "author": author_name,
        "author_image_url": author_info.get("author_image_url", ""),
        "topic": topic[0] if topic else source_topic,
        "category": normalize_reading_category(source.get("category", "")),
        "topic_display": reading_visible_topic_label(topic[0] if topic else source_topic, source.get("category", "")),
        "image_url": image_url,
        "lead_image_url": lead_image_url,
        "lead_image_kind": lead_image_kind,
        "excerpt": excerpt,
        "content_text": content_text,
        "content_html": content_html,
        "content_score": content_score,
        "extraction_status": "feed" if content_text else "",
        "extraction_error": "",
        "content_cached_at": imported_at if content_text or image_url else "",
        "origin": "rss",
        "feed_url": feed_url,
    }


def fetch_reading_feed(source):
    source = normalize_reading_source(source)
    feed_url = str(source.get("url", "") or "").strip()
    if not feed_url:
        return {
            "ok": False,
            "feed_kind": "empty",
            "source_url": feed_url,
            "status_code": 0,
            "content_type": "",
            "raw_count": 0,
            "normalized_count": 0,
            "items": [],
            "error": "Missing feed URL.",
        }
    response = None
    content_type = ""
    try:
        response = requests.get(feed_url, timeout=20, headers={"User-Agent": "DragonReading/1.0 (+local)"})
        response.raise_for_status()
        content_type = str(response.headers.get("Content-Type", "") or "").strip()
        root = ET.fromstring(response.content)
    except Exception as exc:
        response_obj = getattr(exc, "response", None) or response
        headers = getattr(response_obj, "headers", {}) or {}
        return {
            "ok": False,
            "feed_kind": "error",
            "source_url": feed_url,
            "status_code": int(getattr(response_obj, "status_code", 0) or 0),
            "content_type": str(headers.get("Content-Type", "") or ""),
            "raw_count": 0,
            "normalized_count": 0,
            "items": [],
            "error": str(exc),
        }

    items = []
    source_topic = str(source.get("topic", "") or "").strip()
    rss_items = []
    atom_items = []
    feed_kind = "unknown"
    if root.tag.endswith("rss"):
        feed_kind = "rss"
        channel = root.find(".//{*}channel")
        if channel is not None:
            rss_items = channel.findall("./item")
        if not rss_items:
            rss_items = root.findall(".//{*}item")
    elif root.tag.endswith("RDF") or root.tag.endswith("rdf"):
        feed_kind = "rdf"
        rss_items = root.findall(".//{*}item")
    elif root.tag.endswith("feed"):
        feed_kind = "atom"
        atom_items = root.findall(".//{*}entry")

    for node in rss_items:
        items.append(build_reading_import_item(source, feed_url, node, source_topic=source_topic))

    for node in atom_items:
        items.append(build_reading_import_item(source, feed_url, node, source_topic=source_topic))

    return {
        "ok": True,
        "feed_kind": feed_kind,
        "source_url": feed_url,
        "status_code": getattr(response, "status_code", 200),
        "content_type": content_type,
        "raw_count": len(rss_items) + len(atom_items),
        "normalized_count": len(items),
        "items": items,
        "error": "",
    }


def sync_reading_sources(source_id=""):
    data = load_reading_data()
    source_id = str(source_id or "").strip()
    target_sources = []
    for source in data.get("sources", []):
        if not isinstance(source, dict):
            continue
        if source_id and source.get("id") != source_id:
            continue
        if not source.get("active", True):
            continue
        if not str(source.get("url", "") or "").strip():
            continue
        target_sources.append(source)

    entries = list(data.get("entries", []))
    existing_by_key = {}
    for index, entry in enumerate(entries):
        for dedupe_key in reading_entry_dedupe_keys(entry):
            existing_by_key[dedupe_key] = index

    imported_total = 0
    source_results = []
    zero_import_reasons = {}
    now = current_timestamp()
    for source in target_sources:
        try:
            fetch_result = fetch_reading_feed(source)
            imported_items = list(fetch_result.get("items", []) or [])
            raw_count = int(fetch_result.get("raw_count", 0) or 0)
            normalized_count = int(fetch_result.get("normalized_count", len(imported_items)) or len(imported_items))
            fetch_ok = bool(fetch_result.get("ok", False))
            fetch_error = str(fetch_result.get("error", "") or "").strip()
            fetch_kind = str(fetch_result.get("feed_kind", "") or "").strip()
            fetch_status_code = int(fetch_result.get("status_code", 0) or 0)
            fetch_content_type = str(fetch_result.get("content_type", "") or "").strip()
            source_imported = 0
            source_skipped_existing = 0
            source_skipped_missing_key = 0
            for imported in imported_items:
                dedupe_keys = reading_entry_dedupe_keys({
                    **imported,
                    "source_id": source.get("id", ""),
                })
                if not dedupe_keys:
                    source_skipped_missing_key += 1
                    continue
                existing_index = next((existing_by_key[key] for key in dedupe_keys if key in existing_by_key), None)
                import_seen_before = existing_index is not None
                import_added_at = current_timestamp()
                normalized_import = normalize_reading_entry({
                    **imported,
                    "source": source.get("name", "Unknown Source"),
                    "source_id": source.get("id", ""),
                    "added_at": import_added_at,
                    "imported_at": import_added_at,
                    "status": "unread",
                    "starred": False,
                    "origin": "rss",
                    "category": normalize_reading_category(imported.get("category", "") or source.get("category", "")),
                }, len(entries))
                if import_seen_before:
                    existing = dict(entries[existing_index])
                    existing_content_snapshot = {
                        key: existing.get(key, "")
                        for key in ("image_url", "lead_image_url", "lead_image_kind", "author", "author_image_url", "excerpt", "content_html", "content_text", "content_score", "extraction_status", "extraction_error", "content_cached_at")
                        if existing.get(key)
                    }
                    existing_score = reading_entry_content_score(existing)
                    import_score = reading_entry_content_score(normalized_import)
                    preserve_existing_content = bool(existing_content_snapshot) and existing_score >= import_score
                    preserved = {
                        "id": existing.get("id") or normalized_import["id"],
                        "added_at": existing.get("added_at") or normalized_import["added_at"],
                        "imported_at": existing.get("imported_at") or existing.get("added_at") or normalized_import["imported_at"],
                        "status": normalize_reading_status(existing.get("status")),
                        "starred": bool(existing.get("starred", False)),
                    }
                    existing.update(normalized_import)
                    existing.update(preserved)
                    if preserve_existing_content:
                        existing.update(existing_content_snapshot)
                    else:
                        cached_content = {
                            key: existing.get(key, "")
                            for key in ("image_url", "lead_image_url", "lead_image_kind", "author", "author_image_url", "excerpt", "content_html", "content_text", "content_score", "extraction_status", "extraction_error", "content_cached_at")
                            if existing.get(key) and not normalized_import.get(key)
                        }
                        existing.update(cached_content)
                    if not existing.get("topic"):
                        existing["topic"] = normalized_import.get("topic", "")
                    if not existing.get("category"):
                        existing["category"] = normalized_import.get("category", "")
                    if not existing.get("published_at"):
                        existing["published_at"] = normalized_import.get("published_at", "")
                    existing["published_display"] = format_timestamp_label(existing.get("published_at", ""), default="")
                    entries[existing_index] = existing
                    for dedupe_key in reading_entry_dedupe_keys(existing):
                        existing_by_key[dedupe_key] = existing_index
                    source_skipped_existing += 1
                else:
                    primary_key = sorted(dedupe_keys)[0]
                    normalized_import["id"] = normalized_import.get("id") or f"reading-{reading_hash_key(primary_key)}"
                    entries.append(normalized_import)
                    new_index = len(entries) - 1
                    for dedupe_key in reading_entry_dedupe_keys(normalized_import):
                        existing_by_key[dedupe_key] = new_index
                    imported_total += 1
                    source_imported += 1
            source["last_synced_at"] = now
            source["last_sync_count"] = raw_count
            source["last_sync_raw_count"] = raw_count
            source["last_sync_normalized_count"] = normalized_count
            source["last_sync_imported_count"] = source_imported
            source["last_sync_already_had_count"] = source_skipped_existing
            source["last_sync_missing_key_count"] = source_skipped_missing_key
            source["last_sync_zero_import_streak"] = int(source.get("last_sync_zero_import_streak", 0) or 0)
            source["last_sync_status_code"] = fetch_status_code
            source["last_sync_content_type"] = fetch_content_type
            source["last_sync_feed_kind"] = fetch_kind
            source["last_sync_status"] = "ok" if fetch_ok else "error"
            source["last_sync_error"] = fetch_error if not fetch_ok else ""
            source["last_sync_reason"] = reading_source_sync_reason(source)
            source["last_sync_message"] = (
                f"Fetched {raw_count} item(s), normalized {normalized_count}, imported {source_imported}, already had {source_skipped_existing}."
                if fetch_ok else
                f"Fetch failed ({fetch_kind or 'feed'}{f' {fetch_status_code}' if fetch_status_code else ''}): {fetch_error}"
            )
            if fetch_ok and source_skipped_missing_key:
                source["last_sync_message"] += f" Skipped {source_skipped_missing_key} item(s) with no stable URL or id."
            if fetch_ok and raw_count == 0:
                source["last_sync_message"] = "Fetched 0 items from feed."
            if source_imported == 0:
                source["last_sync_zero_import_streak"] = int(source.get("last_sync_zero_import_streak", 0) or 0) + 1
            else:
                source["last_sync_zero_import_streak"] = 0
            source["last_sync_reason"] = reading_source_sync_reason(source)
            if source_imported == 0:
                reason = str(source.get("last_sync_reason", "") or "").strip() or "No new items"
                zero_import_reasons[reason] = zero_import_reasons.get(reason, 0) + 1
            source["updated_at"] = now
            source_results.append({
                "name": source.get("name", "Unknown Source"),
                "count": raw_count,
                "normalized": normalized_count,
                "imported": source_imported,
                "already_existing": source_skipped_existing,
                "missing_key": source_skipped_missing_key,
                "status": "ok" if fetch_ok else "error",
                "reason": source.get("last_sync_reason", ""),
                "feed_kind": fetch_kind,
                "status_code": fetch_status_code,
                "content_type": fetch_content_type,
                "error": fetch_error,
            })
        except Exception as exc:
            source["last_synced_at"] = now
            source["last_sync_count"] = 0
            source["last_sync_raw_count"] = 0
            source["last_sync_normalized_count"] = 0
            source["last_sync_imported_count"] = 0
            source["last_sync_already_had_count"] = 0
            source["last_sync_missing_key_count"] = 0
            source["last_sync_zero_import_streak"] = int(source.get("last_sync_zero_import_streak", 0) or 0) + 1
            source["last_sync_status_code"] = 0
            source["last_sync_content_type"] = ""
            source["last_sync_feed_kind"] = "error"
            source["last_sync_status"] = "error"
            source["last_sync_error"] = str(exc)
            source["last_sync_message"] = f"Fetch failed: {exc}"
            source["last_sync_reason"] = reading_source_sync_reason(source)
            zero_import_reasons[source["last_sync_reason"]] = zero_import_reasons.get(source["last_sync_reason"], 0) + 1
            source["updated_at"] = now
            source_results.append({
                "name": source.get("name", "Unknown Source"),
                "count": 0,
                "normalized": 0,
                "imported": 0,
                "status": "error",
                "reason": source.get("last_sync_reason", ""),
                "error": str(exc),
            })

    entries.sort(key=reading_entry_sort_key, reverse=True)
    data["entries"] = entries
    data["last_sync_at"] = now if target_sources else data.get("last_sync_at", "")
    data["last_sync_count"] = imported_total
    data["last_sync_sources"] = len(target_sources)
    if not target_sources:
        data["last_sync_message"] = "No active sources were available for sync"
    elif imported_total:
        data["last_sync_message"] = f"Imported {imported_total} new items from {len(target_sources)} active source(s)"
    else:
        reason_text = ", ".join(
            f"{count} {reason.lower()}"
            for reason, count in sorted(zero_import_reasons.items(), key=lambda item: (-item[1], item[0].lower()))
        )
        data["last_sync_message"] = f"0 new items from {len(target_sources)} active source(s)"
        if reason_text:
            data["last_sync_message"] += f": {reason_text}"
    save_reading_data(data)
    return {
        "imported_total": imported_total,
        "source_results": source_results,
        "zero_import_reasons": zero_import_reasons,
        "source_count": len(data.get("sources", [])),
        "active_source_count": len(target_sources),
        "last_sync_at": data.get("last_sync_at", ""),
        "last_sync_message": data.get("last_sync_message", ""),
    }


def update_reading_source(source_id, updates):
    data = load_reading_data()
    source_id = str(source_id or "").strip()
    updated = False
    for index, source in enumerate(data.get("sources", [])):
        if not isinstance(source, dict) or source.get("id") != source_id:
            continue
        source.update(updates)
        source["updated_at"] = current_timestamp()
        data["sources"][index] = normalize_reading_source(source, index)
        updated = True
        break
    if updated:
        save_reading_data(data)
    return updated


def remove_reading_source(source_id):
    data = load_reading_data()
    source_id = str(source_id or "").strip()
    before = len(data.get("sources", []))
    data["sources"] = [source for source in data.get("sources", []) if not isinstance(source, dict) or source.get("id") != source_id]
    removed = len(data["sources"]) != before
    if removed:
        data["last_sync_message"] = f"Removed source {source_id}"
        save_reading_data(data)
    return removed


def upsert_reading_source_record(name, url="", topic="", category="", active=True, source_id="", next_index=0):
    data = load_reading_data()
    sources = list(data.get("sources", []))
    normalized_category = normalize_reading_category(category)
    payload = {
        "name": str(name or "").strip(),
        "url": normalize_reading_url(url),
        "topic": normalize_reading_topic(topic or "", normalized_category),
        "category": normalized_category,
        "active": bool(active),
        "updated_at": current_timestamp(),
    }
    match_index = None
    if source_id:
        source_id = str(source_id or "").strip()
        for index, source in enumerate(sources):
            if isinstance(source, dict) and source.get("id") == source_id:
                match_index = index
                break
    else:
        for index, source in enumerate(sources):
            if not isinstance(source, dict):
                continue
            existing_url = normalize_reading_url(source.get("url", ""))
            existing_name = str(source.get("name", "") or "").strip().lower()
            if payload["url"] and existing_url and existing_url == payload["url"]:
                match_index = index
                break
            if not payload["url"] and existing_name == payload["name"].lower():
                match_index = index
                break
    if match_index is not None:
        existing = dict(sources[match_index])
        existing.update(payload)
        sources[match_index] = normalize_reading_source(existing, match_index)
        message = f"Updated source {payload['name']}."
    else:
        sources.append(normalize_reading_source(payload, next_index or len(sources)))
        message = f"Added source {payload['name']}."
    data["sources"] = sources
    save_reading_data(data)
    return data, message


def toggle_reading_source_active(source_id):
    data = load_reading_data()
    source_id = str(source_id or "").strip()
    for index, source in enumerate(data.get("sources", [])):
        if not isinstance(source, dict) or source.get("id") != source_id:
            continue
        source = dict(source)
        source["active"] = not bool(source.get("active", True))
        source["updated_at"] = current_timestamp()
        data["sources"][index] = normalize_reading_source(source, index)
        save_reading_data(data)
        return data["sources"][index]
    return None


def update_reading_entry(entry_id, updates):
    data = load_reading_data()
    entry_id = str(entry_id or "").strip()
    updates = updates if isinstance(updates, dict) else {}
    updated_entry = None
    for index, entry in enumerate(data.get("entries", [])):
        if not isinstance(entry, dict):
            continue
        if entry.get("id") != entry_id:
            continue
        merged = dict(entry)
        merged.update(updates)
        if "status" in updates:
            merged["status"] = normalize_reading_status(merged.get("status", ""))
        if "starred" in updates:
            merged["starred"] = bool(merged.get("starred", False))
        normalized = normalize_reading_entry(merged, index)
        data["entries"][index] = normalized
        updated_entry = normalized
        break
    if updated_entry is not None:
        save_reading_data(data)
    return updated_entry


def get_reading_entry(entry_id):
    data = load_reading_data()
    for index, entry in enumerate(data.get("entries", [])):
        normalized = normalize_reading_entry(entry, index)
        if normalized.get("id") == str(entry_id or "").strip():
            return normalized
    return None


def extract_reading_article_page(url):
    article_url = normalize_reading_url(url)
    if not article_url:
        return {"status": "failed", "error": "Missing article URL."}
    try:
        response = requests.get(article_url, timeout=20, headers={"User-Agent": "DragonReading/1.0 (+local)"})
        response.raise_for_status()
        html = response.text or ""
        parser = ReadingHTMLExtractor()
        parser.feed(html)
        parser.close()
        selected_html = reading_select_source_article_fragment(html, article_url) or reading_select_article_fragment(html)
        lead_image_url = extract_reading_lead_image_from_meta(parser.meta, article_url)
        lead_image_kind = "explicit" if lead_image_url and reading_is_explicit_lead_image_meta(parser.meta) else ""
        image_candidates = []
        if lead_image_url:
            image_candidates.append({"url": lead_image_url, "kind": "explicit", "attrs": {}})
        for candidate in getattr(parser, "image_candidates", []) or []:
            image_candidates.append({"url": candidate.get("src", ""), "kind": "body", "attrs": candidate})
        if not image_candidates and parser.images:
            image_candidates.extend({"url": image, "kind": "body", "attrs": {}} for image in parser.images if image)
        best_image = reading_pick_best_image_candidate(image_candidates, article_url=article_url)
        image_url = best_image.get("url", "") or lead_image_url or (absolutize_reading_url(parser.images[0], article_url) if parser.images else "")
        author_info = extract_reading_author_info_from_html(html, article_url)
        sanitized_candidates = []
        for candidate_html in (selected_html, html):
            candidate_html = str(candidate_html or "").strip()
            if not candidate_html:
                continue
            sanitized_candidate = sanitize_reading_article_html(candidate_html, base_url=article_url, hero_image=lead_image_url, author_image=author_info.get("author_image_url", ""))
            if sanitized_candidate:
                sanitized_candidates.append(sanitized_candidate)
        if not sanitized_candidates:
            sanitized_candidates = [""]
        content_html = ""
        content_text = ""
        content_score = 0
        for candidate_html in sanitized_candidates:
            candidate_text = reading_html_to_text(candidate_html)
            candidate_score = reading_html_structure_score(candidate_html)
            if candidate_score > content_score or (candidate_score == content_score and len(candidate_text) > len(content_text)):
                content_html = candidate_html
                content_text = candidate_text
                content_score = candidate_score
        excerpt = content_text[:420].strip()
        if content_html and not content_text:
            content_text = reading_html_to_text(content_html)
        if content_score < 200 and not content_text:
            content_text = reading_html_to_text(selected_html or html)
            content_score = max(content_score, len(content_text))
        status = "ok" if content_score >= 900 or len(content_text) >= 900 else ("partial" if content_text or image_url else "failed")
        return {
            "status": status,
            "image_url": image_url,
            "lead_image_url": lead_image_url,
            "lead_image_kind": lead_image_kind,
            "author": author_info.get("author", ""),
            "author_image_url": author_info.get("author_image_url", ""),
            "excerpt": excerpt,
            "content_html": content_html,
            "content_text": content_text,
            "content_score": content_score,
            "content_cached_at": current_timestamp(),
            "error": "" if content_text or image_url else "No readable article body found.",
        }
    except Exception as exc:
        return {
            "status": "failed",
            "image_url": "",
            "lead_image_url": "",
            "lead_image_kind": "",
            "author": "",
            "author_image_url": "",
            "excerpt": "",
            "content_html": "",
            "content_text": "",
            "content_score": 0,
            "content_cached_at": current_timestamp(),
            "error": str(exc),
        }


def ensure_reading_entry_content(entry_id, force_refresh=False):
    entry = get_reading_entry(entry_id)
    if not entry:
        return None
    media_needs_enrichment = reading_content_needs_media_enrichment(entry.get("content_html", ""))
    needs_upgrade = reading_entry_needs_content_upgrade(entry, force_refresh=force_refresh)
    if force_refresh:
        needs_upgrade = True
    if entry.get("content_html") and not media_needs_enrichment and not needs_upgrade:
        return entry
    if media_needs_enrichment and entry.get("extraction_status") == "failed" and entry.get("content_cached_at") and not needs_upgrade:
        return entry
    if entry.get("content_text") and entry.get("image_url") and entry.get("extraction_status") == "ok" and not media_needs_enrichment and not needs_upgrade:
        return entry
    if entry.get("extraction_status") in {"ok", "failed"} and entry.get("content_cached_at") and not media_needs_enrichment and not needs_upgrade:
        return entry
    if entry.get("content_text") and entry.get("extraction_status") == "feed" and entry.get("content_html") and not media_needs_enrichment and not needs_upgrade:
        return entry

    extraction = extract_reading_article_page(entry.get("original_url") or entry.get("url"))
    current_score = reading_entry_content_score(entry)
    extraction_score = int(extraction.get("content_score", 0) or 0)
    updates = {
        "extraction_status": extraction.get("status", ""),
        "extraction_error": extraction.get("error", ""),
        "content_cached_at": extraction.get("content_cached_at", current_timestamp()),
    }
    for key in ("image_url", "lead_image_url", "lead_image_kind", "author", "author_image_url", "excerpt", "content_html", "content_text"):
        if key == "content_html" and extraction.get(key) and (force_refresh or (not entry.get(key)) or media_needs_enrichment or extraction_score >= current_score):
            updates[key] = extraction.get(key)
        elif extraction.get(key) and (force_refresh or not entry.get(key) or extraction_score >= current_score):
            updates[key] = extraction.get(key)
    if extraction_score:
        updates["content_score"] = extraction_score
    updated = update_reading_entry(entry_id, updates)
    return updated or entry


def reading_filter_query_params(filters=None):
    filters = filters if isinstance(filters, dict) else {}
    query = {}
    source = str(filters.get("source", "All Sources") or "All Sources").strip()
    status = str(filters.get("status", "All Status") or "All Status").strip()
    category = str(filters.get("category", "All Categories") or "All Categories").strip()
    search = str(filters.get("search", "") or "").strip()
    if source and source != "All Sources":
        query["source"] = source
    if status and status != "All Status":
        query["status"] = status
    if category and category != "All Categories":
        query["category"] = category
    if search:
        query["search"] = search
    return query


def reading_entry_body_text(entry):
    entry = entry if isinstance(entry, dict) else {}
    for key in ("content_text", "excerpt", "content_html"):
        value = str(entry.get(key, "") or "").strip()
        if not value:
            continue
        if key == "content_html":
            value = strip_reading_html(value)
        else:
            value = normalize_reading_space(value)
        if value:
            return value
    return ""


def reading_tts_detect_language(text):
    sample = str(text or "")
    arabic_count = len(re.findall(r"[\u0590-\u08ff\ufb50-\ufdff\ufe70-\ufeff]", sample))
    latin_count = len(re.findall(r"[A-Za-z]", sample))
    if arabic_count and arabic_count >= latin_count:
        return "ar"
    return "en"


def reading_tts_default_voice(lang):
    normalized = str(lang or "").strip().lower()
    return READING_TTS_DEFAULT_VOICES.get(normalized, READING_TTS_DEFAULT_VOICES["en"])


def reading_tts_sentence_terminated(text):
    return bool(re.search(r"[.!?؟۔。！？]\s*$", str(text or "").strip()))


def reading_tts_title_for_speech(title):
    title = normalize_reading_space(title)
    if not title:
        return ""
    if reading_tts_sentence_terminated(title):
        return title
    return f"{title}."


def reading_tts_clean_text(text, title=""):
    raw = unescape(str(text or ""))
    raw = raw.replace("\r\n", "\n").replace("\r", "\n").strip()
    if not raw:
        return ""

    title_text = normalize_reading_space(unescape(str(title or "")))
    title_lower = title_text.lower()
    noise_patterns = (
        r"^(read more|continue reading|open original|original source|subscribe|sign up|share|share this|related articles|you may also like|also read|comments?|leave a comment|follow us|all rights reserved|copyright|download the app|watch video|watch now|embedded content|sponsored content|advertisement|ad)\b",
        r"^(المزيد|اقرأ المزيد|تابع القراءة|تابع القراءة على الموقع|المصدر|شارك|اشترك|سجل|تعليقات?|محتوى مدمج|إعلان)\b",
        r"^(source|via|photo|images?|tags?|tag):",
    )

    blocks = []
    seen_blocks = set()
    for chunk in re.split(r"\n\s*\n", raw):
        normalized_chunk = normalize_reading_space(re.sub(r"[ \t]+", " ", chunk))
        if not normalized_chunk:
            continue
        lowered = normalized_chunk.lower()
        if title_lower and lowered == title_lower:
            continue
        if title_lower and lowered.startswith(title_lower + " "):
            normalized_chunk = normalize_reading_space(normalized_chunk[len(title_text):].lstrip(" :-–—"))
            lowered = normalized_chunk.lower()
        if any(re.match(pattern, lowered, flags=re.IGNORECASE) for pattern in noise_patterns):
            continue
        if lowered in seen_blocks:
            continue
        seen_blocks.add(lowered)
        blocks.append(normalized_chunk)

    text = "\n\n".join(blocks)
    if title_text:
        speech_title = reading_tts_title_for_speech(title_text)
        if not text:
            text = speech_title
        elif not text.lower().startswith(title_lower):
            text = f"{speech_title}\n\n{text}"

    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    if len(text) > READING_TTS_MAX_CHARS:
        clipped = text[:READING_TTS_MAX_CHARS].rstrip()
        cutoff = max(clipped.rfind(". "), clipped.rfind("! "), clipped.rfind("? "), clipped.rfind("۔"), clipped.rfind("؟"))
        if cutoff > int(READING_TTS_MAX_CHARS * 0.6):
            clipped = clipped[:cutoff + 1].rstrip()
        text = clipped
    return normalize_reading_space(text)


class ReadingTTSBlockExtractor(HTMLParser):
    BLOCK_TAGS = {"h1", "h2", "h3", "h4", "h5", "h6", "p", "li", "blockquote", "figcaption"}
    SKIP_TAGS = {"script", "style"}

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.blocks = []
        self.current = None
        self.skip_depth = 0

    def handle_starttag(self, tag, attrs):
        tag = str(tag or "").lower()
        if tag in self.SKIP_TAGS:
            self.skip_depth += 1
            return
        if self.skip_depth:
            return
        if tag == "br" and self.current is not None:
            self.current["parts"].append("\n")
            return
        if tag in self.BLOCK_TAGS:
            self._flush_current()
            self.current = {"tag": tag, "parts": []}

    def handle_endtag(self, tag):
        tag = str(tag or "").lower()
        if tag in self.SKIP_TAGS and self.skip_depth:
            self.skip_depth -= 1
            return
        if self.skip_depth:
            return
        if self.current is not None and tag == self.current["tag"]:
            self._flush_current()

    def handle_data(self, data):
        if self.skip_depth or self.current is None:
            return
        self.current["parts"].append(str(data or ""))

    def _flush_current(self):
        if self.current is None:
            return
        text = normalize_reading_space("".join(self.current.get("parts", [])))
        if text:
            self.blocks.append({"tag": self.current["tag"], "text": text})
        self.current = None

    def close(self):
        super().close()
        self._flush_current()


def reading_tts_block_type(tag):
    normalized = str(tag or "").strip().lower()
    if normalized == "title":
        return "title"
    if normalized in {"h1", "h2"}:
        return "heading"
    if normalized in {"h3", "h4", "h5", "h6"}:
        return "subheading"
    if normalized == "li":
        return "list_item"
    if normalized == "blockquote":
        return "blockquote"
    if normalized == "figcaption":
        return "paragraph"
    return "paragraph"


def reading_tts_sentence_text_for_block(text, block_type):
    normalized = normalize_reading_space(text)
    if not normalized:
        return ""
    if block_type in {"title", "heading", "subheading"}:
        return reading_tts_title_for_speech(normalized)
    return normalized


def reading_tts_sanitized_article_html(entry):
    entry = entry if isinstance(entry, dict) else {}
    source_url = entry.get("original_url") or entry.get("url")
    preferred_image = normalize_reading_url(entry.get("image_url", ""))
    lead_image_url = normalize_reading_url(entry.get("lead_image_url", "")) if entry.get("lead_image_kind") in {"explicit", "feed_cover"} else ""
    article_hero_image = reading_choose_article_hero_image(
        preferred_image=preferred_image,
        lead_image=lead_image_url,
        content_html=entry.get("content_html", ""),
        article_url=source_url,
        source_url=entry.get("feed_url", ""),
        source_name=entry.get("source", ""),
    )
    article_author_image_url = entry.get("author_image_url", "")
    if article_author_image_url and not reading_is_valid_author_avatar(
        article_author_image_url,
        article_url=source_url,
        author_name=entry.get("author", ""),
        hero_image=article_hero_image,
        entry_image=entry.get("image_url", ""),
        source="html",
    ):
        article_author_image_url = ""
    return sanitize_reading_article_html(
        entry.get("content_html", ""),
        base_url=source_url,
        hero_image=article_hero_image,
        author_image=article_author_image_url,
    )


def build_reading_tts_structure(entry):
    entry = entry if isinstance(entry, dict) else {}
    title = normalize_reading_space(entry.get("title", ""))
    article_html = reading_tts_sanitized_article_html(entry)
    body_text = reading_entry_body_text(entry)
    raw_blocks = []
    if title:
        raw_blocks.append({"type": "title", "text": title})
    if article_html:
        parser = ReadingTTSBlockExtractor()
        try:
            parser.feed(article_html)
            parser.close()
        except Exception:
            parser = None
        for block in (parser.blocks if parser is not None else []):
            raw_blocks.append({
                "type": reading_tts_block_type(block.get("tag", "")),
                "text": block.get("text", ""),
            })
    elif body_text:
        for paragraph in [part.strip() for part in body_text.split("\n\n") if part.strip()]:
            raw_blocks.append({"type": "paragraph", "text": paragraph})

    blocks = []
    sentence_units = []
    seen_body_blocks = set()
    title_key = re.sub(r"\s+", " ", title.lower()).strip() if title else ""
    for raw_block in raw_blocks:
        block_type = str(raw_block.get("type", "") or "paragraph").strip().lower() or "paragraph"
        block_text = reading_tts_clean_text(raw_block.get("text", ""))
        if not block_text:
            continue
        block_key = re.sub(r"\s+", " ", block_text.lower()).strip()
        if block_type != "title":
            if title_key and block_key == title_key:
                continue
            if block_key in seen_body_blocks:
                continue
            seen_body_blocks.add(block_key)
        speech_text = reading_tts_sentence_text_for_block(block_text, block_type)
        sentences = [speech_text] if block_type in {"title", "heading", "subheading"} else split_reading_tts_sentences(speech_text)
        if not sentences:
            continue
        block_index = len(blocks)
        sentence_indexes = []
        for sentence in sentences:
            normalized_sentence = normalize_reading_space(sentence)
            if not normalized_sentence:
                continue
            sentence_index = len(sentence_units)
            sentence_units.append({
                "index": sentence_index,
                "text": normalized_sentence,
                "block_type": block_type,
                "block_index": block_index,
                "block_order": block_index,
            })
            sentence_indexes.append(sentence_index)
        if not sentence_indexes:
            continue
        blocks.append({
            "index": block_index,
            "order": block_index,
            "type": block_type,
            "text": speech_text,
            "sentence_indexes": sentence_indexes,
        })
    speech_text = "\n\n".join(item.get("text", "") for item in sentence_units if item.get("text"))
    return {
        "text": normalize_reading_space(speech_text),
        "blocks": blocks,
        "sentences": sentence_units,
        "article_html": article_html,
    }


def build_reading_tts_payload(entry):
    entry = entry if isinstance(entry, dict) else {}
    structure = build_reading_tts_structure(entry)
    cleaned = normalize_reading_space(structure.get("text", ""))
    title = str(entry.get("title", "") or "").strip()
    lang = reading_tts_detect_language(f"{title}\n\n{cleaned}")
    voice = reading_tts_default_voice(lang)
    text_hash = hashlib.sha256(cleaned.encode("utf-8")).hexdigest()
    safe_entry_id = re.sub(r"[^A-Za-z0-9._-]+", "_", str(entry.get("id", "") or "reading"))
    safe_voice = re.sub(r"[^A-Za-z0-9._-]+", "_", voice)
    cache_dir = READING_TTS_CACHE_DIR / safe_entry_id[:2]
    cache_path = cache_dir / f"{safe_entry_id}-{lang}-{safe_voice}-{text_hash[:24]}.mp3"
    metadata_path = cache_dir / f"{safe_entry_id}-{lang}-{safe_voice}-{text_hash[:24]}.metadata.jsonl"
    timings_path = cache_dir / f"{safe_entry_id}-{lang}-{safe_voice}-{text_hash[:24]}.timings.json"
    return {
        "text": cleaned,
        "lang": lang,
        "voice": voice,
        "text_hash": text_hash,
        "blocks": structure.get("blocks", []),
        "sentence_units": structure.get("sentences", []),
        "cache_path": cache_path,
        "metadata_path": metadata_path,
        "timings_path": timings_path,
        "cache_dir": cache_dir,
        "available": len(cleaned) >= READING_TTS_MIN_CHARS,
        "status_message": "Ready to listen." if len(cleaned) >= READING_TTS_MIN_CHARS else "Not enough readable text for audio yet.",
    }


def split_reading_tts_sentences(text):
    text = normalize_reading_space(text)
    if not text:
        return []
    parts = re.split(r"(?<=[.!?؟۔])\s+|(?<=[。！？])\s*", text)
    sentences = [normalize_reading_space(part) for part in parts if normalize_reading_space(part)]
    return sentences or [text]


def _edge_tts_time_to_seconds(value):
    try:
        number = float(value or 0)
    except Exception:
        return 0.0
    if number > 100000:
        return number / 10000000.0
    if number > 1000:
        return number / 1000.0
    return number


def build_reading_tts_fallback_timings(text, sentence_units=None):
    units = [item for item in (sentence_units or []) if isinstance(item, dict) and normalize_reading_space(item.get("text", ""))]
    sentences = units if units else [{"index": index, "text": sentence} for index, sentence in enumerate(split_reading_tts_sentences(text))]
    timings = []
    cursor = 0.0
    for index, sentence in enumerate(sentences):
        sentence_text = normalize_reading_space(sentence.get("text", "") if isinstance(sentence, dict) else sentence)
        if not sentence_text:
            continue
        word_count = max(len(sentence_text.split()), 1)
        duration = max(1.6, min(14.0, word_count * 0.42))
        timings.append({
            "index": int(sentence.get("index", index) if isinstance(sentence, dict) else index),
            "text": sentence_text,
            "start": round(cursor, 3),
            "end": round(cursor + duration, 3),
        })
        cursor += duration
    return timings


def normalize_reading_tts_timing_rows(rows):
    rows = list(rows or [])
    if not rows:
        return [], 0.0
    first_start = float(rows[0].get("start", 0.0) or 0.0)
    applied_offset = 0.0
    if 0.04 <= first_start <= READING_TTS_MAX_AUDIO_START_OFFSET_SECONDS:
        applied_offset = first_start

    normalized_rows = []
    for row in rows:
        start = max(float(row.get("start", 0.0) or 0.0) - applied_offset, 0.0)
        end = max(float(row.get("end", 0.0) or 0.0) - applied_offset, start)
        normalized_rows.append({
            "text": normalize_reading_space(row.get("text", "")),
            "start": start,
            "end": end,
        })
    return normalized_rows, round(applied_offset, 3)


def build_reading_tts_timings_from_metadata(metadata_path, text, sentence_units=None):
    metadata_path = Path(metadata_path)
    rows = []
    if metadata_path.exists():
        try:
            for line in metadata_path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                item = json.loads(line)
                if not isinstance(item, dict) or item.get("type") != "SentenceBoundary":
                    continue
                start = _edge_tts_time_to_seconds(item.get("offset"))
                duration = _edge_tts_time_to_seconds(item.get("duration"))
                sentence_text = normalize_reading_space(item.get("text", ""))
                rows.append({
                    "text": sentence_text,
                    "start": max(start, 0.0),
                    "end": max(start + duration, start),
                })
        except Exception:
            rows = []

    if not rows:
        rows = build_reading_tts_fallback_timings(text, sentence_units=sentence_units)

    rows, audio_start_offset = normalize_reading_tts_timing_rows(rows)
    timings = []
    sentence_units = [item for item in (sentence_units or []) if isinstance(item, dict)]
    for index, row in enumerate(rows):
        start = float(row.get("start", 0.0) or 0.0)
        end = float(row.get("end", 0.0) or 0.0)
        next_start = float(rows[index + 1].get("start", 0.0) or 0.0) if index + 1 < len(rows) else 0.0
        if next_start > start:
            end = min(end, next_start)
        if end <= start:
            end = max(next_start if next_start > start else 0.0, start + 1.0)
        sentence_unit = sentence_units[index] if index < len(sentence_units) else {}
        timings.append({
            "index": int(sentence_unit.get("index", row.get("index", index)) if isinstance(sentence_unit, dict) else row.get("index", index)),
            "text": normalize_reading_space((sentence_unit.get("text", "") if isinstance(sentence_unit, dict) else "") or row.get("text", "")),
            "start": round(start, 3),
            "end": round(end, 3),
            "block_type": str(sentence_unit.get("block_type", "paragraph") if isinstance(sentence_unit, dict) else "paragraph"),
            "block_index": int(sentence_unit.get("block_index", index) if isinstance(sentence_unit, dict) else index),
            "block_order": int(sentence_unit.get("block_order", index) if isinstance(sentence_unit, dict) else index),
        })
    return timings, audio_start_offset


def save_reading_tts_timings(timings_path, metadata_path, text, payload):
    timings_path = Path(timings_path)
    timings_path.parent.mkdir(parents=True, exist_ok=True)
    sentence_units = payload.get("sentence_units", []) if isinstance(payload, dict) else []
    block_units = payload.get("blocks", []) if isinstance(payload, dict) else []
    timings, audio_start_offset = build_reading_tts_timings_from_metadata(metadata_path, text, sentence_units=sentence_units)
    data = {
        "version": payload.get("text_hash", ""),
        "timings_version": READING_TTS_TIMINGS_VERSION,
        "lang": payload.get("lang", ""),
        "voice": payload.get("voice", ""),
        "source": "edge_sentence_boundary" if Path(metadata_path).exists() else "estimated",
        "sync_lead_seconds": READING_TTS_SYNC_LEAD_SECONDS,
        "audio_start_offset_seconds": audio_start_offset,
        "sentence_count": len(timings),
        "block_count": len(block_units),
        "blocks": block_units,
        "sentences": [
            {
                "index": item.get("index", index),
                "text": item.get("text", ""),
                "block_type": item.get("block_type", "paragraph"),
                "block_index": item.get("block_index", index),
                "block_order": item.get("block_order", index),
            }
            for index, item in enumerate(timings)
        ],
        "timings": timings,
    }
    timings_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return data


async def _save_reading_tts_audio(text, voice, output_path, metadata_path):
    communicator = edge_tts.Communicate(text, voice, boundary="SentenceBoundary")
    await communicator.save(str(output_path), str(metadata_path))


def generate_reading_tts_audio(text, voice, output_path, metadata_path=None):
    if edge_tts is None:
        raise RuntimeError("Audio generation backend is not available.")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_target = Path(metadata_path) if metadata_path else output_path.with_suffix(".metadata.jsonl")
    asyncio.run(_save_reading_tts_audio(text, voice, output_path, metadata_target))


def ensure_reading_tts_cache(tts_payload):
    cache_path = Path(tts_payload["cache_path"])
    metadata_path = Path(tts_payload["metadata_path"])
    timings_path = Path(tts_payload["timings_path"])
    needs_audio = not cache_path.exists()
    needs_timings = not timings_path.exists()
    if not needs_audio and not needs_timings:
        return
    if edge_tts is None:
        raise RuntimeError("Audio generation backend is not available.")
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    generate_reading_tts_audio(tts_payload["text"], tts_payload["voice"], cache_path, metadata_path)
    save_reading_tts_timings(timings_path, metadata_path, tts_payload["text"], tts_payload)


def reading_tts_audio_url(entry_id, version=""):
    params = {"entry_id": entry_id}
    version_value = str(version or "").strip()
    if version_value:
        params["v"] = version_value
    return url_for("reading_article_audio", **params)


def reading_tts_timings_url(entry_id, version=""):
    params = {"entry_id": entry_id}
    version_value = str(version or "").strip()
    if version_value:
        params["v"] = version_value
    return url_for("reading_article_audio_timings", **params)


def build_reading_view():
    data = load_reading_data()
    sources = [normalize_reading_source(source, index) for index, source in enumerate(data.get("sources", []))]
    source_lookup = {source["name"].lower(): source["id"] for source in sources if source.get("name")}
    source_lookup.update({
        normalize_reading_url(source.get("url", "")).lower(): source["id"]
        for source in sources
        if source.get("url")
    })
    source_lookup.update({source["id"]: source["id"] for source in sources})
    source_category_lookup = {source["id"]: source.get("category", "news") for source in sources}
    source_category_lookup.update({source["name"].lower(): source.get("category", "news") for source in sources if source.get("name")})
    entries = [
        normalize_reading_entry(entry, index, source_lookup=source_lookup, source_category_lookup=source_category_lookup)
        for index, entry in enumerate(data.get("entries", []))
    ]
    last_sync_timestamp = parse_timestamp(str(data.get("last_sync_at", "") or "").strip())
    for entry in entries:
        entry_import_timestamp = parse_timestamp(entry.get("imported_at", "")) or parse_timestamp(entry.get("added_at", "")) or parse_timestamp(entry.get("published_at", ""))
        entry["is_fresh_import"] = bool(
            last_sync_timestamp
            and entry_import_timestamp
            and entry_import_timestamp.timestamp() >= last_sync_timestamp.timestamp()
        )
    source_entry_count = {}
    for entry in entries:
        key = entry.get("source_id") or entry.get("source", "")
        source_entry_count[key] = source_entry_count.get(key, 0) + 1
    extra_sources = []
    seen_filter_ids = {source["id"] for source in sources}
    for entry in entries:
        source_id = entry.get("source_id") or ""
        source_name = entry.get("source", "") or "Unknown Source"
        if source_id and source_id not in seen_filter_ids:
            extra_sources.append({
                "id": source_id,
                "name": source_name,
                "url": "",
                "topic": entry.get("topic", ""),
                "topic_display": reading_visible_topic_label(entry.get("topic", ""), entry.get("category", "news")),
                "category": entry.get("category", "news"),
                "active": False,
                "added_at": "",
                "updated_at": "",
                "last_synced_at": "",
                "last_sync_count": 0,
                "last_sync_status": "",
            })
            seen_filter_ids.add(source_id)
    source_filters = [{"id": "All Sources", "name": "All Sources"}] + sources + extra_sources
    selected_source = str(request.args.get("source", "All Sources") or "All Sources").strip()
    raw_category = str(request.args.get("category", "All Categories") or "All Categories").strip()
    selected_category = "All Categories" if raw_category.lower() == "all categories" else normalize_reading_category(raw_category)
    raw_status = str(request.args.get("status", "All Status") or "All Status").strip().lower()
    selected_status = "All Status" if raw_status == "all status" else normalize_reading_status(raw_status)
    raw_search = str(request.args.get("search", "") or "").strip()
    search = raw_search.lower()
    fresh_only = str(request.args.get("fresh", "") or "").strip().lower() in {"1", "true", "yes", "on"}
    filtered = entries[:]
    if selected_source != "All Sources":
        filtered = [
            entry for entry in filtered
            if entry.get("source_id") == selected_source or entry.get("source") == selected_source
        ]
    if selected_status != "All Status":
        filtered = [entry for entry in filtered if entry.get("status") == selected_status]
    if selected_category != "All Categories":
        filtered = [entry for entry in filtered if entry.get("category") == selected_category]
    if search:
        filtered = [
            entry for entry in filtered
            if search in " ".join([
                entry.get("title", ""),
                entry.get("source", ""),
                entry.get("topic", ""),
                entry.get("url", ""),
            ]).lower()
        ]
    fresh_entries = [entry for entry in filtered if entry.get("is_fresh_import")]
    fresh_entries.sort(key=reading_entry_sort_key, reverse=True)
    if fresh_only and last_sync_timestamp:
        filtered = fresh_entries[:]
    filtered.sort(key=reading_entry_sort_key, reverse=True)
    fresh_count = len(fresh_entries)
    summary = {
        "total": len(entries),
        "unread": len([entry for entry in entries if entry.get("status") == "unread"]),
        "reading": len([entry for entry in entries if entry.get("status") == "reading"]),
        "starred": len([entry for entry in entries if entry.get("starred")]),
    }
    last_sync_at = str(data.get("last_sync_at", "") or "").strip()
    return {
        "entries": filtered,
        "sources": source_filters,
        "source_options": source_filters,
        "reading_sources": sources,
        "status_options": [("All Status", "All Status")] + [(status, status.title()) for status in READING_STATUSES],
        "category_options": [("All Categories", "All Categories")] + [(category, reading_category_label(category)) for category in READING_CATEGORIES],
        "current_filters": {
            "source": selected_source,
            "status": selected_status,
            "category": selected_category,
            "search": raw_search,
        },
        "filter_query": reading_filter_query_params({
            "source": selected_source,
            "status": selected_status,
            "category": selected_category,
            "search": raw_search,
        }),
        "summary": summary,
        "source_count": len(sources),
        "active_source_count": len([source for source in sources if source.get("active", True) and source.get("url")]),
        "source_entry_count": source_entry_count,
        "total_filtered": len(filtered),
        "fresh_only": fresh_only,
        "fresh_count": fresh_count,
        "fresh_label": "New since last sync" if fresh_count else "Up to date",
        "last_sync_at": last_sync_at,
        "last_sync_at_display": format_timestamp_label(last_sync_at, default="Never"),
        "last_sync_count": int(data.get("last_sync_count", 0) or 0),
        "last_sync_sources": int(data.get("last_sync_sources", 0) or 0),
        "last_sync_message": str(data.get("last_sync_message", "") or "").strip(),
    }


def build_reading_admin_context():
    data = load_reading_data()
    sources = [normalize_reading_source(source, index) for index, source in enumerate(data.get("sources", []))]
    rss_sources = [source for source in sources if reading_is_rss_source(source)]
    source_lookup = {source["name"].lower(): source["id"] for source in sources if source.get("name")}
    source_lookup.update({normalize_reading_url(source.get("url", "")).lower(): source["id"] for source in sources if source.get("url")})
    source_lookup.update({source["id"]: source["id"] for source in sources})
    source_category_lookup = {source["id"]: source.get("category", "news") for source in sources}
    source_category_lookup.update({source["name"].lower(): source.get("category", "news") for source in sources if source.get("name")})
    entries = [normalize_reading_entry(entry, index, source_lookup=source_lookup, source_category_lookup=source_category_lookup) for index, entry in enumerate(data.get("entries", []))]
    source_entry_count = {}
    for entry in entries:
        key = entry.get("source_id") or entry.get("source", "")
        source_entry_count[key] = source_entry_count.get(key, 0) + 1
    for source in sources:
        known_entries = int(source_entry_count.get(source.get("id", ""), 0) or source_entry_count.get(source.get("name", ""), 0) or 0)
        source["known_entries_count"] = known_entries
        source["last_sync_reason"] = source.get("last_sync_reason") or reading_source_sync_reason(source)
        source["health"] = reading_source_health(source, known_entries=known_entries)
        source["health_label"] = reading_source_health_label(source, known_entries=known_entries)
        source["last_synced_display"] = format_timestamp_label(source.get("last_synced_at", ""), default="Never")
    for source in rss_sources:
        source["rss_known_entries_count"] = int(source.get("known_entries_count", 0) or 0)
    backup_files = list_reading_backup_files()
    recovery_hits = collect_reading_recovery_trace_hits()
    return {
        "reading_sources": sources,
        "reading_rss_sources": rss_sources,
        "reading_source_count": len(sources),
        "reading_rss_source_count": len(rss_sources),
        "reading_active_source_count": len([source for source in sources if source.get("active", True) and source.get("url")]),
        "reading_rss_active_source_count": len([source for source in rss_sources if source.get("active", True) and source.get("url")]),
        "reading_source_entry_count": source_entry_count,
        "reading_category_options": [(category, reading_category_label(category)) for category in READING_CATEGORIES],
        "reading_summary": {
            "total": len(entries),
            "unread": len([entry for entry in entries if entry.get("status") == "unread"]),
            "reading": len([entry for entry in entries if entry.get("status") == "reading"]),
            "starred": len([entry for entry in entries if entry.get("starred")]),
        },
        "reading_last_sync_at": str(data.get("last_sync_at", "") or "").strip(),
        "reading_last_sync_at_display": format_timestamp_label(str(data.get("last_sync_at", "") or "").strip(), default="Never"),
        "reading_last_sync_count": int(data.get("last_sync_count", 0) or 0),
        "reading_last_sync_sources": int(data.get("last_sync_sources", 0) or 0),
        "reading_last_sync_message": str(data.get("last_sync_message", "") or "").strip(),
        "reading_backup_files": backup_files,
        "reading_backup_count": len(backup_files),
        "reading_latest_backup": backup_files[0] if backup_files else {},
        "reading_recovery_hits": recovery_hits,
        "reading_recovery_hit_count": len(recovery_hits),
    }


def is_cache_entry_stale(entry, max_age_seconds=CACHE_MAX_AGE_SECONDS):
    if not entry or not isinstance(entry, dict):
        return True
    timestamp = parse_timestamp(entry.get("updated_at"))
    if not timestamp:
        return True
    return (datetime.now().astimezone() - timestamp).total_seconds() > max_age_seconds


def load_cache_data():
    with CACHE_DATA_LOCK:
        data = load_json_file(CACHE_DATA_PATH, {})
        if not isinstance(data, dict):
            data = {}
        data.setdefault("films", {})
        data.setdefault("youtube_playlists", {})
        data.setdefault("youtube_section_feeds", {})
        return data


def save_cache_data(cache_data):
    with CACHE_DATA_LOCK:
        save_json_file(CACHE_DATA_PATH, cache_data)


def set_cache_entry(cache_data, key, payload):
    cache_data[key] = {"updated_at": current_timestamp(), "data": payload}


def clone_film_rows(films):
    return [dict(film) for film in films or [] if isinstance(film, dict)]


def get_persisted_film_cache_entry(key, force_refresh=False, allow_stale=False):
    if force_refresh:
        return None, None
    cache_data = load_cache_data()
    entry = cache_data.get("films", {}).get(key)
    if not isinstance(entry, dict):
        return None, None
    stale = is_cache_entry_stale(entry)
    if stale and not allow_stale:
        return None, True
    data = entry.get("data", [])
    if not isinstance(data, list):
        return None, stale
    freshness = "stale" if stale else "fresh"
    print(f"[movie-source-cache] key={key} source=disk-{freshness} rows={len(data)}")
    return clone_film_rows(data), stale


def set_persisted_film_cache_entry(key, films):
    cache_data = load_cache_data()
    cache_data.setdefault("films", {})
    set_cache_entry(cache_data["films"], key, clone_film_rows(films))
    save_cache_data(cache_data)


def clear_runtime_film_cache_keys(keys=None):
    with RUNTIME_CACHE_LOCK:
        if keys is None:
            RUNTIME_CACHE["films"] = None
            RUNTIME_CACHE["library_films"] = {}
            RUNTIME_CACHE["want_to_union_films"] = None
            return
        keys = set(keys)
        if "all" in keys:
            RUNTIME_CACHE["films"] = None
        if "want_to_union" in keys:
            RUNTIME_CACHE["want_to_union_films"] = None
        if {"library_union_enabled", "library_union_disabled"} & keys:
            RUNTIME_CACHE["library_films"] = {}


def schedule_movie_cache_refresh(cache_key, refresh_fn):
    with RUNTIME_CACHE_LOCK:
        refreshing = RUNTIME_CACHE.setdefault("refreshing", {})
        if refreshing.get(cache_key):
            return False
        refreshing[cache_key] = True

    def _runner():
        try:
            print(f"[movie-source-cache] key={cache_key} refresh=background-start")
            refresh_fn()
            print(f"[movie-source-cache] key={cache_key} refresh=background-done")
        except Exception as exc:
            print(f"[movie-source-cache] key={cache_key} refresh=background-failed error={type(exc).__name__}: {exc}")
        finally:
            with RUNTIME_CACHE_LOCK:
                RUNTIME_CACHE.setdefault("refreshing", {})[cache_key] = False

    threading.Thread(target=_runner, daemon=True).start()
    return True


def is_movie_cache_refresh_pending(cache_key):
    with RUNTIME_CACHE_LOCK:
        return bool(RUNTIME_CACHE.setdefault("refreshing", {}).get(cache_key))


def clear_film_cache_entry(cache_data=None, keys=None):
    payload = cache_data if isinstance(cache_data, dict) else load_cache_data()
    changed = False
    if "films" in payload:
        if keys is None:
            payload["films"] = {}
            changed = True
        else:
            film_keys = payload["films"]
            for key in keys:
                if key in film_keys:
                    film_keys.pop(key, None)
                    changed = True
    if changed:
        save_cache_data(payload)


def set_playlist_cache_entry(cache_data, playlist_id, payload):
    cache_data.setdefault("youtube_playlists", {})
    cache_data["youtube_playlists"][playlist_id] = {
        "updated_at": current_timestamp(),
        "data": payload
    }


def get_persisted_youtube_section_feed_entry(key, force_refresh=False, allow_stale=False):
    if force_refresh:
        return None, None
    cache_data = load_cache_data()
    entry = cache_data.get("youtube_section_feeds", {}).get(key)
    if not isinstance(entry, dict):
        return None, None
    stale = is_cache_entry_stale(entry)
    if stale and not allow_stale:
        return None, True
    data = entry.get("data", [])
    if not isinstance(data, dict):
        return None, stale
    freshness = "stale" if stale else "fresh"
    print(f"[youtube-group-cache] key={key} source=disk-{freshness}")
    return json.loads(json.dumps(data)), stale


def set_persisted_youtube_section_feed_entry(key, payload):
    cache_data = load_cache_data()
    cache_data.setdefault("youtube_section_feeds", {})
    cache_data["youtube_section_feeds"][key] = {
        "updated_at": current_timestamp(),
        "data": json.loads(json.dumps(payload)),
    }
    save_cache_data(cache_data)


def clear_persisted_youtube_section_feed_cache(keys=None):
    cache_data = load_cache_data()
    feeds = cache_data.get("youtube_section_feeds", {})
    if not isinstance(feeds, dict):
        feeds = {}
    changed = False
    if keys is None:
        if feeds:
            cache_data["youtube_section_feeds"] = {}
            changed = True
    else:
        for key in list(keys):
            if key in feeds:
                feeds.pop(key, None)
                changed = True
    if changed:
        save_cache_data(cache_data)


def schedule_youtube_cache_refresh(cache_key, refresh_fn):
    with RUNTIME_CACHE_LOCK:
        refreshing = RUNTIME_CACHE.setdefault("refreshing", {})
        if refreshing.get(cache_key):
            return False
        refreshing[cache_key] = True

    def _runner():
        try:
            print(f"[youtube-group-cache] key={cache_key} refresh=background-start")
            refresh_fn()
            print(f"[youtube-group-cache] key={cache_key} refresh=background-done")
        except Exception as exc:
            print(f"[youtube-group-cache] key={cache_key} refresh=background-failed error={type(exc).__name__}: {exc}")
        finally:
            with RUNTIME_CACHE_LOCK:
                RUNTIME_CACHE.setdefault("refreshing", {})[cache_key] = False

    threading.Thread(target=_runner, daemon=True).start()
    return True


def is_youtube_cache_refresh_pending(cache_key):
    with RUNTIME_CACHE_LOCK:
        return bool(RUNTIME_CACHE.setdefault("refreshing", {}).get(cache_key))


def apply_cached_durations(videos):
    for video in videos:
        duration = YOUTUBE_DURATION_CACHE.get(video.get("video_id", ""), {"seconds": 0, "display": "0:00"})
        video["duration_seconds"] = duration.get("seconds", 0)
        video["duration"] = duration.get("display", "0:00")
    return videos


def clear_runtime_cache():
    with RUNTIME_CACHE_LOCK:
        RUNTIME_CACHE["films"] = None
        RUNTIME_CACHE["library_films"] = {}
        RUNTIME_CACHE["want_to_union_films"] = None
        RUNTIME_CACHE["youtube_playlists"] = {}
        RUNTIME_CACHE["youtube_channel_debug"] = {}
        RUNTIME_CACHE["youtube_section_picks"] = {}
        RUNTIME_CACHE["youtube_section_feeds"] = {}
        RUNTIME_CACHE["youtube_channel_latest_uploads"] = {}
        RUNTIME_CACHE["youtube_channel_group_feed_videos"] = {}
        RUNTIME_CACHE["refreshing"] = {}
        RUNTIME_CACHE["initialized"] = False


def clear_youtube_runtime_cache():
    with RUNTIME_CACHE_LOCK:
        RUNTIME_CACHE["youtube_playlists"] = {}
        RUNTIME_CACHE["youtube_channel_debug"] = {}
        RUNTIME_CACHE["youtube_section_picks"] = {}
        RUNTIME_CACHE["youtube_section_feeds"] = {}
        RUNTIME_CACHE["youtube_channel_latest_uploads"] = {}
        RUNTIME_CACHE["youtube_channel_group_feed_videos"] = {}


def reset_playlists_metadata():
    if not PLAYLISTS_PATH.exists():
        return
    data = load_json_file(PLAYLISTS_PATH, {})
    if not isinstance(data, dict):
        return
    changed = False
    for transient_key in ("_local_deleted_videos",):
        if transient_key in data:
            data.pop(transient_key, None)
            changed = True
    if changed:
        save_json_file(PLAYLISTS_PATH, data)


def get_requested_limit(default=10):
    raw_value = request.args.get("limit", str(default))
    if raw_value == "All":
        return "All"
    try:
        return max(int(raw_value), 1)
    except (TypeError, ValueError):
        return default


def apply_limit(items, limit_value):
    if limit_value == "All":
        return list(items)
    return list(items)[:max(int(limit_value), 1)]


def build_youtube_section_playlists(section_name, admin_data=None):
    playlists = get_section_playlists(section_name)
    limit_value = get_requested_limit(10)
    requested_page = request.args.get("playlist_page", "")
    if not requested_page and not request.args.get("per_page"):
        requested_page = request.args.get("page", "1")
    if not requested_page:
        requested_page = "1"
    shuffle_playlist_id = (request.args.get("shuffle_playlist") or "").strip()
    shuffle_seed = (request.args.get("shuffle_seed") or "").strip()
    try:
        requested_page = max(int(requested_page), 1)
    except (TypeError, ValueError):
        requested_page = 1

    section_profile = youtube_section_blueprint(section_name)
    section_profile.update(build_youtube_channel_curation_context(section_name, admin_data=admin_data))
    playlists_with_videos = []
    section_channel_inputs = []
    is_german_section = normalize_section_name(section_name) == normalize_section_name("German")
    for index, pl in enumerate(playlists):
        videos = get_all_playlist_videos(pl["id"])
        # Some German admin playlists were cached from an earlier bad fetch with only
        # the first item. Retry through the normal playlistItems helper to refresh the
        # cache when the count looks suspiciously low.
        if (
            is_german_section
            and str(pl.get("source", "") or "").strip().lower() == "admin"
            and len(videos) <= 1
        ):
            refreshed_videos = get_all_playlist_videos(pl["id"], force_refresh=True)
            if len(refreshed_videos) >= len(videos):
                videos = refreshed_videos
        ordered_videos = list(videos)
        shuffle_active = bool(shuffle_seed) and shuffle_playlist_id == pl["id"]
        if shuffle_active:
            ordered_videos = build_shuffled_related_entries(ordered_videos, shuffle_seed)
        channel_groups = build_youtube_channel_groups(
            videos,
            section_name=section_name,
            section_profile=section_profile,
            source_playlist={"id": pl["id"], "name": pl["name"]},
        )
        section_channel_inputs.extend([
            dict(video, playlist_name=pl["name"], playlist_id=pl["id"])
            for video in videos
            if isinstance(video, dict)
        ])
        if limit_value == "All":
            paginated = {
                "items": list(ordered_videos),
                "page": 1,
                "total": len(ordered_videos),
                "total_pages": 1,
                "pagination": [1],
            }
        else:
            paginated = paginate_items(ordered_videos, requested_page, int(limit_value))
        playlists_with_videos.append({
            "name": pl["name"],
            "playlist_id": pl["id"],
            "url": pl["url"],
            "source": pl.get("source", ""),
            "source_section_key": pl.get("source_section_key", ""),
            "source_category_key": pl.get("source_category_key", ""),
            "videos": paginated["items"],
            "total_videos": paginated["total"],
            "current_page": paginated["page"],
            "total_pages": paginated["total_pages"],
            "pagination_numbers": paginated["pagination"],
            "shuffle_active": shuffle_active,
            "sort_index": index,
            "has_videos": bool(videos),
            "channel_groups": channel_groups,
            "channel_group_count": len(channel_groups),
        })

    playlists_with_videos.sort(key=lambda pl: (not pl["has_videos"], pl["sort_index"]))
    for pl in playlists_with_videos:
        pl.pop("sort_index", None)
        pl.pop("has_videos", None)
    section_channel_groups = build_youtube_channel_groups(
        section_channel_inputs,
        section_name=section_name,
        section_profile=section_profile,
    )
    return playlists_with_videos, limit_value, section_channel_groups


def youtube_dependencies_ready():
    return all([GoogleAuthRequest, Credentials, Flow, build])


def youtube_oauth_ready():
    return youtube_dependencies_ready() and YOUTUBE_CLIENT_SECRET_PATH.exists()


def load_deleted_video_records():
    data = load_json_file(PLAYLISTS_PATH, {})
    records = data.get("_local_deleted_videos", [])
    return records if isinstance(records, list) else []


def load_deleted_video_lookup():
    return {
        record.get("playlist_item_id")
        for record in load_deleted_video_records()
        if record.get("playlist_item_id")
    }


def append_deleted_history(video):
    history = load_json_file(DELETED_HISTORY_PATH, [])
    if not isinstance(history, list):
        history = []
    history.insert(0, {
        "title": video.get("title") or video.get("name") or "Untitled",
        "url": video.get("url") or f"https://www.youtube.com/watch?v={video.get('video_id', '')}",
        "thumbnail_url": video.get("thumb") or get_best_thumbnail({}, video.get("video_id", "")),
        "playlist_name": video.get("playlist_name") or "Unknown Playlist",
        "date_deleted": current_timestamp()
    })
    save_json_file(DELETED_HISTORY_PATH, history)


def sync_deleted_video_to_playlists(video):
    data = load_json_file(PLAYLISTS_PATH, {})
    deleted_records = data.setdefault("_local_deleted_videos", [])
    playlist_item_id = video.get("playlist_item_id")
    if not playlist_item_id:
        return
    if any(item.get("playlist_item_id") == playlist_item_id for item in deleted_records if isinstance(item, dict)):
        return
    deleted_records.append({
        "playlist_item_id": playlist_item_id,
        "playlist_id": video.get("playlist_id", ""),
        "video_id": video.get("video_id", ""),
        "playlist_name": video.get("playlist_name", ""),
        "title": video.get("title", ""),
        "deleted_at": current_timestamp()
    })
    save_json_file(PLAYLISTS_PATH, data)


def remove_video_from_local_playlists_cache(video):
    target_playlist_item_id = video.get("playlist_item_id")
    target_video_id = video.get("video_id")
    target_playlist_id = video.get("playlist_id")

    def prune(value):
        if isinstance(value, list):
            kept = []
            changed = False
            for item in value:
                if isinstance(item, dict):
                    same_playlist_item = target_playlist_item_id and item.get("playlist_item_id") == target_playlist_item_id
                    same_video = target_video_id and item.get("video_id") == target_video_id
                    same_playlist = not target_playlist_id or item.get("playlist_id") == target_playlist_id
                    if same_playlist_item or (same_video and same_playlist):
                        changed = True
                        continue
                new_item, item_changed = prune(item)
                kept.append(new_item)
                changed = changed or item_changed
            return kept, changed
        if isinstance(value, dict):
            updated = {}
            changed = False
            for key, item in value.items():
                new_item, item_changed = prune(item)
                updated[key] = new_item
                changed = changed or item_changed
            return updated, changed
        return value, False

    playlists_data = load_json_file(PLAYLISTS_PATH, {})
    updated_playlists_data, playlists_changed = prune(playlists_data)
    if playlists_changed:
        save_json_file(PLAYLISTS_PATH, updated_playlists_data)

    cache_data = load_cache_data()
    updated_cache_data, cache_changed = prune(cache_data)
    if cache_changed:
        save_cache_data(updated_cache_data)

    if target_playlist_id:
        with RUNTIME_CACHE_LOCK:
            runtime_videos = RUNTIME_CACHE["youtube_playlists"].get(target_playlist_id)
            if runtime_videos is not None:
                RUNTIME_CACHE["youtube_playlists"][target_playlist_id], _ = prune(runtime_videos)


def describe_http_error(exc):
    status = getattr(getattr(exc, "resp", None), "status", None)
    content = getattr(exc, "content", b"")
    if isinstance(content, bytes):
        content = content.decode("utf-8", errors="replace")
    return status, content or str(exc)


def find_playlist_item_id(service, playlist_id, video_id):
    if not service or not playlist_id or not video_id:
        return ""
    next_page_token = None
    while True:
        response = service.playlistItems().list(
            part="snippet",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        ).execute()
        for item in response.get("items", []):
            snippet = item.get("snippet", {})
            resource = snippet.get("resourceId") or {}
            if resource.get("videoId") == video_id:
                return item.get("id", "")
        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break
    return ""


def load_youtube_credentials():
    if not youtube_dependencies_ready() or not YOUTUBE_TOKEN_PATH.exists():
        return None
    try:
        credentials = Credentials.from_authorized_user_file(str(YOUTUBE_TOKEN_PATH), YOUTUBE_OAUTH_SCOPES)
    except Exception:
        return None
    if credentials and credentials.expired and credentials.refresh_token:
        try:
            credentials.refresh(GoogleAuthRequest())
            YOUTUBE_TOKEN_PATH.write_text(credentials.to_json(), encoding="utf-8")
        except Exception:
            return None
    return credentials if credentials and credentials.valid else None


def build_youtube_service():
    credentials = load_youtube_credentials()
    if not credentials:
        return None
    return build("youtube", "v3", credentials=credentials)


def get_youtube_auth_url(next_url):
    return url_for("youtube_auth_start", next=next_url)


def build_query_url(base_args, **updates):
    params = dict(base_args)
    for key, value in updates.items():
        params[key] = value
    return "?" + urllib.parse.urlencode(params)


def append_query_param(url, **updates):
    parsed = urllib.parse.urlsplit(str(url or ""))
    params = dict(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True))
    for key, value in updates.items():
        if value in (None, ""):
            params.pop(key, None)
        else:
            params[key] = value
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urllib.parse.urlencode(params), parsed.fragment))


def canonical_section_name(value):
    cleaned = (value or "").strip()
    normalized = normalize_section_name(cleaned)
    aliases = {
        "german": "German",
        "chess": "Chess",
        "library": "Library",
        "ytlibrary": "Library",
        "youtubewatchlater": "YouTube Watch Later",
        "watchlater": "YouTube Watch Later",
    }
    return aliases.get(normalized, cleaned)


YOUTUBE_SECTION_BLUEPRINTS = {
    "german": {
        "section_kind": "curated",
        "section_scope": "study",
        "channel_group_key": "study",
        "channel_group_label": "Study",
        "section_order": 10,
    },
    "chess": {
        "section_kind": "curated",
        "section_scope": "study",
        "channel_group_key": "study",
        "channel_group_label": "Study",
        "section_order": 20,
    },
    "library": {
        "section_kind": "curated",
        "section_scope": "reference",
        "channel_group_key": "reference",
        "channel_group_label": "Reference",
        "section_order": 30,
    },
    "youtubewatchlater": {
        "section_kind": "raw",
        "section_scope": "queue",
        "channel_group_key": "queue",
        "channel_group_label": "Queue",
        "section_order": 40,
    },
}

YOUTUBE_SECTION_DEFAULT_BLUEPRINT = {
    "section_kind": "curated",
    "section_scope": "custom",
    "channel_group_key": "custom",
    "channel_group_label": "Custom",
    "section_order": 999,
}


def youtube_section_blueprint(section_name):
    normalized = normalize_section_name(section_name)
    blueprint = dict(YOUTUBE_SECTION_DEFAULT_BLUEPRINT)
    blueprint.update(YOUTUBE_SECTION_BLUEPRINTS.get(normalized, {}))
    return blueprint


def normalize_youtube_section_record(section):
    if not isinstance(section, dict):
        return {}
    section_name = canonical_section_name(section.get("name", ""))
    blueprint = youtube_section_blueprint(section_name)
    normalized_section = dict(section)
    normalized_section["name"] = section_name
    normalized_section["slug"] = section_slug(section_name)
    for key, value in blueprint.items():
        if normalized_section.get(key) in (None, ""):
            normalized_section[key] = value
    channel_groups = normalized_section.get("channel_groups")
    normalized_section["channel_groups"] = channel_groups if isinstance(channel_groups, list) else []
    return normalized_section


def parse_playlist_input(raw_value):
    raw_value = (raw_value or "").strip()
    if not raw_value:
        raise ValueError("Playlist URL is required.")
    parsed = urllib.parse.urlparse(raw_value)
    playlist_id = ""
    if parsed.scheme and parsed.netloc:
        query_list = urllib.parse.parse_qs(parsed.query).get("list", [])
        if query_list:
            playlist_id = query_list[0].strip()
    elif re.fullmatch(r"[A-Za-z0-9_-]{10,}", raw_value):
        playlist_id = raw_value
    if not playlist_id or playlist_id == "PASTE_PLAYLIST_ID_HERE":
        raise ValueError("Enter a valid YouTube playlist URL or playlist ID.")
    return playlist_id, f"https://www.youtube.com/playlist?list={playlist_id}"


def _empty_admin_data():
    return {
        "sections": [],
        "youtube_channel_curation": {"channels": []},
        "youtube_pockettube_imports": {"latest": {}, "imports": []},
    }


def _youtube_channel_tier_rank(tier):
    normalized = normalize_section_name(tier)
    if normalized == "best":
        return 0
    if normalized == "favorite":
        return 1
    return 9


def _sanitize_youtube_curated_channel(item):
    if not isinstance(item, dict):
        return None
    channel_name = str(item.get("channel_name", "") or item.get("name", "") or "").strip()
    channel_id = str(item.get("channel_id", "") or item.get("channelId", "") or "").strip()
    if not channel_name:
        return None
    tier = str(item.get("tier", "") or item.get("status", "") or "").strip().lower()
    if tier not in {"best", "favorite"}:
        tier = "favorite"
    section_name = canonical_section_name(item.get("section_name", "") or item.get("section", ""))
    group_name = str(item.get("group_name", "") or item.get("group", "") or "").strip()
    if not group_name:
        group_name = section_name or tier.title()
    channel_key = normalize_section_name(str(item.get("channel_key", "") or channel_name))
    return {
        "channel_name": channel_name,
        "channel_id": channel_id,
        "channel_key": channel_key,
        "tier": tier,
        "section_name": section_name,
        "section_key": normalize_section_name(section_name),
        "group_name": group_name,
        "group_key": normalize_section_name(group_name),
        "notes": str(item.get("notes", "") or "").strip(),
        "source": str(item.get("source", "") or "admin").strip() or "admin",
    }


def normalize_youtube_channel_curation(raw):
    source = raw if isinstance(raw, dict) else {}
    records = []
    seen = set()

    def add_record(item, tier_override=None):
        candidate = dict(item or {})
        if tier_override and not candidate.get("tier"):
            candidate["tier"] = tier_override
        record = _sanitize_youtube_curated_channel(candidate)
        if not record:
            return
        record_key = (
            record.get("channel_key", ""),
            record.get("tier", ""),
            record.get("section_key", ""),
            record.get("group_key", ""),
        )
        if record_key in seen:
            return
        seen.add(record_key)
        records.append(record)

    for item in source.get("channels", []):
        if isinstance(item, dict):
            add_record(item)
        elif isinstance(item, str) and item.strip():
            add_record({"channel_name": item.strip(), "tier": "favorite"})

    for tier_name, default_tier in (("best_channels", "best"), ("favorite_channels", "favorite")):
        for item in source.get(tier_name, []):
            if isinstance(item, dict):
                add_record(item, tier_override=default_tier)
            elif isinstance(item, str) and item.strip():
                add_record({"channel_name": item.strip(), "tier": default_tier})

    records.sort(
        key=lambda item: (
            _youtube_channel_tier_rank(item.get("tier", "")),
            item.get("section_name", "").lower(),
            item.get("group_name", "").lower(),
            item.get("channel_name", "").lower(),
        )
    )
    return {"channels": records}


def build_youtube_channel_curation_context(section_name="", admin_data=None):
    data = admin_data if isinstance(admin_data, dict) else load_admin_data()
    curation = normalize_youtube_channel_curation(data.get("youtube_channel_curation", {}))
    section_key = normalize_section_name(section_name)
    curated_channels = []
    curated_channel_lookup = {}
    for record in curation.get("channels", []):
        record_section_key = record.get("section_key", "")
        if section_key and record_section_key and record_section_key != section_key:
            continue
        curated_channels.append(record)
        channel_key = record.get("channel_key", "")
        existing = curated_channel_lookup.get(channel_key)
        if existing is None or _youtube_channel_tier_rank(record.get("tier", "")) < _youtube_channel_tier_rank(existing.get("tier", "")):
            curated_channel_lookup[channel_key] = record
    curated_group_keys = sorted({
        record.get("group_key", "")
        for record in curated_channels
        if record.get("group_key", "")
    })
    return {
        "youtube_channel_curation": curation,
        "curated_channels": curated_channels,
        "curated_channel_lookup": curated_channel_lookup,
        "curated_channel_count": len(curated_channels),
        "curated_group_count": len(curated_group_keys),
        "curated_group_keys": curated_group_keys,
        "best_channel_count": sum(1 for record in curated_channels if record.get("tier") == "best"),
        "favorite_channel_count": sum(1 for record in curated_channels if record.get("tier") == "favorite"),
    }


def _pockettube_first_string(item, keys, default=""):
    if not isinstance(item, dict):
        return default
    for key in keys:
        value = item.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        if value not in (None, "", []):
            return str(value).strip()
    return default


def _pockettube_guess_channel_id(value):
    candidate = str(value or "").strip()
    if re.fullmatch(r"UC[a-zA-Z0-9_-]{20,}", candidate):
        return candidate
    return ""


def _pockettube_flag(item, keys):
    if not isinstance(item, dict):
        return False
    for key in keys:
        value = item.get(key)
        if isinstance(value, bool):
            if value:
                return True
        elif isinstance(value, str) and value.strip().lower() in {"1", "true", "yes", "best", "favorite"}:
            return True
    return False


def _pockettube_channel_record(item, section_name="", group_name="", default_tier="favorite", source_name="pockettube"):
    if isinstance(item, str):
        item = {"channel_name": item}
    if not isinstance(item, dict):
        return None
    channel_name = _pockettube_first_string(item, ("channel_name", "name", "title", "channelTitle", "label"))
    channel_id = _pockettube_first_string(item, ("channel_id", "channelId", "id", "browse_id", "browseId"))
    if not channel_id:
        channel_id = _pockettube_guess_channel_id(channel_name)
    if not channel_name and not channel_id:
        return None
    tier = _pockettube_first_string(item, ("tier", "status", "group_tier", "favorite_type", "favorite"), default_tier).lower()
    if _pockettube_flag(item, ("best", "is_best", "best_channel", "isBest")):
        tier = "best"
    elif _pockettube_flag(item, ("favorite", "is_favorite", "favorite_channel", "isFavorite")):
        tier = "favorite"
    if tier not in {"best", "favorite"}:
        tier = default_tier if default_tier in {"best", "favorite"} else "favorite"
    section_name = canonical_section_name(_pockettube_first_string(item, ("section_name", "section", "sectionTitle", "category", "collection"), section_name))
    group_name = str(_pockettube_first_string(item, ("group_name", "group", "groupTitle", "label", "name"), group_name) or "").strip() or section_name
    channel_key_source = channel_id or channel_name
    channel_key = normalize_section_name(channel_key_source)
    if not channel_key:
        return None
    return {
        "channel_name": channel_name or channel_key_source,
        "channel_id": channel_id,
        "channel_key": channel_key,
        "tier": tier,
        "section_name": section_name,
        "section_key": normalize_section_name(section_name),
        "group_name": group_name,
        "group_key": normalize_section_name(group_name),
        "notes": str(_pockettube_first_string(item, ("notes", "note", "description"), "")).strip(),
        "source": source_name,
    }


def _pockettube_extract_groups(payload):
    groups = []
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict) and any(isinstance(item.get(key), list) for key in ("channels", "items", "videos")):
                groups.append(item)
            else:
                groups.append({"name": "PocketTube", "channels": payload})
                break
        return groups
    if not isinstance(payload, dict):
        return groups

    for key in ("groups", "sections", "collections"):
        value = payload.get(key)
        if isinstance(value, list):
            groups.extend([item for item in value if isinstance(item, dict)])

    if groups:
        return groups

    special_keys = {
        "channels", "favorites", "favorite_channels", "best_channels", "pinned_channels", "hidden_channels",
        "imports", "metadata", "version", "created_at", "updated_at", "source", "source_name",
    }
    for key, value in payload.items():
        if key in special_keys or key.startswith("_") or str(key).startswith("ysc_"):
            continue
        if isinstance(value, list):
            groups.append({"name": key, "channels": value})
        elif isinstance(value, dict) and any(isinstance(value.get(child_key), list) for child_key in ("channels", "items", "videos")):
            group_item = dict(value)
            group_item.setdefault("name", key)
            groups.append(group_item)

    if groups:
        return groups

    if isinstance(payload.get("channels"), list):
        groups.append({"name": payload.get("name", payload.get("title", "PocketTube")), "channels": payload.get("channels", [])})
    return groups


def normalize_pockettube_import_payload(payload):
    groups = _pockettube_extract_groups(payload)
    normalized_sections = []
    normalized_channels = []
    seen_keys = set()
    source_name = "PocketTube"
    settings_map = {}
    main_collection_page = ""
    if isinstance(payload, dict):
        source_name = str(payload.get("source_name") or payload.get("source") or payload.get("name") or source_name).strip() or source_name
        settings_map = payload.get("ysc_settings", {}) if isinstance(payload.get("ysc_settings", {}), dict) else {}
    main_collection_page = normalize_section_name(
        settings_map.get("collection_main_page")
        or settings_map.get("ng")
        or payload.get("collection_main_page")
        or payload.get("ng")
        or ""
        )

    for group in groups:
        group_name = canonical_section_name(_pockettube_first_string(group, ("group_name", "group", "name", "title", "label"), "PocketTube"))
        section_name = canonical_section_name(_pockettube_first_string(group, ("section_name", "section", "sectionTitle", "category", "collection"), group_name))
        default_tier = _pockettube_first_string(group, ("tier", "status", "group_tier"), "").lower()
        if not default_tier:
            default_tier = "favorite" if normalize_section_name(group_name) == main_collection_page else "best"
        if default_tier not in {"best", "favorite"}:
            default_tier = "favorite" if normalize_section_name(group_name) == main_collection_page else "best"

        channel_items = []
        for key in ("channels", "items", "videos", "favorite_channels", "best_channels"):
            value = group.get(key)
            if isinstance(value, list):
                channel_items = value
                break

        section_channels = []
        for item in channel_items:
            channel_record = _pockettube_channel_record(item, section_name=section_name, group_name=group_name, default_tier=default_tier, source_name="pockettube")
            if not channel_record:
                continue
            record_key = (channel_record["channel_key"], channel_record["tier"], channel_record["section_key"], channel_record["group_key"])
            if record_key in seen_keys:
                continue
            seen_keys.add(record_key)
            normalized_channels.append(channel_record)
            section_channels.append(channel_record)

        normalized_sections.append({
            "section_name": section_name,
            "section_key": normalize_section_name(section_name),
            "group_name": group_name,
            "group_key": normalize_section_name(group_name),
            "tier": default_tier,
            "channel_count": len(section_channels),
            "channels": section_channels,
        })

    normalized_channels.sort(
        key=lambda item: (
            _youtube_channel_tier_rank(item.get("tier", "")),
            item.get("section_name", "").lower(),
            item.get("group_name", "").lower(),
            item.get("channel_name", "").lower(),
        )
    )
    normalized_sections.sort(key=lambda item: (item.get("section_name", "").lower(), item.get("group_name", "").lower()))
    fingerprint_source = {
        "sections": normalized_sections,
        "channels": normalized_channels,
    }
    return {
        "source_name": source_name,
        "source_structure": {
            "top_level_groups": [item.get("section_name", "") for item in normalized_sections],
            "main_collection_page": main_collection_page,
            "meta_keys": sorted([key for key in payload.keys() if isinstance(payload, dict) and str(key).startswith("ysc_")]) if isinstance(payload, dict) else [],
        },
        "fingerprint": __import__("hashlib").sha256(json.dumps(fingerprint_source, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest(),
        "imported_at": current_timestamp(),
        "section_count": len(normalized_sections),
        "group_count": len(normalized_sections),
        "channel_count": len(normalized_channels),
        "sections": normalized_sections,
        "channels": normalized_channels,
    }


def merge_pockettube_import_into_admin_data(admin_data, import_summary):
    admin_data = admin_data if isinstance(admin_data, dict) else _empty_admin_data()
    import_summary = import_summary if isinstance(import_summary, dict) else {}
    normalized_import_channels = list(import_summary.get("channels", []) or [])
    curation = normalize_youtube_channel_curation(admin_data.get("youtube_channel_curation", {}))
    current_records = list(curation.get("channels", []))
    by_key = {
        (item.get("channel_key", ""), item.get("tier", ""), item.get("section_key", ""), item.get("group_key", "")): item
        for item in current_records
    }
    for record in normalized_import_channels:
        key = (record.get("channel_key", ""), record.get("tier", ""), record.get("section_key", ""), record.get("group_key", ""))
        if not key[0]:
            continue
        by_key[key] = dict(record)
    merged_channels = sorted(
        by_key.values(),
        key=lambda item: (
            _youtube_channel_tier_rank(item.get("tier", "")),
            item.get("section_name", "").lower(),
            item.get("group_name", "").lower(),
            item.get("channel_name", "").lower(),
        )
    )
    admin_data["youtube_channel_curation"] = {"channels": merged_channels}
    pockettube_bucket = dict(admin_data.get("youtube_pockettube_imports", {}) or {})
    imports = list(pockettube_bucket.get("imports", []) or [])
    latest_fingerprint = str(import_summary.get("fingerprint", "") or "").strip()
    if not imports or str(imports[-1].get("fingerprint", "") or "").strip() != latest_fingerprint:
        imports.append({
            "source_name": import_summary.get("source_name", "PocketTube"),
            "imported_at": import_summary.get("imported_at", current_timestamp()),
            "fingerprint": latest_fingerprint,
            "section_count": import_summary.get("section_count", 0),
            "group_count": import_summary.get("group_count", 0),
            "channel_count": import_summary.get("channel_count", 0),
            "sections": import_summary.get("sections", []),
        })
    pockettube_bucket["imports"] = imports
    pockettube_bucket["latest"] = import_summary
    admin_data["youtube_pockettube_imports"] = pockettube_bucket
    save_admin_data(admin_data)
    clear_youtube_runtime_cache()
    clear_persisted_youtube_section_feed_cache()
    return admin_data


def collect_youtube_section_video_pool(section_name):
    playlists = get_section_playlists(section_name)
    section_videos = []
    playlist_summaries = []
    for playlist in playlists:
        playlist_id = playlist.get("id", "")
        playlist_name = playlist.get("name", "")
        playlist_url = playlist.get("url", "")
        videos = []
        for video in get_all_playlist_videos(playlist_id):
            if not isinstance(video, dict):
                continue
            video_copy = dict(video, playlist_id=playlist_id, playlist_name=playlist_name, playlist_url=playlist_url)
            videos.append(video_copy)
            section_videos.append(video_copy)
        playlist_summaries.append({
            "name": playlist_name,
            "id": playlist_id,
            "url": playlist_url,
            "videos": videos,
            "video_count": len(videos),
        })
    return playlists, playlist_summaries, section_videos


def _pockettube_latest_import_snapshot(admin_data=None):
    data = admin_data if isinstance(admin_data, dict) else load_admin_data()
    pockettube = data.get("youtube_pockettube_imports", {})
    if not isinstance(pockettube, dict):
        pockettube = {}
    latest = pockettube.get("latest", {})
    if not isinstance(latest, dict):
        latest = {}
    sections = latest.get("sections", [])
    if not isinstance(sections, list):
        sections = []
    return latest, sections


def _pockettube_section_membership_context(section_name, admin_data=None):
    latest, sections = _pockettube_latest_import_snapshot(admin_data=admin_data)
    wanted = normalize_pockettube_group_key(section_name)
    section_display_name = canonical_section_name(section_name)
    group_display_name = section_display_name
    section_records = []
    seen_channel_keys = set()
    matched_sections = 0

    for section in sections:
        if not isinstance(section, dict):
            continue
        candidate_section_name = canonical_section_name(section.get("section_name", "") or section.get("group_name", "") or "")
        candidate_group_name = canonical_section_name(section.get("group_name", "") or candidate_section_name or "")
        candidate_section_key = normalize_pockettube_group_key(section.get("section_key", "") or candidate_section_name)
        candidate_group_key = normalize_pockettube_group_key(section.get("group_key", "") or candidate_group_name)
        candidate_keys = {
            candidate_section_key,
            candidate_group_key,
            normalize_pockettube_group_key(candidate_section_name),
            normalize_pockettube_group_key(candidate_group_name),
        }
        if wanted and wanted not in {key for key in candidate_keys if key}:
            continue
        matched_sections += 1
        if not section_display_name:
            section_display_name = candidate_section_name or candidate_group_name or section_name
        if not group_display_name:
            group_display_name = candidate_group_name or candidate_section_name or section_name
        default_tier = str(section.get("tier", "") or "").strip().lower() or "favorite"
        if default_tier not in {"best", "favorite"}:
            default_tier = "favorite"
        for item in section.get("channels", []):
            record = _pockettube_channel_record(
                item,
                section_name=candidate_section_name,
                group_name=candidate_group_name,
                default_tier=default_tier,
                source_name=latest.get("source_name", "pockettube"),
            )
            if not record:
                continue
            dedupe_key = record.get("channel_id", "") or record.get("channel_key", "")
            if not dedupe_key or dedupe_key in seen_channel_keys:
                continue
            seen_channel_keys.add(dedupe_key)
            section_records.append(record)

    return {
        "section_name": section_display_name or section_name,
        "group_name": group_display_name or section_name,
        "section_key": normalize_pockettube_group_key(section_display_name or section_name),
        "group_key": normalize_pockettube_group_key(group_display_name or section_name),
        "matched_sections": matched_sections,
        "source_name": latest.get("source_name", "PocketTube"),
        "fingerprint": latest.get("fingerprint", ""),
        "imported_at": latest.get("imported_at", ""),
        "channels": section_records,
        "channel_count": len(section_records),
    }


def _pockettube_display_name(value):
    cleaned = str(value or "").strip()
    if not cleaned:
        return ""
    special = {
        normalize_section_name("German"): "German",
        normalize_section_name("Chess"): "Chess",
        normalize_section_name("Library"): "Library",
        normalize_section_name("YouTube Watch Later"): "YouTube Watch Later",
    }
    normalized = normalize_section_name(cleaned)
    if normalized in special:
        return special[normalized]
    if cleaned.lower() == cleaned:
        return cleaned.title()
    return cleaned


def _youtube_section_feed_cache_key(section_name):
    return normalize_pockettube_group_key(section_name)


def _youtube_channel_latest_video_cache_key(channel_id):
    return normalize_section_name(channel_id)


def _youtube_channel_upload_playlist_id(channel_id):
    channel_id = str(channel_id or "").strip()
    if not channel_id:
        return ""
    cache_key = _youtube_channel_latest_video_cache_key(channel_id)
    with RUNTIME_CACHE_LOCK:
        channel_cache = RUNTIME_CACHE.setdefault("youtube_channel_latest_uploads", {})
        cached = channel_cache.get(cache_key, {})
        if isinstance(cached, dict) and cached.get("uploads_playlist_id"):
            return str(cached.get("uploads_playlist_id", "") or "").strip()
    if not YOUTUBE_API_KEY:
        return ""
    params = {
        "part": "contentDetails",
        "id": channel_id,
        "key": YOUTUBE_API_KEY,
    }
    try:
        response = requests.get("https://www.googleapis.com/youtube/v3/channels", params=params, timeout=15)
        if response.status_code != 200:
            return ""
        data = response.json() or {}
    except Exception:
        return ""
    uploads_playlist_id = ""
    for item in data.get("items", []) or []:
        uploads_playlist_id = (
            item.get("contentDetails", {})
            .get("relatedPlaylists", {})
            .get("uploads", "")
        )
        if uploads_playlist_id:
            break
    with RUNTIME_CACHE_LOCK:
        RUNTIME_CACHE.setdefault("youtube_channel_latest_uploads", {})[cache_key] = {
            "channel_id": channel_id,
            "uploads_playlist_id": uploads_playlist_id,
            "updated_at": current_timestamp(),
        }
    return str(uploads_playlist_id or "").strip()


def _youtube_channel_latest_video_summary(channel_id, channel_name="", fallback_video=None):
    channel_id = str(channel_id or "").strip()
    cache_key = _youtube_channel_latest_video_cache_key(channel_id or channel_name)
    with RUNTIME_CACHE_LOCK:
        channel_cache = RUNTIME_CACHE.setdefault("youtube_channel_latest_uploads", {})
        cached = channel_cache.get(cache_key, {})
        if isinstance(cached, dict) and isinstance(cached.get("latest_video"), dict):
            return json.loads(json.dumps(cached.get("latest_video", {})))

    fallback_video = fallback_video if isinstance(fallback_video, dict) else {}
    if not channel_id:
        if fallback_video:
            return build_youtube_channel_video_summary(fallback_video)
        return {}

    uploads_playlist_id = _youtube_channel_upload_playlist_id(channel_id)
    if not uploads_playlist_id:
        if fallback_video:
            return build_youtube_channel_video_summary(fallback_video)
        return {}

    latest_video = {}
    if YOUTUBE_API_KEY:
        params = {
            "part": "snippet",
            "playlistId": uploads_playlist_id,
            "maxResults": 1,
            "key": YOUTUBE_API_KEY,
        }
        try:
            response = requests.get("https://www.googleapis.com/youtube/v3/playlistItems", params=params, timeout=15)
            if response.status_code == 200:
                data = response.json() or {}
                for item in data.get("items", []) or []:
                    snippet = item.get("snippet", {}) or {}
                    resource = snippet.get("resourceId", {}) or {}
                    video_id = str(resource.get("videoId", "") or "").strip()
                    if not video_id:
                        continue
                    latest_video = {
                        "title": str(snippet.get("title", "") or "").strip(),
                        "video_id": video_id,
                        "playlist_id": uploads_playlist_id,
                        "playlist_item_id": str(item.get("id", "") or "").strip(),
                        "playlist_name": "Uploads",
                        "channel_name": str(snippet.get("videoOwnerChannelTitle") or snippet.get("channelTitle") or channel_name or "").strip() or channel_name or "Unknown Channel",
                        "thumb": get_best_thumbnail(snippet, video_id),
                        "duration": get_youtube_duration(video_id).get("display", "0:00"),
                        "published_at": str(snippet.get("publishedAt", "") or "").strip(),
                        "url": f"https://www.youtube.com/watch?v={video_id}&list={uploads_playlist_id}",
                    }
                    break
        except Exception:
            latest_video = {}

    if not latest_video and fallback_video:
        latest_video = build_youtube_channel_video_summary(fallback_video)

    with RUNTIME_CACHE_LOCK:
        RUNTIME_CACHE.setdefault("youtube_channel_latest_uploads", {})[cache_key] = {
            "channel_id": channel_id,
            "uploads_playlist_id": uploads_playlist_id,
            "latest_video": json.loads(json.dumps(latest_video)),
            "updated_at": current_timestamp(),
        }
    return json.loads(json.dumps(latest_video))


def _youtube_channel_group_feed_cache_key(section_name, channel_id, limit):
    return f"{normalize_section_name(section_name)}:{normalize_section_name(channel_id)}:{int(limit or 0)}"


POCKETTUBE_FEED_PAGE_SIZES = (10, 12, 50, 100, 200, 400)


def _resolve_pockettube_feed_page(value, default=1):
    try:
        page = int(value)
    except (TypeError, ValueError):
        page = default
    return max(page, 1)


def _resolve_pockettube_feed_per_page(value, default=12):
    try:
        per_page = int(value)
    except (TypeError, ValueError):
        per_page = default
    return per_page if per_page in POCKETTUBE_FEED_PAGE_SIZES else default


def _apply_pockettube_feed_pagination(feed_context, page=None, per_page=None):
    context = json.loads(json.dumps(feed_context if isinstance(feed_context, dict) else {}))
    feed_all = list(context.get("feed_all", []) or context.get("feed_preview", []) or context.get("feed_items", []) or [])
    if page is None or per_page is None:
        try:
            request_page = request.args.get("page", 1)
            request_per_page = request.args.get("per_page", 12)
            request_video_types = request.args.get("video_types", "")
            request_video_type = request.args.get("video_type", "videos")
            request_feed_order = request.args.get("feed_order", "normal")
            request_feed_shuffle_seed = request.args.get("feed_shuffle_seed", "")
        except RuntimeError:
            request_page = 1
            request_per_page = 12
            request_video_types = ""
            request_video_type = "videos"
            request_feed_order = "normal"
            request_feed_shuffle_seed = ""
        if page is None:
            page = request_page
        if per_page is None:
            per_page = request_per_page
    else:
        try:
            request_video_types = request.args.get("video_types", "")
            request_video_type = request.args.get("video_type", "videos")
            request_feed_order = request.args.get("feed_order", "normal")
            request_feed_shuffle_seed = request.args.get("feed_shuffle_seed", "")
        except RuntimeError:
            request_video_types = ""
            request_video_type = "videos"
            request_feed_order = "normal"
            request_feed_shuffle_seed = ""

    page = _resolve_pockettube_feed_page(page, default=1)
    per_page = _resolve_pockettube_feed_per_page(per_page, default=12)
    active_video_types = _resolve_pockettube_feed_video_types(
        context.get("video_types", request_video_types or request_video_type),
        default=["videos"],
    )
    feed_order = _normalize_pockettube_feed_order(context.get("feed_order", request_feed_order), default="normal")
    feed_shuffle_seed = str(context.get("feed_shuffle_seed", request_feed_shuffle_seed) or "").strip()
    feed_all = _enrich_pockettube_feed_videos(feed_all, fetch_missing=True)
    feed_all = [
        item for item in feed_all
        if _normalize_pockettube_feed_video_type(item.get("video_type", ""), default="videos") in active_video_types
    ]
    shuffle_pool = _build_pockettube_shuffle_pool(context)
    shuffle_pool = [
        item for item in shuffle_pool
        if _normalize_pockettube_feed_video_type(item.get("video_type", ""), default="videos") in active_video_types
    ]
    shuffle_pool = _enrich_pockettube_feed_videos(shuffle_pool, fetch_missing=True)
    shuffle_pool.sort(key=_pockettube_feed_sort_key)
    shuffle_pool_count = len(shuffle_pool)
    if shuffle_pool_count > 400:
        shuffle_pool = shuffle_pool[:400]
    shuffle_subset_count = len(shuffle_pool)
    context["shuffle_pool_count"] = shuffle_pool_count
    context["shuffle_subset_count"] = shuffle_subset_count
    context["shuffle_candidate_items"] = [dict(item) for item in shuffle_pool]
    if feed_order == "shuffle":
        shuffle_seed = feed_shuffle_seed or context.get("slug", "") or context.get("group_key", "") or "pockettube"
        feed_all = build_shuffled_related_entries(shuffle_pool, shuffle_seed)
    total_count = len(feed_all)
    total_pages = max(math.ceil(total_count / per_page), 1) if total_count else 1
    page = min(page, total_pages)
    start_index = (page - 1) * per_page
    end_index = min(start_index + per_page, total_count)
    page_items = feed_all[start_index:end_index] if feed_all else []
    page_items = apply_cached_durations([dict(item) for item in page_items])

    context["page"] = page
    context["per_page"] = per_page
    context["video_type"] = active_video_types[0] if len(active_video_types) == 1 else "multi"
    context["video_types"] = active_video_types
    context["video_types_csv"] = ",".join(active_video_types)
    context["feed_order"] = feed_order
    context["feed_shuffle_seed"] = feed_shuffle_seed
    context["shuffle_cap"] = 400
    context["total_count"] = total_count
    context["feed_count"] = total_count
    context["video_count"] = total_count
    context["total_pages"] = total_pages
    context["start_index"] = start_index + 1 if total_count else 0
    context["end_index"] = end_index if total_count else 0
    context["has_previous"] = page > 1 and total_count > 0
    context["has_next"] = page < total_pages and total_count > 0
    context["previous_page"] = max(page - 1, 1)
    context["next_page"] = min(page + 1, total_pages)
    context["feed_page_items"] = page_items
    context["feed_preview"] = page_items
    context["feed_items"] = page_items
    context["pagination_numbers"] = [page]
    context["feed_visible"] = bool(context.get("pockettube_group_visible") or int(context.get("channel_count", 0) or 0) > 0)
    return context


def fetch_youtube_channel_group_feed_videos(channel_id, channel_name="", limit=4):
    channel_id = str(channel_id or "").strip()
    channel_name = str(channel_name or "").strip()
    try:
        limit = max(int(limit), 1)
    except (TypeError, ValueError):
        limit = 4
    cache_key = _youtube_channel_group_feed_cache_key(channel_name or channel_id, channel_id or channel_name, limit)
    with RUNTIME_CACHE_LOCK:
        cached_feeds = RUNTIME_CACHE.setdefault("youtube_channel_group_feed_videos", {})
        cached = cached_feeds.get(cache_key)
        if isinstance(cached, list):
            return json.loads(json.dumps(cached))

    uploads_playlist_id = _youtube_channel_upload_playlist_id(channel_id)
    if not uploads_playlist_id:
        return []

    raw_videos = fetch_playlist_videos_from_youtube(uploads_playlist_id, max_total=limit)
    videos = []
    for index, video in enumerate(raw_videos or []):
        if not isinstance(video, dict):
            continue
        videos.append({
            "title": str(video.get("title", "") or "").strip(),
            "video_id": str(video.get("video_id", "") or "").strip(),
            "playlist_id": str(video.get("playlist_id", "") or uploads_playlist_id).strip(),
            "playlist_item_id": str(video.get("playlist_item_id", "") or "").strip(),
            "playlist_name": str(video.get("playlist_name", "") or "Uploads").strip() or "Uploads",
            "channel_name": str(video.get("channel_name", "") or channel_name or "Unknown Channel").strip() or "Unknown Channel",
            "channel_id": channel_id,
            "thumb": str(video.get("thumb", "") or "").strip(),
            "duration": str(video.get("duration", "") or "").strip(),
            "published_at": str(video.get("published_at", "") or "").strip(),
            "url": str(video.get("url", "") or "").strip() or f"https://www.youtube.com/watch?v={str(video.get('video_id', '') or '').strip()}&list={uploads_playlist_id}",
            "source_index": index,
        })
    videos = apply_cached_durations(videos)
    with RUNTIME_CACHE_LOCK:
        RUNTIME_CACHE.setdefault("youtube_channel_group_feed_videos", {})[cache_key] = json.loads(json.dumps(videos))
    return json.loads(json.dumps(videos))


def build_youtube_section_feed_context(section_name, admin_data=None, limit=24, force_refresh=False):
    admin_data = admin_data if isinstance(admin_data, dict) else load_admin_data()
    section_key = _youtube_section_feed_cache_key(section_name)

    def has_pockettube_feed_content(payload):
        if not isinstance(payload, dict):
            return False
        feed_count = int(payload.get("feed_count", payload.get("video_count", 0)) or 0)
        channel_count = int(payload.get("channel_count", 0) or 0)
        page_items = payload.get("feed_page_items") or payload.get("feed_items") or payload.get("feed_preview") or []
        return bool(feed_count or channel_count or page_items)

    with RUNTIME_CACHE_LOCK:
        cached_feeds = RUNTIME_CACHE.setdefault("youtube_section_feeds", {})
        cached = cached_feeds.get(section_key)
        if isinstance(cached, dict) and not force_refresh:
            if has_pockettube_feed_content(cached):
                return _apply_pockettube_feed_pagination(cached)
            app.logger.info("[youtube-group-cache] key=%s source=runtime-empty-rebuild", section_key)
            cached_feeds.pop(section_key, None)

    persisted_entry, persisted_stale = get_persisted_youtube_section_feed_entry(section_key, force_refresh=force_refresh, allow_stale=True)
    if persisted_entry is not None and not force_refresh:
        if has_pockettube_feed_content(persisted_entry):
            with RUNTIME_CACHE_LOCK:
                RUNTIME_CACHE.setdefault("youtube_section_feeds", {})[section_key] = json.loads(json.dumps(persisted_entry))
            if persisted_stale and not is_youtube_cache_refresh_pending(f"youtube_section_feeds:{section_key}"):
                schedule_youtube_cache_refresh(
                    f"youtube_section_feeds:{section_key}",
                    lambda: build_youtube_section_feed_context(section_name, admin_data=admin_data, limit=limit, force_refresh=True),
                )
            return _apply_pockettube_feed_pagination(persisted_entry)
        app.logger.info("[youtube-group-cache] key=%s source=disk-empty-rebuild", section_key)
        persisted_entry = None
        persisted_stale = True

    section_profile = youtube_section_blueprint(section_name)
    pockettube_context = _pockettube_section_membership_context(section_name, admin_data=admin_data)
    channels = list(pockettube_context.get("channels", []) or [])
    if not channels:
        empty_context = {
            "name": section_name,
            "slug": section_slug(section_name),
            "section_kind": section_profile.get("section_kind", ""),
            "section_scope": section_profile.get("section_scope", ""),
            "channel_group_key": section_profile.get("channel_group_key", ""),
            "channel_group_label": section_profile.get("channel_group_label", ""),
            "section_order": section_profile.get("section_order", 999),
            "playlist_count": 0,
            "video_count": 0,
            "channel_count": 0,
            "feed_count": 0,
            "feed_mode": "pockettube_group",
            "feed_items": [],
            "feed_preview": [],
            "feed_all": [],
            "playlists": [],
            "channels": [],
            "group_name": pockettube_context.get("group_name", ""),
            "group_key": pockettube_context.get("group_key", ""),
            "source_name": pockettube_context.get("source_name", "PocketTube"),
            "fingerprint": pockettube_context.get("fingerprint", ""),
            "matched_sections": pockettube_context.get("matched_sections", 0),
            "pockettube_group_visible": False,
        }
        with RUNTIME_CACHE_LOCK:
            RUNTIME_CACHE.setdefault("youtube_section_feeds", {})[section_key] = json.loads(json.dumps(empty_context))
        set_persisted_youtube_section_feed_entry(section_key, empty_context)
        return _apply_pockettube_feed_pagination(empty_context)

    section_profile.update({
        "section_kind": "curated",
        "section_scope": "group",
        "channel_group_key": pockettube_context.get("group_key", section_profile.get("channel_group_key", "")),
        "channel_group_label": pockettube_context.get("group_name", section_profile.get("channel_group_label", "")),
    })

    per_channel_limit = 1

    feed_items = []
    channel_groups = []
    seen_video_ids = set()
    feed_order_index = 0
    for record in channels:
        channel_id = str(record.get("channel_id", "") or "").strip()
        if not channel_id:
            continue
        recent_videos = fetch_youtube_channel_group_feed_videos(
            channel_id,
            channel_name=record.get("channel_name", ""),
            limit=per_channel_limit,
        )
        channel_videos = []
        for video in recent_videos:
            if not isinstance(video, dict):
                continue
            summary = build_youtube_channel_video_summary(video)
            if summary.get("video_id"):
                channel_videos.append(summary)
            video_id = summary.get("video_id", "")
            if not video_id or video_id in seen_video_ids:
                continue
            seen_video_ids.add(video_id)
            feed_items.append({
                **video,
                "section_name": section_name,
                "section_kind": section_profile.get("section_kind", ""),
                "section_scope": section_profile.get("section_scope", ""),
                "channel_group_key": section_profile.get("channel_group_key", ""),
                "channel_group_label": section_profile.get("channel_group_label", ""),
                "group_name": pockettube_context.get("group_name", ""),
                "group_key": pockettube_context.get("group_key", ""),
                "channel_key": record.get("channel_key", ""),
                "channel_id": channel_id,
                "tier": record.get("tier", ""),
                "notes": record.get("notes", ""),
                "source": record.get("source", "pockettube"),
                "feed_order_index": feed_order_index,
            })
            feed_order_index += 1
        channel_groups.append({
            "channel_key": record.get("channel_key", ""),
            "channel_name": record.get("channel_name", "") or "Unknown Channel",
            "channel_id": channel_id,
            "group_name": pockettube_context.get("group_name", ""),
            "group_key": pockettube_context.get("group_key", ""),
            "tier": record.get("tier", ""),
            "notes": record.get("notes", ""),
            "video_count": len(channel_videos),
            "latest_video": channel_videos[0] if channel_videos else {},
            "videos": channel_videos,
            "source": record.get("source", "pockettube"),
        })

    feed_items.sort(key=_pockettube_feed_sort_key)
    feed_items = _enrich_pockettube_feed_videos(feed_items, fetch_missing=True)
    feed_context = {
        "name": pockettube_context.get("section_name", section_name) or section_name,
        "slug": section_slug(section_name),
        "section_kind": section_profile.get("section_kind", ""),
        "section_scope": section_profile.get("section_scope", ""),
        "channel_group_key": section_profile.get("channel_group_key", ""),
        "channel_group_label": section_profile.get("channel_group_label", ""),
        "section_order": section_profile.get("section_order", 999),
        "playlist_count": 0,
        "video_count": len(feed_items),
        "channel_count": len(channel_groups),
        "feed_count": len(feed_items),
        "feed_mode": "pockettube_group",
        "feed_items": feed_items,
        "feed_preview": feed_items,
        "feed_all": feed_items,
        "playlists": [],
        "channels": channel_groups,
        "group_name": pockettube_context.get("group_name", ""),
        "group_key": pockettube_context.get("group_key", ""),
        "source_name": pockettube_context.get("source_name", "PocketTube"),
        "fingerprint": pockettube_context.get("fingerprint", ""),
        "matched_sections": pockettube_context.get("matched_sections", 0),
        "pockettube_group_visible": True,
    }
    with RUNTIME_CACHE_LOCK:
        RUNTIME_CACHE.setdefault("youtube_section_feeds", {})[section_key] = json.loads(json.dumps(feed_context))
    set_persisted_youtube_section_feed_entry(section_key, feed_context)
    return _apply_pockettube_feed_pagination(feed_context)


def _sanitize_admin_playlist(item):
    if not isinstance(item, dict):
        return None
    name = str(item.get("name", "")).strip()
    raw_url = str(item.get("url", "")).strip()
    raw_id = str(item.get("id", "")).strip()
    if not name:
        return None
    try:
        playlist_id, canonical_url = parse_playlist_input(raw_url or raw_id)
    except ValueError:
        if raw_id and re.fullmatch(r"[A-Za-z0-9_-]{10,}", raw_id):
            playlist_id = raw_id
            canonical_url = f"https://www.youtube.com/playlist?list={playlist_id}"
        else:
            return None
    return {"name": name, "url": canonical_url, "id": playlist_id}


def load_admin_data():
    raw = load_json_file(ADMIN_DATA_PATH, _empty_admin_data())
    if not isinstance(raw, dict):
        raw = _empty_admin_data()
    sections = []
    if isinstance(raw, dict) and isinstance(raw.get("sections"), list):
        source_sections = raw.get("sections", [])
    elif isinstance(raw, dict) and isinstance(raw.get("youtube_sections"), dict):
        source_sections = [
            {"name": canonical_section_name(section_name), "playlists": playlists}
            for section_name, playlists in raw.get("youtube_sections", {}).items()
        ]
    else:
        source_sections = []
    seen_sections = set()
    for section in source_sections:
        if not isinstance(section, dict):
            continue
        section_name = canonical_section_name(section.get("name", ""))
        if not section_name:
            continue
        normalized_section = normalize_section_name(section_name)
        if normalized_section in seen_sections:
            continue
        seen_sections.add(normalized_section)
        playlists = []
        seen_playlist_ids = set()
        for playlist in section.get("playlists", []):
            sanitized = _sanitize_admin_playlist(playlist)
            if not sanitized or sanitized["id"] in seen_playlist_ids:
                continue
            seen_playlist_ids.add(sanitized["id"])
            playlists.append(sanitized)
        section_record = dict(section)
        section_record["name"] = section_name
        section_record["playlists"] = playlists
        sections.append(normalize_youtube_section_record(section_record))
    data = {key: value for key, value in raw.items() if key not in {"sections", "youtube_sections"}}
    data["sections"] = sections
    data["youtube_channel_curation"] = normalize_youtube_channel_curation(raw.get("youtube_channel_curation", {}))
    data["youtube_pockettube_imports"] = raw.get("youtube_pockettube_imports", {"latest": {}, "imports": []})
    if data != raw:
        save_json_file(ADMIN_DATA_PATH, data)
    return data


def save_admin_data(data):
    payload = dict(data) if isinstance(data, dict) else _empty_admin_data()
    payload.setdefault("sections", [])
    payload["youtube_channel_curation"] = normalize_youtube_channel_curation(payload.get("youtube_channel_curation", {}))
    pockettube_imports = payload.get("youtube_pockettube_imports", {"latest": {}, "imports": []})
    if not isinstance(pockettube_imports, dict):
        pockettube_imports = {"latest": {}, "imports": []}
    pockettube_imports.setdefault("latest", {})
    pockettube_imports.setdefault("imports", [])
    payload["youtube_pockettube_imports"] = pockettube_imports
    save_json_file(ADMIN_DATA_PATH, payload)


def load_legacy_playlist_data():
    data = load_json_file(PLAYLISTS_PATH, {})
    return data if isinstance(data, dict) else {}


def save_legacy_playlist_data(data):
    save_json_file(PLAYLISTS_PATH, data)


def section_slug(value):
    return slugify(normalize_section_name(value or "section"))


def section_icon(section_name):
    normalized = normalize_section_name(section_name)
    if normalized == normalize_section_name("PocketTube"):
        return "fa-solid fa-layer-group"
    if normalized == "german":
        return "fa-solid fa-language"
    if normalized == "chess":
        return "fa-solid fa-chess-knight"
    if normalized == "youtubewatchlater":
        return "fa-solid fa-clock"
    if normalized == "library":
        return "fa-brands fa-youtube"
    if "history" in normalized:
        return "fa-solid fa-clock-rotate-left"
    return "fa-brands fa-youtube"


def build_legacy_sections():
    data = load_legacy_playlist_data()
    sections = []
    for section_key, content in data.items():
        if str(section_key).startswith("_") or section_key == "sections":
            continue
        section_name = canonical_section_name(section_key)
        playlists = []
        if isinstance(content, list):
            for item in content:
                sanitized = _sanitize_admin_playlist(item)
                if not sanitized:
                    continue
                playlists.append({
                    **sanitized,
                    "source": "legacy",
                    "source_section_key": section_key,
                    "source_category_key": "",
                })
        elif isinstance(content, dict):
            for category_key, items in content.items():
                if not isinstance(items, list):
                    continue
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    sanitized = _sanitize_admin_playlist({
                        "name": f"{category_key}: {item.get('name', '')}",
                        "url": item.get("url", ""),
                        "id": item.get("id", ""),
                    })
                    if not sanitized:
                        continue
                    playlists.append({
                        **sanitized,
                        "source": "legacy",
                        "source_section_key": section_key,
                        "source_category_key": category_key,
                    })
        sections.append(normalize_youtube_section_record({
            "name": section_name,
            "slug": section_slug(section_name),
            "playlists": playlists,
            "source": "legacy",
            "source_section_key": section_key,
        }))
    return sections


def build_admin_sections():
    data = load_admin_data()
    sections = []
    for section in data.get("sections", []):
        section_name = canonical_section_name(section.get("name", ""))
        if not section_name:
            continue
        playlists = []
        for item in section.get("playlists", []):
            sanitized = _sanitize_admin_playlist(item)
            if not sanitized:
                continue
            playlists.append({
                **sanitized,
                "source": "admin",
                "source_section_key": section_name,
                "source_category_key": "",
            })
        sections.append(normalize_youtube_section_record({
            "name": section_name,
            "slug": section_slug(section_name),
            "playlists": playlists,
            "source": "admin",
            "source_section_key": section_name,
        }))
    return sections


def build_combined_sections():
    merged = {}
    # Prefer current admin storage when the same playlist exists in both sources.
    # Legacy entries remain available only when there is no current admin copy.
    for collection in (build_admin_sections(), build_legacy_sections()):
        for section in collection:
            key = normalize_section_name(section.get("name", ""))
            if not key:
                continue
            current = merged.setdefault(key, {
                "name": section.get("name", ""),
                "slug": section_slug(section.get("name", "")),
                "playlists": [],
                "section_kind": section.get("section_kind", ""),
                "section_scope": section.get("section_scope", ""),
                "channel_group_key": section.get("channel_group_key", ""),
                "channel_group_label": section.get("channel_group_label", ""),
                "section_order": section.get("section_order", 999),
                "channel_groups": list(section.get("channel_groups", [])) if isinstance(section.get("channel_groups", []), list) else [],
            })
            current.setdefault("section_kind", section.get("section_kind", ""))
            current.setdefault("section_scope", section.get("section_scope", ""))
            current.setdefault("channel_group_key", section.get("channel_group_key", ""))
            current.setdefault("channel_group_label", section.get("channel_group_label", ""))
            current.setdefault("section_order", section.get("section_order", 999))
            if not isinstance(current.get("channel_groups"), list):
                current["channel_groups"] = []
            seen_ids = {item.get("id", "") for item in current["playlists"]}
            for playlist in section.get("playlists", []):
                playlist_id = playlist.get("id", "")
                if playlist_id and playlist_id in seen_ids:
                    continue
                if playlist_id:
                    seen_ids.add(playlist_id)
                current["playlists"].append(playlist)
    latest_import, imported_sections = _pockettube_latest_import_snapshot()
    for section in imported_sections:
        if not isinstance(section, dict):
            continue
        section_name = _pockettube_display_name(section.get("section_name", "") or section.get("group_name", "") or "")
        if not section_name:
            continue
        key = normalize_section_name(section_name)
        if key == normalize_section_name("German"):
            continue
        existing = key in merged
        current = merged.setdefault(key, {
            "name": section_name,
            "slug": section_slug(section_name),
            "playlists": [],
            "section_kind": "curated",
            "section_scope": "group",
            "channel_group_key": normalize_section_name(section.get("group_key", "") or section_name),
            "channel_group_label": section.get("group_name", "") or section_name,
            "section_order": 1500,
            "channel_groups": [],
            "source": "pockettube",
            "pockettube_channel_count": int(section.get("channel_count", 0) or 0),
        })
        if existing:
            current.setdefault("pockettube_channel_count", int(section.get("channel_count", 0) or 0))
            if not current.get("channel_group_key"):
                current["channel_group_key"] = normalize_section_name(section.get("group_key", "") or section_name)
            if not current.get("channel_group_label"):
                current["channel_group_label"] = section.get("group_name", "") or section_name
            if not current.get("section_scope"):
                current["section_scope"] = "group"
        else:
            current.setdefault("source", "pockettube")
            current.setdefault("pockettube_channel_count", int(section.get("channel_count", 0) or 0))
            current.setdefault("section_kind", "curated")
            current.setdefault("section_scope", "group")
            current.setdefault("channel_group_key", normalize_section_name(section.get("group_key", "") or section_name))
            current.setdefault("channel_group_label", section.get("group_name", "") or section_name)
            current.setdefault("section_order", 1500)
        if not isinstance(current.get("channel_groups"), list):
            current["channel_groups"] = []
    sections = list(merged.values())
    sections.sort(key=lambda item: (int(item.get("section_order", 999) or 999), item.get("name", "").lower()))
    for section in sections:
        section["playlists"].sort(key=lambda item: item.get("name", "").lower())
    return sections


def find_admin_section(admin_data, section_name):
    wanted = normalize_section_name(section_name)
    for section in admin_data.get("sections", []):
        if normalize_section_name(section.get("name", "")) == wanted:
            return section
    return None


def get_or_create_admin_section(admin_data, section_name):
    section = find_admin_section(admin_data, section_name)
    if section:
        return section
    section = normalize_youtube_section_record({"name": canonical_section_name(section_name), "playlists": []})
    admin_data.setdefault("sections", []).append(section)
    admin_data["sections"].sort(key=lambda item: item.get("name", "").lower())
    return section


def find_youtube_curated_channel(admin_data, channel_key="", tier="", section_name="", group_name=""):
    curation = normalize_youtube_channel_curation((admin_data or {}).get("youtube_channel_curation", {}))
    wanted_key = normalize_section_name(channel_key)
    wanted_tier = str(tier or "").strip().lower()
    wanted_section = normalize_section_name(section_name)
    wanted_group = normalize_section_name(group_name)
    for record in curation.get("channels", []):
        if wanted_key and record.get("channel_key", "") != wanted_key:
            continue
        if wanted_tier and record.get("tier", "") != wanted_tier:
            continue
        if wanted_section and record.get("section_key", "") != wanted_section:
            continue
        if wanted_group and record.get("group_key", "") != wanted_group:
            continue
        return record
    return None


def ensure_unique_playlist(section, playlist_id, skip_id=""):
    for playlist in section.get("playlists", []):
        existing_id = playlist.get("id", "")
        if existing_id == skip_id:
            continue
        if existing_id == playlist_id:
            raise ValueError("This playlist already exists in the selected category.")


def find_legacy_section_key(legacy_data, section_name):
    wanted = normalize_section_name(section_name)
    for key in legacy_data:
        if str(key).startswith("_") or key == "sections":
            continue
        if normalize_section_name(canonical_section_name(key)) == wanted:
            return key
    return None


def remove_legacy_playlist(legacy_data, section_key, category_key, playlist_id, playlist_name):
    content = legacy_data.get(section_key)
    if isinstance(content, list):
        legacy_data[section_key] = [
            item for item in content
            if not (
                isinstance(item, dict)
                and (
                    extract_playlist_id(item.get("url", "")) == playlist_id
                    or item.get("id", "") == playlist_id
                    or item.get("name", "") == playlist_name
                )
            )
        ]
        return
    if isinstance(content, dict) and category_key in content:
        content[category_key] = [
            item for item in content.get(category_key, [])
            if not (
                isinstance(item, dict)
                and (
                    extract_playlist_id(item.get("url", "")) == playlist_id
                    or item.get("id", "") == playlist_id
                    or f"{category_key}: {item.get('name', '')}" == playlist_name
                )
            )
        ]


def get_playlist_thumbnail_url(playlist_id):
    if not playlist_id:
        return ""
    cache_data = load_cache_data()
    cached_videos = cache_data.get("youtube_playlists", {}).get(playlist_id, {}).get("data", [])
    if isinstance(cached_videos, list):
        for video in cached_videos:
            if isinstance(video, dict) and video.get("thumb"):
                return video.get("thumb", "")
    videos = get_all_playlist_videos(playlist_id, max_total=1)
    for video in videos:
        if isinstance(video, dict) and video.get("thumb"):
            return video.get("thumb", "")
    return ""


def build_admin_table_rows(admin_data):
    rows = []
    for section in build_combined_sections():
        for playlist in section.get("playlists", []):
            rows.append({
                "section": section.get("name", ""),
                "section_kind": section.get("section_kind", ""),
                "section_scope": section.get("section_scope", ""),
                "channel_group_key": section.get("channel_group_key", ""),
                "channel_group_label": section.get("channel_group_label", ""),
                "name": playlist.get("name", ""),
                "url": playlist.get("url", ""),
                "id": playlist.get("id", ""),
                "thumbnail_url": get_playlist_thumbnail_url(playlist.get("id", "")),
                "source": playlist.get("source", ""),
                "source_section_key": playlist.get("source_section_key", ""),
                "source_category_key": playlist.get("source_category_key", ""),
            })
    rows.sort(key=lambda item: (item["section"].lower(), item["name"].lower()))
    return rows


def handle_admin_action(admin_data, form):
    action = (form.get("action") or "").strip()
    legacy_data = load_legacy_playlist_data()
    if action == "add_youtube_channel_curation":
        channel_name = str(form.get("channel_name", "") or "").strip()
        if not channel_name:
            raise ValueError("Channel name is required.")
        tier = str(form.get("tier", "") or "favorite").strip().lower()
        if tier not in {"best", "favorite"}:
            tier = "favorite"
        section_name = canonical_section_name(form.get("section_name", ""))
        group_name = str(form.get("group_name", "") or "").strip() or (section_name or tier.title())
        record = _sanitize_youtube_curated_channel({
            "channel_name": channel_name,
            "tier": tier,
            "section_name": section_name,
            "group_name": group_name,
            "notes": form.get("notes", ""),
        })
        if not record:
            raise ValueError("Channel name is required.")
        curation = normalize_youtube_channel_curation(admin_data.get("youtube_channel_curation", {}))
        records = list(curation.get("channels", []))
        record_key = (
            record.get("channel_key", ""),
            record.get("tier", ""),
            record.get("section_key", ""),
            record.get("group_key", ""),
        )
        records = [
            item for item in records
            if (
                item.get("channel_key", ""),
                item.get("tier", ""),
                item.get("section_key", ""),
                item.get("group_key", ""),
            ) != record_key
        ]
        records.append(record)
        records.sort(
            key=lambda item: (
                _youtube_channel_tier_rank(item.get("tier", "")),
                item.get("section_name", "").lower(),
                item.get("group_name", "").lower(),
                item.get("channel_name", "").lower(),
            )
        )
        admin_data["youtube_channel_curation"] = {"channels": records}
        save_admin_data(admin_data)
        return f'Curated channel "{channel_name}" saved.'

    if action == "delete_youtube_channel_curation":
        channel_key = str(form.get("channel_key", "") or "").strip()
        tier = str(form.get("tier", "") or "").strip().lower()
        section_name = canonical_section_name(form.get("section_name", ""))
        group_name = str(form.get("group_name", "") or "").strip()
        record = find_youtube_curated_channel(admin_data, channel_key=channel_key, tier=tier, section_name=section_name, group_name=group_name)
        if not record:
            raise ValueError("Curated channel not found.")
        curation = normalize_youtube_channel_curation(admin_data.get("youtube_channel_curation", {}))
        admin_data["youtube_channel_curation"] = {
            "channels": [
                item for item in curation.get("channels", [])
                if not (
                    item.get("channel_key", "") == record.get("channel_key", "")
                    and item.get("tier", "") == record.get("tier", "")
                    and item.get("section_key", "") == record.get("section_key", "")
                    and item.get("group_key", "") == record.get("group_key", "")
                )
            ]
        }
        save_admin_data(admin_data)
        return f'Curated channel "{record.get("channel_name", "")}" removed.'

    if action == "add_section":
        name = canonical_section_name(form.get("section_name", ""))
        if not name:
            raise ValueError("Section name is required.")
        if find_admin_section(admin_data, name) or find_legacy_section_key(legacy_data, name):
            raise ValueError("A section with this name already exists.")
        admin_data.setdefault("sections", []).append(normalize_youtube_section_record({"name": name, "playlists": []}))
        admin_data["sections"].sort(key=lambda item: item.get("name", "").lower())
        save_admin_data(admin_data)
        return f'Section "{name}" added.'

    if action == "rename_section":
        current_name = form.get("current_name", "")
        new_name = canonical_section_name(form.get("new_name", ""))
        section = find_admin_section(admin_data, current_name)
        legacy_key = find_legacy_section_key(legacy_data, current_name)
        if not section:
            section = None
        if not section and not legacy_key:
            raise ValueError("Section not found.")
        if not new_name:
            raise ValueError("New section name is required.")
        existing = find_admin_section(admin_data, new_name)
        existing_legacy = find_legacy_section_key(legacy_data, new_name)
        if (existing and existing is not section) or (existing_legacy and existing_legacy != legacy_key):
            raise ValueError("Another section already uses that name.")
        if section:
            section["name"] = new_name
            section.update(normalize_youtube_section_record(section))
            admin_data["sections"].sort(key=lambda item: item.get("name", "").lower())
            save_admin_data(admin_data)
        if legacy_key:
            legacy_data[new_name] = legacy_data.pop(legacy_key)
            save_legacy_playlist_data(legacy_data)
        return f'Section renamed to "{new_name}".'

    if action == "delete_section":
        section_name = form.get("section_name", "")
        section = next((
            item for item in build_combined_sections()
            if normalize_section_name(item.get("name", "")) == normalize_section_name(section_name)
        ), None)
        if not section:
            raise ValueError("Section not found.")
        if section.get("playlists"):
            raise ValueError("Only empty sections can be deleted.")
        admin_section = find_admin_section(admin_data, section_name)
        if admin_section:
            admin_data["sections"] = [item for item in admin_data.get("sections", []) if item is not admin_section]
            save_admin_data(admin_data)
        legacy_key = find_legacy_section_key(legacy_data, section_name)
        if legacy_key:
            legacy_data.pop(legacy_key, None)
            save_legacy_playlist_data(legacy_data)
        return f'Section "{section_name}" deleted.'

    if action == "add_playlist":
        section_name = canonical_section_name(form.get("section_name", ""))
        playlist_name = (form.get("playlist_name", "") or "").strip()
        playlist_id, canonical_url = parse_playlist_input(form.get("playlist_url", ""))
        if not section_name:
            raise ValueError("Choose a valid section before adding a playlist.")
        section = get_or_create_admin_section(admin_data, section_name)
        if not playlist_name:
            raise ValueError("Playlist name is required.")
        ensure_unique_playlist(section, playlist_id)
        section.setdefault("playlists", []).append({"name": playlist_name, "url": canonical_url, "id": playlist_id})
        section["playlists"].sort(key=lambda item: item.get("name", "").lower())
        save_admin_data(admin_data)
        return f'Playlist "{playlist_name}" added to "{section_name}".'

    if action == "update_playlist":
        source_name = form.get("source_section", "")
        target_name = canonical_section_name(form.get("target_section", ""))
        source_section = find_admin_section(admin_data, source_name)
        target_section = get_or_create_admin_section(admin_data, target_name) if target_name else None
        playlist_id = (form.get("playlist_id", "") or "").strip()
        playlist_name = (form.get("playlist_name", "") or "").strip()
        source_type = (form.get("playlist_source", "") or "admin").strip()
        if not target_section:
            raise ValueError("Target section was not found.")
        if not playlist_name:
            raise ValueError("Playlist name is required.")
        next_id, canonical_url = parse_playlist_input(form.get("playlist_url", ""))
        if source_type == "legacy":
            ensure_unique_playlist(target_section, next_id)
            remove_legacy_playlist(
                legacy_data,
                form.get("source_section_key", ""),
                form.get("source_category_key", ""),
                playlist_id,
                form.get("original_playlist_name", "")
            )
            save_legacy_playlist_data(legacy_data)
            target_section.setdefault("playlists", []).append({"name": playlist_name, "url": canonical_url, "id": next_id})
            target_section["playlists"].sort(key=lambda item: item.get("name", "").lower())
            save_admin_data(admin_data)
        else:
            if not source_section:
                raise ValueError("Source section was not found.")
            playlist = next((item for item in source_section.get("playlists", []) if item.get("id") == playlist_id), None)
            if not playlist:
                raise ValueError("Playlist not found.")
            ensure_unique_playlist(target_section, next_id, skip_id=playlist_id if source_section is target_section else "")
            updated = {"name": playlist_name, "url": canonical_url, "id": next_id}
            if source_section is target_section:
                playlist.update(updated)
                source_section["playlists"].sort(key=lambda item: item.get("name", "").lower())
            else:
                source_section["playlists"] = [item for item in source_section.get("playlists", []) if item.get("id") != playlist_id]
                target_section.setdefault("playlists", []).append(updated)
                target_section["playlists"].sort(key=lambda item: item.get("name", "").lower())
            save_admin_data(admin_data)
        return f'Playlist "{playlist_name}" updated.'

    if action == "delete_playlist":
        source_type = (form.get("playlist_source", "") or "admin").strip()
        playlist_id = (form.get("playlist_id", "") or "").strip()
        playlist_name = form.get("original_playlist_name", "") or form.get("playlist_name", "")
        # Prefer deleting from the current admin storage when a matching row exists there.
        section = find_admin_section(admin_data, form.get("section_name", ""))
        if section:
            playlist = next((item for item in section.get("playlists", []) if item.get("id") == playlist_id), None)
            if playlist:
                section["playlists"] = [item for item in section.get("playlists", []) if item.get("id") != playlist_id]
                save_admin_data(admin_data)
                return f'Playlist "{playlist.get("name", "")}" deleted.'
        if source_type == "legacy":
            remove_legacy_playlist(
                legacy_data,
                form.get("source_section_key", ""),
                form.get("source_category_key", ""),
                playlist_id,
                playlist_name
            )
            save_legacy_playlist_data(legacy_data)
            return f'Playlist "{playlist_name}" deleted.'
        raise ValueError("Playlist not found.")

    if action == "reading_source_add":
        name = str(form.get("name", "") or "").strip()
        url = normalize_reading_url(form.get("url", ""))
        topic = str(form.get("topic", "") or "").strip()
        category = normalize_reading_category(form.get("category", ""))
        active = str(form.get("active", "") or "").strip().lower() in {"1", "true", "yes", "on"}
        source_id = str(form.get("source_id", "") or "").strip()
        if not name:
            raise ValueError("Source name is required.")
        _, message = upsert_reading_source_record(name=name, url=url, topic=topic, category=category, active=active, source_id=source_id)
        return message

    if action == "reading_source_toggle":
        source_id = str(form.get("source_id", "") or "").strip()
        toggled = toggle_reading_source_active(source_id)
        if not toggled:
            raise ValueError("Source not found.")
        return f'Updated source "{toggled.get("name", "")}".'

    if action == "reading_source_remove":
        source_id = str(form.get("source_id", "") or "").strip()
        source_name = next((str(source.get("name", "") or "").strip() for source in load_reading_data().get("sources", []) if isinstance(source, dict) and source.get("id") == source_id), "source")
        if not remove_reading_source(source_id):
            raise ValueError("Source not found.")
        return f'Removed source "{source_name}".'

    if action == "reading_source_sync":
        source_id = str(form.get("source_id", "") or "").strip()
        result = sync_reading_sources(source_id=source_id)
        imported_total = int(result.get("imported_total", 0) or 0)
        active_source_count = int(result.get("active_source_count", 0) or 0)
        if source_id:
            source_name = next((str(source.get("name", "") or "").strip() for source in load_reading_data().get("sources", []) if isinstance(source, dict) and source.get("id") == source_id), "source")
            if not active_source_count:
                return f'Synced "{source_name}": no active source URL available.'
            return f'Synced "{source_name}": {imported_total} new item(s) imported.'
        return result.get("last_sync_message", f"Synced {imported_total} new item(s).")

    if action == "reading_sync_all":
        result = sync_reading_sources()
        return result.get("last_sync_message", "Synced reading sources.")

    raise ValueError("Unknown admin action.")


def get_section_route(section_name):
    normalized = normalize_section_name(section_name)
    routes = {
        normalize_section_name("German"): "german",
        normalize_section_name("Chess"): "chess",
        normalize_section_name("Library"): "library_yt",
        normalize_section_name("YouTube Watch Later"): "watchlater",
    }
    endpoint = routes.get(normalized)
    return url_for(endpoint) if endpoint else url_for("section_page", section_slug=section_slug(section_name))


def get_navigation_items():
    core_items = [
        {"href": url_for("home"), "label": "Home", "short_label": "Home", "icon": "fa-solid fa-house", "active_paths": [url_for("home")]},
        {"href": url_for("library"), "label": "Movies", "short_label": "Movies", "icon": "fa-solid fa-film", "active_paths": [url_for("library")]},
        {"href": url_for("books_archive"), "label": "Books", "short_label": "Books", "icon": "fa-solid fa-book", "active_paths": [url_for("books_archive")]},
        {"href": url_for("reading"), "label": "Reading", "short_label": "Reading", "icon": "fa-solid fa-book-open-reader", "active_paths": [url_for("reading")]},
    ]
    pockettube_item = {
        "href": url_for("pockettube_groups"),
        "label": "PocketTube",
        "short_label": "PocketTube",
        "icon": "fa-solid fa-layer-group",
        "active_paths": [url_for("pockettube_groups")],
        "slug": section_slug("PocketTube"),
    }
    content_items = []
    for section in build_combined_sections():
        if str(section.get("source", "") or "").strip().lower() == "pockettube":
            continue
        href = get_section_route(section.get("name", ""))
        slug = section_slug(section.get("name", ""))
        active_paths = [href]
        if normalize_section_name(section.get("name", "")) == normalize_section_name("German"):
            active_paths.append(url_for("german"))
        elif normalize_section_name(section.get("name", "")) == normalize_section_name("Chess"):
            active_paths.append(url_for("chess"))
        elif normalize_section_name(section.get("name", "")) == normalize_section_name("Library"):
            active_paths.append(url_for("library_yt"))
        elif normalize_section_name(section.get("name", "")) == normalize_section_name("YouTube Watch Later"):
            active_paths.append(url_for("watchlater"))
        content_items.append({
            "href": href,
            "label": section.get("name", ""),
            "short_label": section.get("name", ""),
            "icon": section_icon(section.get("name", "")),
            "active_paths": active_paths,
            "slug": slug,
        })
    tail_items = [
        {"href": url_for("history"), "label": "History", "short_label": "History", "icon": "fa-solid fa-clock-rotate-left", "active_paths": [url_for("history")]},
        {"href": url_for("movies_review"), "label": "Review Queue", "short_label": "Review", "icon": "fa-solid fa-triangle-exclamation", "active_paths": [url_for("movies_review")]},
        {"href": url_for("admin"), "label": "Admin", "short_label": "Admin", "icon": "fa-solid fa-sliders", "active_paths": [url_for("admin")]},
    ]
    return core_items + [pockettube_item] + content_items + tail_items


def build_navigation_context():
    nav_items = get_navigation_items()
    top_priority_order = [
        normalize_section_name("Home"),
        normalize_section_name("Movies"),
        normalize_section_name("Books"),
        normalize_section_name("Reading"),
        normalize_section_name("PocketTube"),
        normalize_section_name("German"),
        normalize_section_name("Chess"),
        normalize_section_name("YouTube Watch Later"),
    ]
    top_priority = set(top_priority_order)
    top_nav_lookup = {}
    sidebar_dynamic_items = []
    for item in nav_items:
        normalized_label = normalize_section_name(item.get("label", ""))
        if normalized_label in top_priority:
            top_nav_lookup[normalized_label] = item
        elif normalized_label not in (normalize_section_name("Library"),):
            sidebar_dynamic_items.append(item)
        else:
            sidebar_dynamic_items.insert(0, item)
    top_nav_items = [top_nav_lookup[key] for key in top_priority_order if key in top_nav_lookup]
    return {
        "nav_items": nav_items,
        "top_nav_items": top_nav_items,
        "sidebar_dynamic_items": sidebar_dynamic_items,
        "dragon_auth_enabled": dragon_auth_enabled(),
        "dragon_site_protection_enabled": dragon_site_protection_enabled(),
        "dragon_authenticated": dragon_is_authenticated(),
        "ai_default_mode": "cinematic",
        "ai_page_context": "general",
    }


def ai_context_for_section(section_name):
    normalized = normalize_section_name(section_name)
    if normalized == normalize_section_name("German"):
        return {"mode": "german", "page_context": "german"}
    if normalized in {
        normalize_section_name("Chess"),
        normalize_section_name("Library"),
        normalize_section_name("YouTube Watch Later"),
    }:
        return {"mode": "study", "page_context": "study"}
    return {"mode": "study", "page_context": "study"}


def ai_context_for_video(entry_type, section_name=""):
    if entry_type == "film":
        return {"mode": "cinematic", "page_context": "movie"}
    return ai_context_for_section(section_name)


@app.context_processor
def inject_navigation_items():
    return build_navigation_context()


def warm_runtime_cache(force_refresh=False):
    if force_refresh:
        refresh_film_cache_from_source()
        return

    primed_keys = []
    scheduled_refresh = False

    cached_all, cached_all_stale = get_persisted_film_cache_entry("all", force_refresh=False, allow_stale=True)
    if cached_all is not None:
        with RUNTIME_CACHE_LOCK:
            RUNTIME_CACHE["films"] = clone_film_rows(cached_all)
        primed_keys.append("all")
        if cached_all_stale and not is_movie_cache_refresh_pending("films"):
            scheduled_refresh = schedule_movie_cache_refresh("films", refresh_film_cache_from_source) or scheduled_refresh

    cached_union, cached_union_stale = get_persisted_film_cache_entry("want_to_union", force_refresh=False, allow_stale=True)
    if cached_union is not None:
        with RUNTIME_CACHE_LOCK:
            RUNTIME_CACHE["want_to_union_films"] = clone_film_rows(cached_union)
        primed_keys.append("want_to_union")
        if cached_union_stale and not is_movie_cache_refresh_pending("want_to_union"):
            scheduled_refresh = schedule_movie_cache_refresh(
                "want_to_union",
                lambda: fetch_want_to_films_from_union_source(force_refresh=True, save_report=False),
            ) or scheduled_refresh

    for cache_key in ("library_union_enabled", "library_union_disabled"):
        cached_library, cached_library_stale = get_persisted_film_cache_entry(cache_key, force_refresh=False, allow_stale=True)
        if cached_library is None:
            continue
        with RUNTIME_CACHE_LOCK:
            RUNTIME_CACHE.setdefault("library_films", {})[cache_key] = clone_film_rows(cached_library)
        primed_keys.append(cache_key)
        if cached_library_stale and not is_movie_cache_refresh_pending("films"):
            scheduled_refresh = schedule_movie_cache_refresh("films", refresh_film_cache_from_source) or scheduled_refresh

    with RUNTIME_CACHE_LOCK:
        RUNTIME_CACHE["initialized"] = True
    if primed_keys:
        print(f"[movie-source-cache] init=primed keys={','.join(primed_keys)}")
    elif scheduled_refresh:
        print("[movie-source-cache] init=deferred refresh=scheduled")
    else:
        print("[movie-source-cache] init=deferred refresh=pending")


@app.before_request
def initialize_runtime_cache_once():
    with RUNTIME_CACHE_LOCK:
        initialized = RUNTIME_CACHE["initialized"]
    if not initialized:
        warm_runtime_cache()

def refresh_film_cache_from_source():
    films = fetch_all_films_from_notion()
    clear_film_cache_entry()
    clear_runtime_film_cache_keys()
    set_persisted_film_cache_entry("all", films)
    with RUNTIME_CACHE_LOCK:
        RUNTIME_CACHE["films"] = [dict(film) for film in films]
        RUNTIME_CACHE["library_films"] = {}
        RUNTIME_CACHE["want_to_union_films"] = None
        RUNTIME_CACHE["initialized"] = True
    return [dict(film) for film in films]


def refresh_all_cached_data(refresh_films=True, refresh_youtube=True):
    if refresh_films:
        refresh_film_cache_from_source()
    if not refresh_youtube:
        return
    with RUNTIME_CACHE_LOCK:
        RUNTIME_CACHE["youtube_playlists"] = {}
        RUNTIME_CACHE["youtube_section_feeds"] = {}
    playlists = load_playlists()
    for section_playlists in playlists.values():
        for playlist in section_playlists:
            if playlist.get("id"):
                get_all_playlist_videos(playlist["id"], force_refresh=True)
    clear_persisted_youtube_section_feed_cache()

# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  NOTION FETCH (unchanged)
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
def fetch_all_films_from_notion():
    url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json"
    }
    director_catalog = build_director_catalog()
    directors_by_page_id = director_catalog.get("records_by_page_id", {})
    genre_catalog = build_genre_catalog()
    genres_by_page_id = genre_catalog.get("records_by_page_id", {})
    films = []
    payload = {"page_size": 100}
    page_number = 0
    retries = 3
    while True:
        page_number += 1
        attempt = 0
        resp = None
        data = None
        while attempt < retries:
            attempt += 1
            try:
                print(
                    f"[notion-fetch] page={page_number} attempt={attempt} "
                    f"fetched_so_far={len(films)} start_cursor={payload.get('start_cursor', '') or ''}"
                )
                resp = requests.post(url, headers=headers, json=payload, timeout=30)
                if resp.status_code != 200:
                    print(
                        f"[notion-fetch] page={page_number} attempt={attempt} "
                        f"status={resp.status_code} body={resp.text[:500]}"
                    )
                    if resp.status_code in (429, 500, 502, 503, 504) and attempt < retries:
                        time.sleep(1.5 * attempt)
                        continue
                    raise RuntimeError(f"Notion query failed with status {resp.status_code}: {resp.text[:500]}")
                data = resp.json()
                break
            except (requests.Timeout, requests.ConnectionError, requests.RequestException) as exc:
                print(
                    f"[notion-fetch] page={page_number} attempt={attempt} "
                    f"error={type(exc).__name__}: {exc}"
                )
                if attempt >= retries:
                    raise RuntimeError(f"Notion query failed after {retries} attempts on page {page_number}: {exc}") from exc
                time.sleep(1.5 * attempt)
            except ValueError as exc:
                print(f"[notion-fetch] page={page_number} invalid_json_error={exc}")
                raise RuntimeError(f"Notion query returned invalid JSON on page {page_number}: {exc}") from exc
        if data is None:
            raise RuntimeError(f"Notion query returned no data on page {page_number}.")
        has_more = bool(data.get("has_more"))
        next_cursor = data.get("next_cursor")
        print(
            f"[notion-fetch] page={page_number} fetched_rows={len(data.get('results', []))} "
            f"total_so_far={len(films)} has_more={has_more} next_cursor={'set' if next_cursor else 'missing'}"
        )
        for page in data.get("results", []):
            page_id = page.get("id", "")
            props = page.get("properties", {})
            def _text(key):
                prop = props.get(key, {})
                ptype = prop.get("type", "")
                if ptype == "title": parts = prop.get("title", [])
                elif ptype == "rich_text": parts = prop.get("rich_text", [])
                elif ptype == "url": return prop.get("url") or ""
                else: parts = []
                return "".join(t.get("plain_text", "") for t in parts).strip()
            def _number(key):
                prop = props.get(key, {})
                if prop.get("type") != "number":
                    return None
                return prop.get("number")
            def _select(key):
                sel = props.get(key, {}).get("select")
                return sel["name"].strip() if sel else ""
            def _multi_select(key):
                items = props.get(key, {}).get("multi_select", [])
                return ", ".join(item.get("name", "").strip() for item in items if item.get("name"))
            def _date(key):
                d = props.get(key, {}).get("date")
                return d["start"] if d else ""
            def _files_url(key):
                file_prop = props.get(key, {})
                if file_prop.get("type") != "files":
                    return ""
                files = file_prop.get("files", [])
                if not files:
                    return ""
                first_file = files[0]
                if first_file.get("type") == "external":
                    return first_file.get("external", {}).get("url", "")
                if first_file.get("type") == "file":
                    return first_file.get("file", {}).get("url", "")
                return ""
            def _relation_ids(key):
                prop = props.get(key, {})
                if prop.get("type") != "relation":
                    return []
                return [
                    str(item.get("id") or "").strip()
                    for item in prop.get("relation", [])
                    if str(item.get("id") or "").strip()
                ]
            name = _text("Name")
            if not name: continue
            poster_url = _files_url("poster ") or _text("Poster URL")
            score = _select("Score /5")
            status = _select("Status")
            source_value = normalize_movie_source(_select("source"))
            category = _select("category")
            watch_date = _date("watching history")
            finish_date = _date("finishing history")
            rewatch = _select("I will watch it again")
            trailer_url = _text("Trailer")
            year_value = _number("Year")
            runtime_value = _number("Runtime")
            overview_text = _text("Overview") or _text("Synopsis") or _text("Description")
            genres_text = _multi_select("Genres") or _text("Genres")
            director_relation_ids = _relation_ids(DIRECTOR_RELATION_PROPERTY)
            genre_relation_ids = _relation_ids(GENRE_RELATION_PROPERTY)
            director_entries = []
            for relation_id in director_relation_ids:
                director_record = directors_by_page_id.get(relation_id)
                if not director_record:
                    continue
                director_entries.append({
                    "page_id": director_record.get("page_id", ""),
                    "name": director_record.get("display_name", ""),
                    "image_url": director_record.get("image_url", ""),
                    "page_url": director_record.get("page_url", ""),
                })
            genre_entries = []
            for relation_id in genre_relation_ids:
                genre_record = genres_by_page_id.get(relation_id)
                if not genre_record:
                    continue
                genre_entries.append({
                    "page_id": genre_record.get("page_id", ""),
                    "name": genre_record.get("display_name", ""),
                    "page_url": genre_record.get("page_url", ""),
                })
            films.append({
                "notion_page_id": page_id,
                "name": name, "poster": poster_url, "score": score,
                "score_num": SCORE_ORDER.get(score, 0), "status": status,
                "source": source_value,
                "category": category, "watch_date": watch_date,
                "finish_date": finish_date, "rewatch": rewatch, "trailer": trailer_url,
                "year": int(year_value) if isinstance(year_value, (int, float)) else (str(year_value).strip() if year_value not in (None, "") else ""),
                "director": _text("Director"),
                "director_relation_ids": director_relation_ids,
                "director_entries": director_entries,
                "genre_relation_ids": genre_relation_ids,
                "genre_entries": genre_entries,
                "genres": genres_text,
                "runtime": int(runtime_value) if isinstance(runtime_value, (int, float)) else (str(runtime_value).strip() if runtime_value not in (None, "") else ""),
                "overview": overview_text,
                "tmdb_rating": _number("Rating") if _number("Rating") not in (None, "") else ""
            })
        if not has_more:
            print(f"[notion-fetch] page={page_number} complete: no more results")
            break
        if not next_cursor:
            print(f"[notion-fetch] page={page_number} stopped early: has_more was true but next_cursor was missing")
            break
        payload["start_cursor"] = next_cursor
        print(f"[notion-fetch] page={page_number} continuing with next_cursor")
    return films


def extract_notion_page_title(properties):
    props = properties or {}
    preferred = props.get("Name")
    if preferred and preferred.get("type") == "title":
        title_parts = preferred.get("title", [])
        title = "".join(part.get("plain_text", "") for part in title_parts).strip()
        if title:
            return title
    for prop in props.values():
        if prop.get("type") != "title":
            continue
        title_parts = prop.get("title", [])
        title = "".join(part.get("plain_text", "") for part in title_parts).strip()
        if title:
            return title
    return "Untitled"


def notion_title_text(value):
    text = str(value or "").strip()
    if not text:
        return []
    return [{"type": "text", "text": {"content": text[:2000]}}]


def notion_rich_text(value):
    text = str(value or "").strip()
    if not text:
        return []
    return [{"type": "text", "text": {"content": text[:2000]}}]


def notion_database_title_text(database_payload):
    parts = (database_payload or {}).get("title", []) or []
    return "".join(part.get("plain_text", "") for part in parts).strip()


def notion_files_first_url(prop):
    if not isinstance(prop, dict) or prop.get("type") != "files":
        return ""
    files = prop.get("files", []) or []
    if not files:
        return ""
    first = files[0] or {}
    if first.get("type") == "external":
        return str((first.get("external") or {}).get("url") or "").strip()
    if first.get("type") == "file":
        return str((first.get("file") or {}).get("url") or "").strip()
    return ""


def notion_property_text_value(prop):
    if not isinstance(prop, dict):
        return ""
    ptype = str(prop.get("type") or "").strip().lower()
    if ptype == "title":
        parts = prop.get("title", []) or []
        return "".join(part.get("plain_text", "") for part in parts).strip()
    if ptype == "rich_text":
        parts = prop.get("rich_text", []) or []
        return "".join(part.get("plain_text", "") for part in parts).strip()
    if ptype == "select":
        select_value = prop.get("select")
        return str(select_value.get("name") or "").strip() if isinstance(select_value, dict) else ""
    if ptype == "multi_select":
        return ", ".join(
            str(item.get("name") or "").strip()
            for item in prop.get("multi_select", []) or []
            if str(item.get("name") or "").strip()
        )
    if ptype == "url":
        return str(prop.get("url") or "").strip()
    if ptype == "number":
        value = prop.get("number")
        return "" if value in (None, "") else str(value).strip()
    if ptype == "status":
        status_value = prop.get("status")
        return str(status_value.get("name") or "").strip() if isinstance(status_value, dict) else ""
    if ptype == "date":
        date_value = prop.get("date")
        return str((date_value or {}).get("start") or "").strip() if isinstance(date_value, dict) else ""
    if ptype == "checkbox":
        return "true" if bool(prop.get("checkbox")) else "false"
    return ""


def notion_property_multi_select_names(prop):
    if not isinstance(prop, dict) or prop.get("type") != "multi_select":
        return []
    names = []
    for item in prop.get("multi_select", []) or []:
        name = str(item.get("name") or "").strip()
        if name:
            names.append(name)
    return names


def notion_property_checkbox_value(prop):
    if not isinstance(prop, dict):
        return False
    ptype = str(prop.get("type") or "").strip().lower()
    if ptype == "checkbox":
        return bool(prop.get("checkbox"))
    if ptype == "select":
        value = notion_property_text_value(prop).strip().lower()
        return value in {"true", "yes", "on", "pinned", "favorite", "starred"}
    if ptype == "number":
        value = prop.get("number")
        return value not in (None, "", 0, 0.0)
    if ptype in {"rich_text", "title"}:
        value = notion_property_text_value(prop).strip().lower()
        return value in {"true", "yes", "on", "pinned", "favorite", "starred"}
    return False


def notion_api_headers():
    return {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json"
    }


def notion_property_is_empty(prop):
    if not isinstance(prop, dict):
        return True
    ptype = prop.get("type", "")
    if ptype == "title":
        return not "".join(item.get("plain_text", "") for item in prop.get("title", [])).strip()
    if ptype == "rich_text":
        return not "".join(item.get("plain_text", "") for item in prop.get("rich_text", [])).strip()
    if ptype == "number":
        return prop.get("number") in (None, "")
    if ptype == "url":
        return not (prop.get("url") or "").strip()
    if ptype == "files":
        return not bool(prop.get("files") or [])
    if ptype == "multi_select":
        return not bool(prop.get("multi_select") or [])
    if ptype == "relation":
        return not bool(prop.get("relation") or [])
    return True


def extract_notion_relation_ids(properties, key):
    props = properties or {}
    prop = props.get(key, {}) or {}
    if prop.get("type") != "relation":
        return []
    ids = []
    for item in prop.get("relation", []) or []:
        relation_id = str(item.get("id") or "").strip()
        if relation_id:
            ids.append(relation_id)
    return ids


def split_director_names(value):
    text = re.sub(r"\s+", " ", str(value or "").strip())
    if not text:
        return []
    parts = re.split(r"\s*(?:,|/|;|&|\band\b)\s*", text, flags=re.IGNORECASE)
    cleaned = []
    seen = set()
    for item in parts:
        name = re.sub(r"\s+", " ", str(item or "").strip())
        key = normalized_person_key(name)
        if not key or key in seen:
            continue
        seen.add(key)
        cleaned.append(name)
    return cleaned


def normalized_genre_key(value):
    return re.sub(r"[^a-z0-9]+", "", clean_correction_text(value).lower())


def split_genre_names(value):
    text = re.sub(r"\s+", " ", str(value or "").strip())
    if not text:
        return []
    parts = re.split(r"\s*(?:,|/|;|\|)\s*", text)
    cleaned = []
    seen = set()
    for item in parts:
        name = re.sub(r"\s+", " ", str(item or "").strip())
        key = normalized_genre_key(name)
        if not key or key in seen:
            continue
        seen.add(key)
        cleaned.append(name)
    return cleaned


def normalize_movie_source(value):
    normalized = re.sub(r"[^a-z]+", "", str(value or "").strip().lower())
    if normalized == "mylibrary":
        return "My library"
    if normalized == "ebertslibrary":
        return "Ebert's library"
    if normalized in {"mylibraryandeberts", "mylibraryandebertslibrary"}:
        return "My library and Ebert's"
    return str(value or "").strip()


def normalize_source_filter(value):
    normalized = normalize_movie_source(value)
    if normalized in {"My library", "Ebert's library", "My library and Ebert's"}:
        return normalized
    return "All sources"


def movie_matches_source_filter(film, source_filter):
    normalized_filter = normalize_source_filter(source_filter)
    normalized_source = normalize_movie_source((film or {}).get("source", ""))
    if normalized_filter == "All sources":
        return True
    if normalized_filter == "My library":
        return normalized_source in {"My library", "My library and Ebert's"}
    if normalized_filter == "Ebert's library":
        return normalized_source in {"Ebert's library", "My library and Ebert's"}
    if normalized_filter == "My library and Ebert's":
        return normalized_source == "My library and Ebert's"
    return True


def choose_genre_display_name(current_value, candidate):
    current = str(current_value or "").strip()
    next_value = re.sub(r"\s+", " ", str(candidate or "").strip())
    if not current:
        return next_value
    if not next_value:
        return current
    if len(next_value) > len(current):
        return next_value
    if current.lower() == current and next_value != next_value.lower():
        return next_value
    return current


def extract_notion_genres_value(properties):
    genres_prop = (properties or {}).get("Genres", {}) or {}
    if genres_prop.get("type") == "multi_select":
        return ", ".join(
            item.get("name", "").strip()
            for item in genres_prop.get("multi_select", []) or []
            if item.get("name")
        )
    if genres_prop.get("type") == "rich_text":
        return "".join(item.get("plain_text", "") for item in genres_prop.get("rich_text", [])).strip()
    return ""


def build_notion_genres_property_payload(genres_prop, genres_value):
    cleaned_names = split_genre_names(genres_value)
    if not cleaned_names or not isinstance(genres_prop, dict):
        return None
    if genres_prop.get("type") == "multi_select":
        return {"multi_select": [{"name": name[:100]} for name in cleaned_names]}
    if genres_prop.get("type") == "rich_text":
        return {"rich_text": [{"type": "text", "text": {"content": ", ".join(cleaned_names)[:2000]}}]}
    return None


def choose_director_display_name(current_value, candidate):
    current = str(current_value or "").strip()
    next_value = str(candidate or "").strip()
    if not current:
        return next_value
    if not next_value:
        return current
    if len(next_value) > len(current):
        return next_value
    if current.lower() == current and next_value != next_value.lower():
        return next_value
    return current


def normalize_movie_title(value):
    text = re.sub(r"\(\d{4}\)$", "", str(value or "").strip())
    text = re.sub(r"\[[^\]]+\]$", "", text).strip()
    return re.sub(r"\s+", " ", text)


def safe_console_text(value):
    text = str(value or "")
    try:
        text.encode("cp1252")
        return text
    except Exception:
        return text.encode("ascii", "backslashreplace").decode("ascii")


def normalized_match_key(value):
    return re.sub(r"[^a-z0-9]+", "", normalize_movie_title(value).lower())


def tmdb_media_type_for_category(category):
    normalized = normalize_movie_category(category)
    if normalized == "tv show":
        return "tv"
    if normalized in {"movie", "movis", "anime movie", "short movie", "theatre", "documentary"}:
        return "movie"
    if normalized == "anime":
        return "tv"
    return "movie"


def tmdb_result_title_candidates(result, media_type):
    if media_type == "tv":
        return [
            result.get("name", ""),
            result.get("original_name", ""),
        ]
    return [
        result.get("title", ""),
        result.get("original_title", ""),
    ]


def tmdb_result_year(result, media_type):
    date_value = result.get("first_air_date", "") if media_type == "tv" else result.get("release_date", "")
    return normalize_year_value(date_value)


def score_tmdb_candidate(result, title, media_type, preferred_year=""):
    wanted = normalized_match_key(title)
    candidate_titles = [item for item in tmdb_result_title_candidates(result, media_type) if item]
    if not candidate_titles:
        return None

    score = 0
    for candidate in candidate_titles:
        candidate_key = normalized_match_key(candidate)
        if candidate_key == wanted:
            score = max(score, 100)
        elif wanted and (wanted in candidate_key or candidate_key in wanted):
            score = max(score, 70)

    if score == 0:
        return None

    result_year = tmdb_result_year(result, media_type)
    if preferred_year and result_year:
        if preferred_year == result_year:
            score += 20
        else:
            score -= 25
    return score


def best_tmdb_match(results, title, media_type, preferred_year=""):
    if not results:
        return None

    scored = []
    for item in results:
        candidate_score = score_tmdb_candidate(item, title, media_type, preferred_year=preferred_year)
        if candidate_score is None:
            continue
        popularity = float(item.get("popularity") or 0)
        vote_count = int(item.get("vote_count") or 0)
        scored.append((candidate_score, popularity, vote_count, item))

    if not scored:
        return None

    scored.sort(key=lambda item: (item[0], item[1], item[2]), reverse=True)
    return scored[0][3]


def tmdb_request(path, params=None):
    if not (TMDB_API_KEY or "").strip():
        raise RuntimeError("Missing TMDB_API_KEY. Add it to the root .env file or process environment.")
    request_params = dict(params or {})
    request_params["api_key"] = TMDB_API_KEY.strip()
    response = requests.get(f"https://api.themoviedb.org/3{path}", params=request_params, timeout=20)
    response.raise_for_status()
    return response.json()


def tmdb_country_name_lookup():
    global TMDB_COUNTRY_NAME_CACHE
    if isinstance(TMDB_COUNTRY_NAME_CACHE, dict):
        return TMDB_COUNTRY_NAME_CACHE
    country_map = {}
    try:
        data = tmdb_request("/configuration/countries", {"language": "en-US"})
    except Exception:
        TMDB_COUNTRY_NAME_CACHE = {}
        return TMDB_COUNTRY_NAME_CACHE
    for item in data or []:
        code = str(item.get("iso_3166_1") or "").strip().upper()
        name = str(item.get("english_name") or item.get("native_name") or "").strip()
        if code and name:
            country_map[code] = name
    TMDB_COUNTRY_NAME_CACHE = country_map
    return TMDB_COUNTRY_NAME_CACHE


def extract_tmdb_origin_countries(details, media_type):
    country_names = []
    seen = set()
    for item in (details or {}).get("production_countries", []) or []:
        name = str(item.get("name") or "").strip()
        normalized_name = name.lower()
        if not name or normalized_name in seen:
            continue
        seen.add(normalized_name)
        country_names.append(name)
    if country_names:
        return country_names
    if media_type != "tv":
        return []
    country_map = tmdb_country_name_lookup()
    for code in (details or {}).get("origin_country", []) or []:
        normalized_code = str(code or "").strip().upper()
        name = country_map.get(normalized_code) or normalized_code
        normalized_name = name.lower()
        if not name or normalized_name in seen:
            continue
        seen.add(normalized_name)
        country_names.append(name)
    return country_names


def format_origin_country_display(country_names, limit=2):
    display_names = []
    seen = set()
    for name in list(country_names or []):
        display_name = TMDB_COUNTRY_DISPLAY_ALIASES.get(str(name or "").strip(), str(name or "").strip())
        normalized_name = display_name.lower()
        if not display_name or normalized_name in seen:
            continue
        seen.add(normalized_name)
        display_names.append(display_name)
        if len(display_names) >= max(int(limit or 0), 1):
            break
    return ", ".join(display_names)


def tmdb_image_url(path, size="w185"):
    path_value = str(path or "").strip()
    if not path_value:
        return ""
    return f"https://image.tmdb.org/t/p/{size}{path_value}"


def extract_top_billed_cast(credits, limit=8):
    cast_members = []
    for person in (credits or {}).get("cast", []):
        name = str(person.get("name") or "").strip()
        character = str(person.get("character") or "").strip()
        profile_url = tmdb_image_url(person.get("profile_path"), size="w185")
        if not name:
            continue
        cast_members.append({
            "name": name,
            "character": character,
            "profile_url": profile_url,
        })
        if len(cast_members) >= limit:
            break
    return cast_members


def score_tmdb_person_candidate(result, person_name):
    wanted = normalized_person_key(person_name)
    candidate_key = normalized_person_key(result.get("name", ""))
    if not wanted or not candidate_key:
        return None
    score = 0
    if candidate_key == wanted:
        score = 100
    elif wanted in candidate_key or candidate_key in wanted:
        score = 70
    else:
        return None
    if str(result.get("known_for_department") or "").strip().lower() == "directing":
        score += 20
    if result.get("profile_path"):
        score += 5
    return score


def tmdb_person_match_confidence(candidate_score):
    if candidate_score >= 120:
        return "high"
    if candidate_score >= 100:
        return "medium"
    return "low"


def fetch_tmdb_person_profile(person_name="", tmdb_person_id=None):
    normalized_name = re.sub(r"\s+", " ", str(person_name or "").strip())
    person_id = int(tmdb_person_id) if tmdb_person_id not in (None, "", 0, "0") else None
    cache_key = f"id:{person_id}" if person_id else normalized_person_key(normalized_name)
    if not cache_key:
        return None
    if cache_key in TMDB_PERSON_LOOKUP_CACHE:
        return TMDB_PERSON_LOOKUP_CACHE[cache_key]

    best_score = 120 if person_id else None
    best_match = None
    if person_id:
        best_match = {"id": person_id, "name": normalized_name}
    else:
        search_data = tmdb_request("/search/person", {
            "query": normalized_name,
            "include_adult": "false",
            "language": "en-US",
            "page": 1
        })
        scored = []
        for item in search_data.get("results", []):
            candidate_score = score_tmdb_person_candidate(item, normalized_name)
            if candidate_score is None:
                continue
            popularity = float(item.get("popularity") or 0)
            scored.append((candidate_score, popularity, item))
        if not scored:
            TMDB_PERSON_LOOKUP_CACHE[cache_key] = None
            return None
        scored.sort(key=lambda item: (item[0], item[1]), reverse=True)
        best_score, _, best_match = scored[0]
        person_id = best_match.get("id")

    if not person_id:
        TMDB_PERSON_LOOKUP_CACHE[cache_key] = None
        return None

    details = tmdb_request(f"/person/{person_id}", {"language": "en-US"})
    result = {
        "tmdb_person_id": person_id,
        "matched_name": str(details.get("name") or best_match.get("name") or "").strip(),
        "match_score": best_score if best_score is not None else 120,
        "confidence": "high" if tmdb_person_id not in (None, "", 0, "0") else tmdb_person_match_confidence(best_score or 0),
        "known_for_department": str(details.get("known_for_department") or best_match.get("known_for_department") or "").strip(),
        "profile_url": tmdb_image_url(details.get("profile_path") or best_match.get("profile_path"), size="w500"),
        "biography": str(details.get("biography") or "").strip(),
    }
    TMDB_PERSON_LOOKUP_CACHE[cache_key] = result
    return result


def fetch_tmdb_enrichment(movie_title, category="", year=""):
    normalized_title = normalize_movie_title(movie_title)
    media_type = tmdb_media_type_for_category(category)
    preferred_year = normalize_year_value(year)
    cache_key = f"{media_type}|{normalized_title.lower()}|{preferred_year}"
    if cache_key in TMDB_LOOKUP_CACHE:
        return TMDB_LOOKUP_CACHE[cache_key]

    search_data = tmdb_request(f"/search/{media_type}", {
        "query": normalized_title,
        "include_adult": "false",
        "language": "en-US",
        "page": 1
    })
    match = best_tmdb_match(search_data.get("results", []), normalized_title, media_type, preferred_year=preferred_year)
    if not match:
        TMDB_LOOKUP_CACHE[cache_key] = None
        return None

    movie_id = match.get("id")
    if not movie_id:
        TMDB_LOOKUP_CACHE[cache_key] = None
        return None

    match_score = score_tmdb_candidate(match, normalized_title, media_type, preferred_year=preferred_year) or 0

    details = tmdb_request(f"/{media_type}/{movie_id}", {"language": "en-US"})
    credits = tmdb_request(f"/{media_type}/{movie_id}/credits", {"language": "en-US"})
    if media_type == "tv":
        director = next((
            person.get("name", "").strip()
            for person in credits.get("crew", [])
            if person.get("job") in {"Executive Producer", "Creator"} and person.get("name")
        ), "") or ", ".join(
            item.get("name", "").strip()
            for item in details.get("created_by", [])
            if item.get("name")
        )
    else:
        director = next((
            person.get("name", "").strip()
            for person in credits.get("crew", [])
            if person.get("job") == "Director" and person.get("name")
        ), "")
    release_date = details.get("first_air_date") if media_type == "tv" else details.get("release_date")
    release_date = release_date or ""
    year_value = None
    if release_date[:4].isdigit():
        year_value = int(release_date[:4])
    poster_path = details.get("poster_path") or ""
    poster_url = tmdb_image_url(poster_path, size="w500")
    genres = ", ".join(
        item.get("name", "").strip()
        for item in details.get("genres", [])
        if item.get("name")
    )
    overview = str(details.get("overview") or "").strip()
    result = {
        "tmdb_id": movie_id,
        "tmdb_type": media_type,
        "matched_title": next((title for title in tmdb_result_title_candidates(match, media_type) if title), ""),
        "matched_year": tmdb_result_year(match, media_type),
        "match_score": match_score,
        "year": year_value,
        "director": director,
        "rating": details.get("vote_average"),
        "poster_url": poster_url,
        "genres": genres,
        "overview": overview,
        "origin_countries": extract_tmdb_origin_countries(details, media_type),
        "top_billed_cast": extract_top_billed_cast(credits, limit=8),
    }
    TMDB_LOOKUP_CACHE[cache_key] = result
    return result


def ensure_tmdb_enrichment_properties():
    response = requests.get(
        f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}",
        headers=notion_api_headers(),
        timeout=60
    )
    response.raise_for_status()
    properties = (response.json() or {}).get("properties", {})
    desired = {
        "Year": {"number": {"format": "number"}},
        "Director": {"rich_text": {}},
        "Rating": {"number": {"format": "number"}},
        "Poster URL": {"url": {}},
        "Genres": {"rich_text": {}},
    }
    missing = {name: config for name, config in desired.items() if name not in properties}
    if not missing:
        return properties
    patch_response = requests.patch(
        f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}",
        headers=notion_api_headers(),
        json={"properties": missing},
        timeout=60
    )
    patch_response.raise_for_status()
    return (patch_response.json() or {}).get("properties", properties)


def fetch_all_notion_database_pages(database_id=None):
    target_database_id = database_id or NOTION_DATABASE_ID
    pages = []
    payload = {"page_size": 100}
    while True:
        response = requests.post(
            f"https://api.notion.com/v1/databases/{target_database_id}/query",
            headers=notion_api_headers(),
            json=payload,
            timeout=60
        )
        response.raise_for_status()
        data = response.json() or {}
        pages.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        payload["start_cursor"] = data.get("next_cursor")
    return pages


VALID_BOOK_STATUSES = {"reading", "finished", "abandoned", "want to read", "wishlist", "paused", "dnf"}


def normalize_book_status(value):
    normalized = re.sub(r"\s+", " ", str(value or "").strip().lower())
    if not normalized or normalized == "all":
        return ""
    aliases = {
        "want to read": "want to read",
        "want to read list": "want to read",
        "to read": "want to read",
        "wishlist": "wishlist",
        "reading": "reading",
        "in progress": "reading",
        "finished": "finished",
        "complete": "finished",
        "completed": "finished",
        "abandoned": "abandoned",
        "dropped": "abandoned",
        "dnf": "dnf",
        "paused": "paused",
    }
    if normalized in VALID_BOOK_STATUSES:
        return normalized
    return aliases.get(normalized, normalized)


def format_books_date_label(value):
    timestamp = parse_timestamp(value)
    if not timestamp:
        return ""
    try:
        if timestamp.tzinfo is None:
            return timestamp.strftime("%b %d, %Y")
        return timestamp.astimezone().strftime("%b %d, %Y")
    except Exception:
        return ""


def format_books_rating(value):
    if value in (None, ""):
        return ""
    if isinstance(value, bool):
        return ""
    try:
        number_value = float(value)
    except (TypeError, ValueError):
        return str(value).strip()
    if number_value.is_integer():
        return str(int(number_value))
    formatted = f"{number_value:.1f}"
    return formatted.rstrip("0").rstrip(".")


def books_status_label(value):
    normalized = str(value or "").strip().lower()
    labels = {
        "reading": "Reading",
        "finished": "Finished",
        "abandoned": "Abandoned",
        "want to read": "Want to Read",
        "wishlist": "Wishlist",
        "paused": "Paused",
        "dnf": "DNF",
    }
    return labels.get(normalized, normalized.title() if normalized else "")


def books_content_paragraphs(text):
    source = str(text or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    if not source:
        return []
    return [part.strip() for part in re.split(r"\n{2,}", source) if part.strip()]


def books_notion_property(properties, *keys):
    props = properties or {}
    for key in keys:
        if key in props:
            return props.get(key, {}) or {}
    lowered_map = {str(name).strip().lower(): value for name, value in props.items()}
    for key in keys:
        value = lowered_map.get(str(key).strip().lower())
        if isinstance(value, dict):
            return value
    return {}


def books_notion_text(prop):
    return notion_property_text_value(prop)


def books_notion_list_from_property(prop):
    if not isinstance(prop, dict):
        return []
    ptype = str(prop.get("type") or "").strip().lower()
    if ptype == "multi_select":
        return notion_property_multi_select_names(prop)
    if ptype in {"title", "rich_text", "select", "url", "number", "date", "checkbox"}:
        text_value = notion_property_text_value(prop)
        if not text_value:
            return []
        return [part.strip() for part in re.split(r"\s*(?:,|;|/|&|\band\b)\s*", text_value, flags=re.IGNORECASE) if part.strip()]
    return []


def books_notion_property_text(properties, *keys):
    props = properties or {}
    for key in keys:
        prop = books_notion_property(props, key)
        text_value = notion_property_text_value(prop)
        if text_value:
            return text_value
    return ""


def books_notion_property_list(properties, *keys):
    props = properties or {}
    for key in keys:
        prop = books_notion_property(props, key)
        list_value = books_notion_list_from_property(prop)
        if list_value:
            return list_value
    return []


def books_notion_file_url(prop):
    if not isinstance(prop, dict):
        return ""
    ptype = str(prop.get("type") or "").strip().lower()
    if ptype == "files":
        for item in prop.get("files", []) or []:
            if not isinstance(item, dict):
                continue
            file_value = item.get("file")
            if isinstance(file_value, dict):
                url = str(file_value.get("url") or "").strip()
                if url:
                    return url
            external_value = item.get("external")
            if isinstance(external_value, dict):
                url = str(external_value.get("url") or "").strip()
                if url:
                    return url
    if ptype == "url":
        return str(prop.get("url") or "").strip()
    return ""


def books_notion_property_file_url(properties, *keys):
    props = properties or {}
    for key in keys:
        prop = books_notion_property(props, key)
        file_url = books_notion_file_url(prop)
        if file_url:
            return file_url
    return ""


def normalize_book_cover_cache_key(title, authors):
    title_text = re.sub(r"\s+", " ", str(title or "").strip().lower())
    if isinstance(authors, (list, tuple)):
        authors_text = ", ".join(str(item or "").strip() for item in authors if str(item or "").strip())
    else:
        authors_text = str(authors or "").strip()
    authors_text = re.sub(r"\s+", " ", authors_text).strip().lower()
    return f"{title_text}|{authors_text}"


def books_text_direction(value):
    text = re.sub(r"\s+", " ", str(value or "").strip())
    if not text:
        return "ltr"
    arabic_count = len(re.findall(r"[\u0600-\u06ff]", text))
    latin_count = len(re.findall(r"[A-Za-z]", text))
    if arabic_count > latin_count:
        return "rtl"
    if latin_count > arabic_count:
        return "ltr"
    return "ltr"


def fetch_openlibrary_cover_url(title, authors):
    query_parts = []
    if title:
        query_parts.append(f"title={urllib.parse.quote(str(title).strip())}")
    if authors:
        author_text = ", ".join(authors) if isinstance(authors, (list, tuple)) else str(authors)
        query_parts.append(f"author={urllib.parse.quote(author_text.strip())}")
    if not query_parts:
        return ""
    query_url = "https://openlibrary.org/search.json?" + "&".join(query_parts) + "&limit=1"
    try:
        response = requests.get(query_url, timeout=6)
        response.raise_for_status()
        data = response.json() or {}
    except Exception:
        return ""
    docs = data.get("docs", []) or []
    if not docs:
        return ""
    doc = docs[0] or {}
    cover_id = doc.get("cover_i")
    if cover_id:
        return f"https://covers.openlibrary.org/b/id/{cover_id}-M.jpg"
    edition_key = str(doc.get("cover_edition_key") or "").strip()
    if edition_key:
        return f"https://covers.openlibrary.org/b/olid/{urllib.parse.quote(edition_key)}-M.jpg"
    return ""


def fetch_google_books_cover_url(title, authors):
    query_terms = []
    if title:
        query_terms.append(f"intitle:{str(title).strip()}")
    if authors:
        author_text = ", ".join(authors) if isinstance(authors, (list, tuple)) else str(authors)
        query_terms.append(f"inauthor:{author_text.strip()}")
    if not query_terms:
        return ""
    query = " ".join(query_terms)
    query_url = f"https://www.googleapis.com/books/v1/volumes?q={urllib.parse.quote(query)}&maxResults=1"
    try:
        response = requests.get(query_url, timeout=6)
        response.raise_for_status()
        data = response.json() or {}
    except Exception:
        return ""
    items = data.get("items", []) or []
    if not items:
        return ""
    volume_info = (items[0] or {}).get("volumeInfo", {}) or {}
    image_links = volume_info.get("imageLinks", {}) or {}
    return str(image_links.get("thumbnail") or image_links.get("smallThumbnail") or "").strip()


def resolve_book_cover_url(page, title, authors, properties):
    cache_key = normalize_book_cover_cache_key(title, authors)
    cached_value = BOOK_COVER_CACHE.get(cache_key)
    if cached_value is not None:
        return cached_value

    page = page if isinstance(page, dict) else {}
    props = properties or {}

    cover_candidates = [
        page.get("cover"),
        books_notion_property(props, "Cover"),
        books_notion_property(props, "Cover Image"),
        books_notion_property(props, "Image"),
        books_notion_property(props, "Image URL"),
        books_notion_property(props, "Book Cover"),
        books_notion_property(props, "Poster"),
    ]
    for candidate in cover_candidates:
        file_url = books_notion_file_url(candidate)
        if file_url:
            BOOK_COVER_CACHE[cache_key] = file_url
            return file_url

    for source_fetcher in (fetch_openlibrary_cover_url, fetch_google_books_cover_url):
        try:
            cover_url = source_fetcher(title, authors)
        except Exception:
            cover_url = ""
        if cover_url:
            BOOK_COVER_CACHE[cache_key] = cover_url
            return cover_url

    BOOK_COVER_CACHE[cache_key] = ""
    return ""


def notion_book_page_to_entry(page):
    page = page if isinstance(page, dict) else {}
    props = page.get("properties", {}) or {}
    title = extract_notion_page_title(props)
    if not title or title == "Untitled":
        return {}

    authors = books_notion_property_list(props, "Authors", "Author", "author", "authors")
    status_value = normalize_book_status(books_notion_property_text(props, "Status", "status"))
    rating_prop = books_notion_property(props, "Rating", "rating")
    decision_prop = books_notion_property(props, "Decision", "decision")
    book_quotes_relation_ids = extract_notion_relation_ids(props, "Book Quotes")
    if not book_quotes_relation_ids:
        book_quotes_relation_ids = extract_notion_relation_ids(props, "Book Quote")
    if not book_quotes_relation_ids:
        book_quotes_relation_ids = extract_notion_relation_ids(props, "Quotes")
    date_finished_prop = books_notion_property(
        props,
        "Date Finished",
        "Date finished",
        "Finished",
        "Finished On",
        "Completion Date",
        "Date",
    )
    tags_prop = books_notion_property(props, "Tags", "tags", "Tag", "tag")
    history_prop = books_notion_property(props, "reading history", "Reading History", "Reading history", "History")
    kinde_prop = books_notion_property(props, "kinde", "Kinde", "Kindle")
    content_prop = books_notion_property(props, "Content", "content", "Body", "body", "Excerpt", "excerpt")
    pinned_prop = books_notion_property(props, "Pinned", "pinned", "Favorite", "Starred")

    rating_value = format_books_rating(books_notion_text(rating_prop))
    decision_value = books_notion_text(decision_prop)
    date_finished_value = books_notion_text(date_finished_prop)
    tags = notion_property_multi_select_names(tags_prop)
    history_value = books_notion_text(history_prop)
    kinde_value = books_notion_text(kinde_prop)
    content_value = books_notion_text(content_prop)
    pinned_value = notion_property_checkbox_value(pinned_prop)
    cover_url = (
        books_notion_file_url(page.get("cover"))
        or books_notion_property_file_url(props, "Cover", "Cover Image", "Image", "Image URL", "Book Cover", "Poster")
    )

    effective_date = normalize_timestamp_value(date_finished_value or page.get("created_time", ""))
    created_time = normalize_timestamp_value(page.get("created_time", ""))
    last_edited_time = normalize_timestamp_value(page.get("last_edited_time", ""))
    entry_id = compact_notion_id(page.get("id"))
    history_paragraphs = books_content_paragraphs(history_value)
    content_paragraphs = books_content_paragraphs(content_value)

    return {
        "id": entry_id,
        "notion_page_id": str(page.get("id") or "").strip(),
        "title": title,
        "authors": authors,
        "authors_display": ", ".join(authors),
        "status": status_value,
        "status_label": books_status_label(status_value),
        "rating": rating_value,
        "decision": decision_value,
        "date_finished": effective_date,
        "date_finished_display": format_books_date_label(effective_date),
        "created_time": created_time,
        "last_edited_time": last_edited_time,
        "book_quotes_relation_ids": book_quotes_relation_ids,
        "tags": tags,
        "tags_display": ", ".join(tags),
        "history": history_value,
        "history_paragraphs": history_paragraphs,
        "content": content_value,
        "content_paragraphs": content_paragraphs,
        "excerpt": content_paragraphs[0] if content_paragraphs else (history_paragraphs[0] if history_paragraphs else content_value[:240].strip() or history_value[:240].strip()),
        "kinde": kinde_value,
        "kinde_display": kinde_value,
        "pinned": pinned_value,
        "cover_url": cover_url,
        "cover_source": "notion" if cover_url else "",
        "url": page.get("url", ""),
    }


def _book_quote_page_sort_value(page_value):
    text = str(page_value or "").strip()
    if not text:
        return 10**9
    match = re.search(r"\d+", text)
    if not match:
        return 10**9 - 1
    try:
        return int(match.group(0))
    except (TypeError, ValueError):
        return 10**9 - 1


def notion_book_quote_page_to_entry(page, schema):
    page = page if isinstance(page, dict) else {}
    props = page.get("properties", {}) or {}
    schema = schema if isinstance(schema, dict) else {}
    quote_property = schema.get("quote_property", "")
    book_property = schema.get("book_property", "")
    author_property = schema.get("author_property", "")
    page_property = schema.get("page_property", "")
    chapter_property = schema.get("chapter_property", "")
    favorite_property = schema.get("favorite_property", "")
    tags_property = schema.get("tags_property", "")
    if not quote_property or not book_property:
        return {}

    quote_text = notion_property_text_value(props.get(quote_property, {}))
    book_relation_ids = extract_notion_relation_ids(props, book_property)
    if not quote_text or not book_relation_ids:
        return {}

    author_value = notion_property_text_value(props.get(author_property, {})) if author_property else ""
    page_value = notion_property_text_value(props.get(page_property, {})) if page_property else ""
    chapter_value = notion_property_text_value(props.get(chapter_property, {})) if chapter_property else ""
    favorite_value = notion_property_checkbox_value(props.get(favorite_property, {})) if favorite_property else False
    tags_value = notion_property_multi_select_names(props.get(tags_property, {})) if tags_property else []

    primary_book_id = compact_notion_id(book_relation_ids[0]) if book_relation_ids else ""
    return {
        "id": compact_notion_id(page.get("id", "")),
        "notion_page_id": str(page.get("id") or "").strip(),
        "quote": quote_text,
        "quote_excerpt": quote_text[:240].strip(),
        "book_relation_ids": [compact_notion_id(item) for item in book_relation_ids if compact_notion_id(item)],
        "book_page_id": primary_book_id,
        "author": author_value,
        "page": page_value,
        "page_sort_value": _book_quote_page_sort_value(page_value),
        "chapter": chapter_value,
        "favorite": favorite_value,
        "tags": tags_value,
        "tags_display": ", ".join(tags_value),
        "created_time": normalize_timestamp_value(page.get("created_time", "")),
        "last_edited_time": normalize_timestamp_value(page.get("last_edited_time", "")),
        "url": page.get("url", ""),
    }


def fetch_book_quotes_entries(force_refresh=False):
    cache_age_seconds = 120
    cached_entries = BOOK_QUOTES_ENTRIES_CACHE.get("entries")
    cached_updated_at = float(BOOK_QUOTES_ENTRIES_CACHE.get("updated_at") or 0)
    if not force_refresh and cached_entries is not None and (time.time() - cached_updated_at) < cache_age_seconds:
        return {
            "entries": list(cached_entries or []),
            "error": str(BOOK_QUOTES_ENTRIES_CACHE.get("error") or ""),
        }

    database_id = str(NOTION_BOOK_QUOTES_DATABASE_ID or "").strip()
    if not database_id:
        result = {"entries": [], "error": "Set NOTION_BOOK_QUOTES_DATABASE_ID to enable Book Quotes."}
        BOOK_QUOTES_ENTRIES_CACHE.update({"entries": [], "error": result["error"], "updated_at": time.time()})
        return result

    try:
        database_payload = resolve_book_quotes_database(database_id=database_id)
        schema = build_book_quotes_database_schema(database_payload)
        blockers = validate_book_quotes_database_schema(schema)
        if blockers:
            result = {"entries": [], "error": "; ".join(blockers)}
            BOOK_QUOTES_ENTRIES_CACHE.update({"entries": [], "error": result["error"], "updated_at": time.time()})
            return result
        pages = fetch_all_notion_database_pages(database_id=schema.get("database_id", database_id))
    except Exception as exc:
        result = {"entries": [], "error": f"Could not load Book Quotes from Notion: {exc}"}
        BOOK_QUOTES_ENTRIES_CACHE.update({"entries": [], "error": result["error"], "updated_at": time.time()})
        return result

    entries = []
    for page in pages:
        entry = notion_book_quote_page_to_entry(page, schema)
        if entry:
            entries.append(entry)

    entries.sort(
        key=lambda item: (
            0 if item.get("favorite") else 1,
            item.get("page_sort_value", 10**9),
            item.get("created_time") or "",
            item.get("quote", "").lower(),
        )
    )
    result = {"entries": entries, "error": ""}
    BOOK_QUOTES_ENTRIES_CACHE.update({"entries": list(entries), "error": "", "updated_at": time.time()})
    return result


def fetch_book_quotes_for_entry(book_page_id, force_refresh=False):
    book_id = compact_notion_id(book_page_id)
    if not book_id:
        return {"entries": [], "error": ""}
    fetched = fetch_book_quotes_entries(force_refresh=force_refresh)
    entries = [
        entry for entry in list(fetched.get("entries", []) or [])
        if book_id in list(entry.get("book_relation_ids", []) or [])
    ]
    return {
        "entries": entries,
        "error": str(fetched.get("error") or ""),
    }


def fetch_books_entries(force_refresh=False):
    cache_age_seconds = 120
    cached_entries = BOOKS_ENTRIES_CACHE.get("entries")
    cached_updated_at = float(BOOKS_ENTRIES_CACHE.get("updated_at") or 0)
    if not force_refresh and cached_entries is not None and (time.time() - cached_updated_at) < cache_age_seconds:
        return {
            "entries": list(cached_entries or []),
            "error": str(BOOKS_ENTRIES_CACHE.get("error") or ""),
        }

    database_id = str(NOTION_BOOKS_DATABASE_ID or "").strip()
    if not database_id:
        result = {"entries": [], "error": "Set NOTION_BOOKS_DATABASE_ID to enable Books."}
        BOOKS_ENTRIES_CACHE.update({"entries": [], "error": result["error"], "updated_at": time.time()})
        return result
    try:
        pages = fetch_all_notion_database_pages(database_id=database_id)
    except Exception as exc:
        result = {"entries": [], "error": f"Could not load Books from Notion: {exc}"}
        BOOKS_ENTRIES_CACHE.update({"entries": [], "error": result["error"], "updated_at": time.time()})
        return result

    entries = []
    for page in pages:
        entry = notion_book_page_to_entry(page)
        if entry:
            entries.append(entry)

    entries.sort(
        key=lambda item: (
            1 if item.get("pinned") else 0,
            item.get("date_finished") or item.get("created_time") or "",
            item.get("last_edited_time") or "",
            item.get("title", "").lower(),
        ),
        reverse=True,
    )
    result = {"entries": entries, "error": ""}
    BOOKS_ENTRIES_CACHE.update({"entries": list(entries), "error": "", "updated_at": time.time()})
    return result


def filter_books_entries(entries, search_text="", status_filter="all"):
    normalized_search = str(search_text or "").strip().lower()
    status_text = str(status_filter or "").strip().lower()
    normalized_status = normalize_book_status(status_text) if status_text and status_text != "all" else ""
    filtered = list(entries or [])
    if normalized_status:
        filtered = [entry for entry in filtered if entry.get("status") == normalized_status]
    if normalized_search:
        filtered = [
            entry for entry in filtered
            if normalized_search in str(entry.get("title") or "").lower()
            or normalized_search in str(entry.get("authors_display") or "").lower()
            or normalized_search in str(entry.get("decision") or "").lower()
            or normalized_search in str(entry.get("history") or "").lower()
            or normalized_search in str(entry.get("content") or "").lower()
            or any(normalized_search in str(tag or "").lower() for tag in entry.get("tags", []) or [])
        ]
    return filtered


def build_books_view():
    fetched = fetch_books_entries()
    entries = list(fetched.get("entries", []) or [])
    raw_search = str(request.args.get("search", "") or "").strip()
    raw_status = str(request.args.get("status", "all") or "all").strip().lower() or "all"
    filtered = filter_books_entries(entries, search_text=raw_search, status_filter=raw_status)
    statuses = ["all"]
    for entry in entries:
        status_value = str(entry.get("status") or "").strip().lower()
        if status_value and status_value not in statuses:
            statuses.append(status_value)
    if len(statuses) == 1:
        statuses.extend(["reading", "finished", "want to read"])
    status_labels = {status: books_status_label(status) for status in statuses if status != "all"}
    return {
        "entries": filtered,
        "total": len(filtered),
        "all_entries_count": len(entries),
        "error_message": str(fetched.get("error") or "").strip(),
        "current_filters": {
            "search": raw_search,
            "status": raw_status,
        },
        "status_options": statuses,
        "status_option_labels": status_labels,
    }


def notion_page_title_text(page):
    page = page if isinstance(page, dict) else {}
    props = page.get("properties", {}) or {}
    title = extract_notion_page_title(props)
    if title and title != "Untitled":
        return title
    if page.get("object") == "page":
        title_prop = page.get("title", []) or []
        if title_prop:
            return "".join(part.get("plain_text", "") for part in title_prop).strip()
    return ""


def notion_search_page_by_title(title):
    target = str(title or "").strip()
    if not target:
        return {}
    try:
        results = notion_search_pages(target)
    except Exception:
        return {}
    target_normalized = re.sub(r"\s+", " ", target).strip().lower()
    fuzzy_matches = []
    for page in results:
        page_title = notion_page_title_text(page)
        normalized_title = re.sub(r"\s+", " ", page_title).strip().lower()
        if normalized_title == target_normalized:
            return page
        if target_normalized and target_normalized in normalized_title:
            fuzzy_matches.append(page)
    return fuzzy_matches[0] if len(fuzzy_matches) == 1 else {}


def notion_get_page(page_id):
    response = requests.get(
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=notion_api_headers(),
        timeout=30,
    )
    response.raise_for_status()
    return response.json() or {}


def fetch_all_notion_block_children(block_id):
    children = []
    next_cursor = ""
    while True:
        params = {"page_size": 100}
        if next_cursor:
            params["start_cursor"] = next_cursor
        response = requests.get(
            f"https://api.notion.com/v1/blocks/{block_id}/children",
            headers=notion_api_headers(),
            params=params,
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json() or {}
        batch = payload.get("results", []) or []
        children.extend(batch)
        if not payload.get("has_more"):
            break
        next_cursor = str(payload.get("next_cursor") or "").strip()
        if not next_cursor:
            break
    return children


def notion_enrich_block_children(blocks):
    enriched = []
    for block in list(blocks or []):
        block_copy = dict(block or {})
        if block_copy.get("has_children"):
            try:
                raw_children = fetch_all_notion_block_children(block_copy.get("id", ""))
            except Exception:
                raw_children = []
            block_copy["_children"] = notion_enrich_block_children(raw_children)
        else:
            block_copy["_children"] = []
        enriched.append(block_copy)
    return enriched


def notion_block_rich_text(block):
    block = block if isinstance(block, dict) else {}
    block_type = str(block.get("type") or "").strip()
    if not block_type:
        return []
    payload = block.get(block_type, {}) or {}
    return payload.get("rich_text", []) or []


def notion_block_plain_text(block):
    block = block if isinstance(block, dict) else {}
    parts = notion_block_rich_text(block)
    text = "".join(part.get("plain_text", "") for part in parts).strip()
    return re.sub(r"\s+", " ", text).strip()


def normalize_book_match_key(value):
    text = str(value or "").strip().lower()
    if not text:
        return ""
    text = re.sub(r"[\u064b-\u065f\u0670\u06d6-\u06ed]", "", text)
    text = text.replace("ـ", "")
    replacements = {
        "أ": "ا",
        "إ": "ا",
        "آ": "ا",
        "ٱ": "ا",
        "ؤ": "و",
        "ئ": "ي",
        "ى": "ي",
        "ة": "ه",
    }
    for source, target in replacements.items():
        text = text.replace(source, target)
    text = re.sub(r"^(?:كتاب|رواية|من كتاب|من رواية)\s*[:\-]\s*", "", text)
    text = re.sub(r"[\"'“”‘’«»(){}\[\].,;:!?/\\|_+=~`@#$%^&*<>-]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def compact_book_match_key(value):
    return re.sub(r"[^0-9a-z\u0600-\u06ff]+", "", normalize_book_match_key(value))


def normalize_quote_text(value):
    text = str(value or "").strip()
    if not text:
        return ""
    text = re.sub(r"\r\n?", "\n", text)
    text = text.replace("“", '"').replace("”", '"').replace("‘", "'").replace("’", "'")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def extract_quote_metadata(text):
    source = normalize_quote_text(text)
    if not source:
        return {"quote": "", "page": "", "chapter": "", "favorite": False}

    favorite = bool(re.match(r"^[⭐★♥❤]\s*", source))
    cleaned = re.sub(r"^[⭐★♥❤]\s*", "", source).strip()

    page_value = ""
    chapter_value = ""
    page_patterns = (
        r"(?:^|[,\-–—(])\s*(?:p(?:age)?|pg|ص(?:فحة)?|صفحه)\s*[:.]?\s*(\d{1,4}(?:\s*[-/]\s*\d{1,4})?)\s*(?:$|[)\],])",
        r"(?:^|[,\-–—(])\s*(\d{1,4}(?:\s*[-/]\s*\d{1,4})?)\s*(?:صفحة|ص)\s*(?:$|[)\],])",
        r"\s+(?:p(?:age)?|pg|ص(?:فحة)?|صفحه)\s*[:.]?\s*(\d{1,4}(?:\s*[-/]\s*\d{1,4})?)$",
    )
    for pattern in page_patterns:
        match = re.search(pattern, cleaned, flags=re.IGNORECASE)
        if match:
            page_value = re.sub(r"\s+", "", match.group(1))
            cleaned = (cleaned[:match.start()] + " " + cleaned[match.end():]).strip(" ,;-")
            break

    chapter_patterns = (
        r"(?:^|[,\-–—(])\s*(?:chapter|ch\.?|الفصل|فصل)\s*[:.]?\s*([^)]+?)\s*(?:$|[)\],])",
    )
    for pattern in chapter_patterns:
        match = re.search(pattern, cleaned, flags=re.IGNORECASE)
        if match:
            chapter_value = re.sub(r"\s+", " ", match.group(1)).strip(" ,;-")
            cleaned = (cleaned[:match.start()] + " " + cleaned[match.end():]).strip(" ,;-")
            break

    cleaned = cleaned.strip(' "\'«»')
    return {
        "quote": cleaned,
        "page": page_value,
        "chapter": chapter_value,
        "favorite": favorite,
    }


def extract_quote_candidates_from_block_text(text):
    source = str(text or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    if not source:
        return []
    chunks = [chunk.strip() for chunk in re.split(r"\n{2,}", source) if chunk.strip()]
    if not chunks:
        chunks = [source]
    results = []
    for chunk in chunks:
        metadata = extract_quote_metadata(chunk)
        if metadata.get("quote"):
            results.append(metadata)
    return results


def is_quotes_group_block(block):
    block_type = str((block or {}).get("type") or "").strip().lower()
    return block_type in {"heading_1", "heading_2", "heading_3", "toggle"}


def is_quote_content_block(block):
    block_type = str((block or {}).get("type") or "").strip().lower()
    return block_type in {"paragraph", "quote", "bulleted_list_item", "numbered_list_item", "callout"}


def parse_quotes_groups_from_blocks(blocks, active_group_title="", groups=None):
    groups = groups if isinstance(groups, list) else []
    current_group_title = active_group_title
    for block in list(blocks or []):
        block_text = notion_block_plain_text(block)
        if is_quotes_group_block(block) and block_text:
            current_group_title = block_text
            groups.append({
                "group_title": current_group_title,
                "quotes": [],
                "source_block_id": str(block.get("id") or "").strip(),
                "source_block_type": str(block.get("type") or "").strip(),
            })
            parse_quotes_groups_from_blocks(block.get("_children", []), current_group_title, groups)
            continue

        current_group = groups[-1] if groups and groups[-1].get("group_title") == current_group_title else None
        if current_group and is_quote_content_block(block) and block_text:
            current_group["quotes"].extend(extract_quote_candidates_from_block_text(block_text))

        if block.get("_children"):
            parse_quotes_groups_from_blocks(block.get("_children", []), current_group_title, groups)

    return groups


def quote_group_heading_parts(value):
    raw = re.sub(r"\s+", " ", str(value or "").strip())
    if not raw:
        return {"title": "", "author_hint": ""}
    for separator in (" - ", " — ", " – ", " by ", " لـ ", " ل "):
        if separator in raw:
            left, right = raw.split(separator, 1)
            return {"title": left.strip(), "author_hint": right.strip()}
    return {"title": raw, "author_hint": ""}


BOOK_QUOTE_MATCH_OVERRIDES = {
    normalize_book_match_key("أربعون"): {
        "book_titles": {normalize_book_match_key("اربعون 40")},
    },
}


def manual_book_quote_match_override(group_title, books):
    heading_parts = quote_group_heading_parts(group_title)
    normalized_title = normalize_book_match_key(heading_parts.get("title", ""))
    override = BOOK_QUOTE_MATCH_OVERRIDES.get(normalized_title)
    if not override:
        return None

    wanted_titles = {
        normalize_book_match_key(title)
        for title in list(override.get("book_titles", []) or [])
        if normalize_book_match_key(title)
    }
    candidates = [
        book for book in list(books or [])
        if normalize_book_match_key(book.get("title", "")) in wanted_titles
    ]
    if len(candidates) == 1:
        return {
            "status": "matched",
            "book": candidates[0],
            "candidates": [candidates[0]],
            "reason": "manual_override",
        }
    if len(candidates) > 1:
        return {
            "status": "ambiguous",
            "book": None,
            "candidates": candidates[:5],
            "reason": "manual_override_ambiguous",
        }
    return {"status": "unmatched", "book": None, "candidates": [], "reason": "manual_override_missing"}


def fetch_books_match_catalog(force_refresh=False):
    cache_age_seconds = 120
    cached_catalog = BOOK_QUOTES_IMPORT_CACHE.get("books")
    cached_updated_at = float(BOOK_QUOTES_IMPORT_CACHE.get("updated_at") or 0)
    if not force_refresh and cached_catalog is not None and (time.time() - cached_updated_at) < cache_age_seconds:
        return list(cached_catalog or [])
    fetched = fetch_books_entries(force_refresh=force_refresh)
    entries = list(fetched.get("entries", []) or [])
    BOOK_QUOTES_IMPORT_CACHE.update({"books": list(entries), "updated_at": time.time()})
    return entries


def match_quote_group_to_book(group_title, books):
    override_match = manual_book_quote_match_override(group_title, books)
    if override_match and override_match.get("status") != "unmatched":
        return override_match

    heading_parts = quote_group_heading_parts(group_title)
    title_hint = heading_parts.get("title", "")
    author_hint = heading_parts.get("author_hint", "")
    normalized_hint = normalize_book_match_key(title_hint)
    compact_hint = compact_book_match_key(title_hint)
    author_key = normalize_book_match_key(author_hint)
    if not normalized_hint:
        return {"status": "unmatched", "book": None, "candidates": []}

    scored = []
    for book in list(books or []):
        book_title = str(book.get("title") or "").strip()
        normalized_title = normalize_book_match_key(book_title)
        compact_title = compact_book_match_key(book_title)
        if not normalized_title:
            continue
        score = -1
        reason = ""
        if normalized_title == normalized_hint:
            score = 100
            reason = "exact_title"
        elif compact_title and compact_title == compact_hint:
            score = 95
            reason = "compact_title"
        elif normalized_hint in normalized_title or normalized_title in normalized_hint:
            shorter = min(len(normalized_title), len(normalized_hint))
            if shorter >= 5:
                score = 70
                reason = "contained_title"
        if score < 0:
            continue
        author_blob = normalize_book_match_key(book.get("authors_display", ""))
        if author_key and author_blob:
            if author_key == author_blob or author_key in author_blob or author_blob in author_key:
                score += 12
                reason += "+author"
        scored.append({"book": book, "score": score, "reason": reason})

    if not scored:
        return {"status": "unmatched", "book": None, "candidates": []}

    scored.sort(key=lambda item: (item["score"], item["book"].get("title", "")), reverse=True)
    top_score = scored[0]["score"]
    top_candidates = [item for item in scored if item["score"] == top_score]
    if len(top_candidates) > 1 and top_score < 100:
        return {
            "status": "ambiguous",
            "book": None,
            "candidates": [item["book"] for item in top_candidates[:5]],
        }
    return {
        "status": "matched",
        "book": top_candidates[0]["book"],
        "candidates": [top_candidates[0]["book"]],
    }


def notion_database_property_key(properties, candidate_names, allowed_types=None):
    props = properties or {}
    allowed = {str(item).strip().lower() for item in (allowed_types or []) if str(item).strip()}
    lowered = {str(name).strip().lower(): name for name in props.keys()}
    for candidate in list(candidate_names or []):
        found_key = lowered.get(str(candidate).strip().lower())
        if not found_key:
            continue
        prop = props.get(found_key, {}) or {}
        prop_type = str(prop.get("type") or "").strip().lower()
        if allowed and prop_type not in allowed:
            continue
        return found_key
    return ""


def books_quotes_property_name(database_payload):
    properties = (database_payload or {}).get("properties", {}) or {}
    return notion_database_property_key(properties, ("Book Quotes", "Quotes", "Quote Notes"), ("rich_text",))


def books_quotes_property_schema(database_payload):
    properties = (database_payload or {}).get("properties", {}) or {}
    property_name = books_quotes_property_name(database_payload)
    if not property_name:
        return "", {}
    return property_name, properties.get(property_name, {}) or {}


def normalize_quotes_text_block(value):
    text = normalize_quote_text(value)
    if not text:
        return ""
    text = re.sub(r"\s*[\u2022\-\*]\s*", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def split_existing_book_quotes(value):
    raw = str(value or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    if not raw:
        return []
    parts = [part.strip() for part in re.split(r"\n{2,}", raw) if part.strip()]
    return [normalize_quotes_text_block(part) for part in parts if normalize_quotes_text_block(part)]


def format_book_quotes_text(quotes):
    formatted_quotes = []
    for item in list(quotes or []):
        quote_text = normalize_quote_text(item.get("quote", ""))
        if not quote_text:
            continue
        line = f"• {quote_text}"
        page = str(item.get("page", "") or "").strip()
        chapter = str(item.get("chapter", "") or "").strip()
        details = []
        if page:
            details.append(f"page {page}")
        if chapter:
            details.append(f"chapter {chapter}")
        if details:
            line = f"{line} ({'; '.join(details)})"
        formatted_quotes.append(line)
    return "\n\n".join(formatted_quotes).strip()


def merge_book_quotes_text(existing_text, new_quotes_text):
    existing = str(existing_text or "").strip()
    new_text = str(new_quotes_text or "").strip()
    if existing and new_text:
        return f"{existing}\n\n{new_text}"
    return existing or new_text


def fetch_existing_book_quote_signatures(book_pages, book_quotes_key):
    existing = set()
    for book in list(book_pages or []):
        book_id = compact_notion_id(book.get("notion_page_id", ""))
        if not book_id:
            continue
        props = book.get("properties", {}) or {}
        text_value = notion_property_text_value(props.get(book_quotes_key, {})) if book_quotes_key else ""
        for quote_text in split_existing_book_quotes(text_value):
            existing.add((book_id, quote_text))
    return existing


def update_book_quote_text_on_page(page_id, property_name, combined_text):
    if not property_name:
        raise RuntimeError("The books database does not have a usable Book Quotes property.")
    payload = {
        property_name: {
            "rich_text": notion_rich_text(combined_text)
        }
    }
    return update_notion_page_properties(page_id, payload)


def resolve_book_quotes_source_page(source_page_id="", source_page_title=""):
    page_id = compact_notion_id(source_page_id)
    if page_id:
        return notion_get_page(page_id)
    return notion_search_page_by_title(source_page_title)


def build_book_quotes_import_report(
    source_page,
    groups,
    matched_books,
    updated_books,
    skipped_duplicates,
    unmatched_groups,
    ambiguous_groups,
    blocked_books,
    dry_run=False,
):
    return {
        "ok": True,
        "dry_run": bool(dry_run),
        "source_page_id": compact_notion_id((source_page or {}).get("id", "")),
        "source_page_title": notion_page_title_text(source_page),
        "group_count": len(list(groups or [])),
        "matched_group_count": len(list(matched_books or [])),
        "updated_books_count": len(list(updated_books or [])),
        "skipped_duplicates_count": len(list(skipped_duplicates or [])),
        "unmatched_group_count": len(list(unmatched_groups or [])),
        "ambiguous_group_count": len(list(ambiguous_groups or [])),
        "matched_books": list(matched_books or []),
        "updated_books": list(updated_books or []),
        "skipped_duplicates": list(skipped_duplicates or []),
        "unmatched_book_groups": list(unmatched_groups or []),
        "ambiguous_matches": list(ambiguous_groups or []),
        "blocked_books": list(blocked_books or []),
    }


def import_book_quotes_from_notion(source_page_id="", source_page_title="", dry_run=False):
    if not (NOTION_TOKEN or "").strip():
        raise RuntimeError("Missing NOTION_TOKEN.")

    source_page = resolve_book_quotes_source_page(
        source_page_id=source_page_id or NOTION_BOOK_QUOTES_SOURCE_PAGE_ID,
        source_page_title=source_page_title or NOTION_BOOK_QUOTES_SOURCE_PAGE_TITLE,
    )
    if not source_page:
        raise RuntimeError("Could not find the source quotes page. Set NOTION_BOOK_QUOTES_SOURCE_PAGE_ID or share the page with the integration.")

    source_page_id_value = compact_notion_id(source_page.get("id", ""))
    raw_blocks = fetch_all_notion_block_children(source_page_id_value)
    blocks = notion_enrich_block_children(raw_blocks)
    groups = [group for group in parse_quotes_groups_from_blocks(blocks) if group.get("group_title") and group.get("quotes")]

    books = fetch_books_match_catalog(force_refresh=True)
    books_database = retrieve_notion_database(NOTION_BOOKS_DATABASE_ID)
    book_quotes_property_name_value, book_quotes_property_schema_value = books_quotes_property_schema(books_database)
    if not book_quotes_property_name_value:
        raise RuntimeError("The books database does not contain a usable Book Quotes property.")
    if str((book_quotes_property_schema_value or {}).get("type") or "").strip().lower() != "rich_text":
        raise RuntimeError("The Book Quotes property must be a rich_text field.")

    raw_book_pages = fetch_all_notion_database_pages(database_id=NOTION_BOOKS_DATABASE_ID)
    books_by_page_id = {compact_notion_id(page.get("id", "")): page for page in raw_book_pages}
    existing_signatures = fetch_existing_book_quote_signatures(raw_book_pages, book_quotes_property_name_value)
    seen_signatures = set(existing_signatures)
    matched_groups = []
    unmatched_groups = []
    ambiguous_groups = []
    skipped_duplicates = []
    blocked_books = []
    grouped_quotes_by_book = {}
    grouped_reports_by_book = {}

    for group in groups:
        group_title = group.get("group_title", "")
        match_result = match_quote_group_to_book(group_title, books)
        if match_result.get("status") == "unmatched":
            unmatched_groups.append({
                "group_title": group_title,
                "quote_count": len(group.get("quotes", [])),
            })
            continue
        if match_result.get("status") == "ambiguous":
            ambiguous_groups.append({
                "group_title": group_title,
                "quote_count": len(group.get("quotes", [])),
                "candidates": [
                    {
                        "title": candidate.get("title", ""),
                        "authors": candidate.get("authors_display", ""),
                        "book_id": candidate.get("notion_page_id", ""),
                    }
                    for candidate in match_result.get("candidates", [])
                ],
            })
            continue

        matched_book = match_result.get("book") or {}
        book_id = compact_notion_id(matched_book.get("notion_page_id", ""))
        matched_groups.append({
            "group_title": group_title,
            "book_title": matched_book.get("title", ""),
            "book_id": book_id,
            "quote_count": len(group.get("quotes", [])),
        })
        if not book_id:
            blocked_books.append({
                "group_title": group_title,
                "book_title": matched_book.get("title", ""),
                "reason": "missing_book_page_id",
            })
            continue
        raw_book_page = books_by_page_id.get(book_id, {})
        if not raw_book_page:
            blocked_books.append({
                "group_title": group_title,
                "book_title": matched_book.get("title", ""),
                "reason": "book_page_not_found",
            })
            continue
        existing_text = notion_property_text_value((raw_book_page.get("properties", {}) or {}).get(book_quotes_property_name_value, {}))
        normalized_existing_quotes = set(split_existing_book_quotes(existing_text))
        new_quotes_for_book = []
        duplicate_quotes_for_book = []
        for quote_item in list(group.get("quotes", []) or []):
            normalized_quote = normalize_quote_text(quote_item.get("quote", ""))
            if not normalized_quote:
                continue
            signature = (book_id, normalize_quotes_text_block(normalized_quote))
            if signature in seen_signatures or normalize_quotes_text_block(normalized_quote) in normalized_existing_quotes:
                duplicate_quotes_for_book.append({
                    "group_title": group_title,
                    "book_title": matched_book.get("title", ""),
                    "quote": normalized_quote[:240],
                })
                continue
            seen_signatures.add(signature)
            normalized_existing_quotes.add(normalize_quotes_text_block(normalized_quote))
            new_quotes_for_book.append(quote_item)
        if duplicate_quotes_for_book:
            skipped_duplicates.extend(duplicate_quotes_for_book)
        if not new_quotes_for_book:
            continue
        grouped_quotes_by_book.setdefault(book_id, []).extend(new_quotes_for_book)
        grouped_reports_by_book.setdefault(book_id, {
            "book_title": matched_book.get("title", ""),
            "book_id": book_id,
            "group_titles": [],
            "new_quote_count": 0,
            "existing_content_present": bool(str(existing_text or "").strip()),
            "existing_content_length": len(str(existing_text or "").strip()),
            "property_name": book_quotes_property_name_value,
        })
        grouped_reports_by_book[book_id]["group_titles"].append(group_title)
        grouped_reports_by_book[book_id]["new_quote_count"] += len(new_quotes_for_book)

    updated_books = []
    for book_id, quote_items in grouped_quotes_by_book.items():
        book_page = books_by_page_id.get(book_id, {})
        if not book_page:
            blocked_books.append({
                "book_id": book_id,
                "reason": "book_page_not_found",
                "quote_count": len(quote_items),
            })
            continue
        existing_text = notion_property_text_value((book_page.get("properties", {}) or {}).get(book_quotes_property_name_value, {}))
        combined_new_text = format_book_quotes_text(quote_items)
        merged_text = merge_book_quotes_text(existing_text, combined_new_text)
        if dry_run:
            updated_books.append({
                **grouped_reports_by_book.get(book_id, {}),
                "would_update": True,
                "resulting_length": len(merged_text),
            })
            continue
        try:
            update_book_quote_text_on_page(book_id, book_quotes_property_name_value, merged_text)
        except requests.RequestException as exc:
            blocked_books.append({
                **grouped_reports_by_book.get(book_id, {}),
                "reason": f"notion_update_error: {exc}",
            })
            continue
        updated_books.append({
            **grouped_reports_by_book.get(book_id, {}),
            "updated": True,
            "resulting_length": len(merged_text),
        })

    return build_book_quotes_import_report(
        source_page=source_page,
        groups=groups,
        matched_books=matched_groups,
        updated_books=updated_books,
        skipped_duplicates=skipped_duplicates,
        unmatched_groups=unmatched_groups,
        ambiguous_groups=ambiguous_groups,
        blocked_books=blocked_books,
        dry_run=dry_run,
    )


def split_quotes_source_lines(raw_text):
    raw = str(raw_text or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    if not raw:
        return []
    lines = [line.rstrip() for line in raw.split("\n")]
    entries = []
    buffer = []
    bullet_pattern = re.compile(r"^\s*[\u2022\-\*](?:\s+|$)")

    for line in lines:
        if not line.strip():
            if buffer:
                entry = "\n".join(buffer).strip()
                if entry:
                    entries.append(entry)
                buffer = []
            continue
        if bullet_pattern.match(line) and buffer:
            entry = "\n".join(buffer).strip()
            if entry:
                entries.append(entry)
            buffer = [line]
            continue
        buffer.append(line)

    if buffer:
        entry = "\n".join(buffer).strip()
        if entry:
            entries.append(entry)
    return entries


def parse_quotes_from_books_rich_text(value):
    entries = split_quotes_source_lines(value)
    extracted = []
    failures = []
    for entry in entries:
        normalized_entry = re.sub(r"^\s*[\u2022\-\*](?:\s+|$)", "", str(entry or "").strip())
        if not normalized_entry.strip():
            failures.append({
                "raw_excerpt": str(entry or "")[:240],
                "reason": "empty_bullet_entry",
            })
            continue
        metadata = extract_quote_metadata(normalized_entry)
        if metadata.get("quote"):
            extracted.append(metadata)
            continue
        failures.append({
            "raw_excerpt": normalized_entry[:240],
            "reason": "empty_quote_after_parsing",
        })
    return {
        "entries": extracted,
        "failures": failures,
    }


def resolve_book_quotes_database(database_id=""):
    configured_id = compact_notion_id(database_id or NOTION_BOOK_QUOTES_DATABASE_ID)
    if not configured_id:
        return {}
    return retrieve_notion_database(configured_id)


def book_quotes_database_quote_property_name(database_payload):
    properties = (database_payload or {}).get("properties", {}) or {}
    return notion_database_property_key(properties, ("Quote", "Name", "Title"), ("title",))


def book_quotes_database_book_property_name(database_payload):
    properties = (database_payload or {}).get("properties", {}) or {}
    return notion_database_property_key(properties, ("Book", "Books"), ("relation",))


def book_quotes_database_author_property_name(database_payload):
    properties = (database_payload or {}).get("properties", {}) or {}
    return notion_database_property_key(properties, ("Author", "Authors"), ("rich_text", "title", "select", "multi_select"))


def book_quotes_database_page_property_name(database_payload):
    properties = (database_payload or {}).get("properties", {}) or {}
    return notion_database_property_key(properties, ("Page", "Page Number"), ("rich_text", "number", "select"))


def book_quotes_database_chapter_property_name(database_payload):
    properties = (database_payload or {}).get("properties", {}) or {}
    return notion_database_property_key(properties, ("Chapter",), ("rich_text", "select"))


def book_quotes_database_favorite_property_name(database_payload):
    properties = (database_payload or {}).get("properties", {}) or {}
    return notion_database_property_key(properties, ("Favorite", "Favourite", "Starred"), ("checkbox", "select"))


def book_quotes_database_tags_property_name(database_payload):
    properties = (database_payload or {}).get("properties", {}) or {}
    return notion_database_property_key(properties, ("Tags", "Tag"), ("multi_select",))


def build_book_quotes_database_schema(database_payload):
    if not database_payload:
        return {}
    properties = (database_payload or {}).get("properties", {}) or {}
    return {
        "database_id": compact_notion_id(database_payload.get("id", "")),
        "database_title": notion_database_title_text(database_payload),
        "quote_property": book_quotes_database_quote_property_name(database_payload),
        "book_property": book_quotes_database_book_property_name(database_payload),
        "author_property": book_quotes_database_author_property_name(database_payload),
        "page_property": book_quotes_database_page_property_name(database_payload),
        "chapter_property": book_quotes_database_chapter_property_name(database_payload),
        "favorite_property": book_quotes_database_favorite_property_name(database_payload),
        "tags_property": book_quotes_database_tags_property_name(database_payload),
        "properties": properties,
    }


def validate_book_quotes_database_schema(schema):
    blockers = []
    if not schema:
        blockers.append("Missing NOTION_BOOK_QUOTES_DATABASE_ID or a resolvable Book Quotes database.")
        return blockers

    quote_property = schema.get("quote_property", "")
    book_property = schema.get("book_property", "")
    properties = schema.get("properties", {}) or {}

    if not quote_property:
        blockers.append("The Book Quotes database does not have a usable Quote property.")
    if not book_property:
        blockers.append("The Book Quotes database does not have a usable Book relation property.")

    relation_schema = properties.get(book_property, {}) if book_property else {}
    relation_database_id = compact_notion_id(((relation_schema.get("relation") or {}).get("database_id") or ""))
    expected_books_database_id = compact_notion_id(NOTION_BOOKS_DATABASE_ID)
    if relation_database_id and expected_books_database_id and relation_database_id != expected_books_database_id:
        blockers.append("The Book relation property is not pointed at the current books database.")

    return blockers


def inspect_book_quotes_migration_readiness(database_id=""):
    target_configured_id = compact_notion_id(database_id or NOTION_BOOK_QUOTES_DATABASE_ID)
    target_database = {}
    target_database_error = ""
    if target_configured_id:
        try:
            target_database = retrieve_notion_database(target_configured_id)
        except requests.RequestException as exc:
            target_database_error = str(exc)

    books_database = {}
    books_database_error = ""
    if compact_notion_id(NOTION_BOOKS_DATABASE_ID):
        try:
            books_database = retrieve_notion_database(NOTION_BOOKS_DATABASE_ID)
        except requests.RequestException as exc:
            books_database_error = str(exc)

    source_property_name = ""
    source_property_schema = {}
    source_property_usable = False
    if books_database:
        source_property_name, source_property_schema = books_quotes_property_schema(books_database)
        source_property_usable = bool(source_property_name and str((source_property_schema or {}).get("type") or "").strip().lower() == "rich_text")

    target_schema = build_book_quotes_database_schema(target_database)
    target_blockers = validate_book_quotes_database_schema(target_schema)
    target_quote_property = target_schema.get("quote_property", "")
    target_book_property = target_schema.get("book_property", "")
    target_quote_property_usable = bool(target_quote_property and str((target_schema.get("properties", {}) or {}).get(target_quote_property, {}).get("type") or "").strip().lower() == "title")
    target_book_property_usable = bool(target_book_property and str((target_schema.get("properties", {}) or {}).get(target_book_property, {}).get("type") or "").strip().lower() == "relation")

    blockers = []
    if not target_configured_id:
        blockers.append("Missing NOTION_BOOK_QUOTES_DATABASE_ID.")
    if target_configured_id and not target_database:
        blockers.append("The Book Quotes database could not be reached.")
    if target_database_error:
        blockers.append(f"The Book Quotes database request failed: {target_database_error}")
    if books_database_error:
        blockers.append(f"The Book-List-Database request failed: {books_database_error}")
    if target_configured_id:
        blockers.extend(target_blockers)
    if not source_property_usable:
        blockers.append("The source Book Quotes rich-text field is missing or unusable in Book-List-Database.")
    blockers = list(dict.fromkeys(blockers))

    optional_fields = {
        "author_property": target_schema.get("author_property", ""),
        "page_property": target_schema.get("page_property", ""),
        "chapter_property": target_schema.get("chapter_property", ""),
        "favorite_property": target_schema.get("favorite_property", ""),
        "tags_property": target_schema.get("tags_property", ""),
    }

    return {
        "ok": not blockers,
        "target_database_configured": bool(target_configured_id),
        "target_database_reachable": bool(target_database),
        "target_database_id": target_schema.get("database_id", ""),
        "target_database_title": target_schema.get("database_title", ""),
        "target_quote_property_name": target_quote_property,
        "target_quote_property_usable": target_quote_property_usable,
        "target_book_property_name": target_book_property,
        "target_book_property_usable": target_book_property_usable,
        "target_relation_points_to_books_database": bool(
            compact_notion_id(((target_schema.get("properties", {}) or {}).get(target_book_property, {}) or {}).get("relation", {}).get("database_id", ""))
            == compact_notion_id(NOTION_BOOKS_DATABASE_ID)
        ) if target_book_property else False,
        "source_books_database_reachable": bool(books_database),
        "source_quote_property_name": source_property_name,
        "source_quote_property_usable": source_property_usable,
        "optional_fields": optional_fields,
        "blockers": blockers,
    }


def build_notion_property_payload(property_schema, value):
    schema = property_schema if isinstance(property_schema, dict) else {}
    property_type = str(schema.get("type") or "").strip().lower()
    if property_type == "title":
        return {"title": notion_title_text(value)}
    if property_type == "rich_text":
        return {"rich_text": notion_rich_text(value)}
    if property_type == "relation":
        relation_ids = [str(item).strip() for item in list(value or []) if str(item).strip()]
        return {"relation": [{"id": relation_id} for relation_id in relation_ids]}
    if property_type == "checkbox":
        return {"checkbox": bool(value)}
    if property_type == "number":
        text = str(value or "").strip()
        if not text:
            return None
        number_match = re.search(r"\d+(?:\.\d+)?", text)
        if not number_match:
            return None
        number_value = number_match.group(0)
        return {"number": float(number_value) if "." in number_value else int(number_value)}
    if property_type == "select":
        text = str(value or "").strip()
        if not text:
            return None
        return {"select": {"name": text[:100]}}
    if property_type == "multi_select":
        items = [str(item).strip() for item in list(value or []) if str(item).strip()]
        if not items:
            return None
        return {"multi_select": [{"name": item[:100]} for item in items]}
    return None


def fetch_existing_structured_book_quote_signatures(database_id, schema):
    signatures = set()
    rows = fetch_all_notion_database_pages(database_id=database_id)
    quote_property = schema.get("quote_property", "")
    book_property = schema.get("book_property", "")

    for row in list(rows or []):
        properties = row.get("properties", {}) or {}
        quote_text = notion_property_text_value(properties.get(quote_property, {}))
        if not quote_text:
            continue
        quote_key = normalize_quotes_text_block(quote_text)
        if not quote_key:
            continue
        for relation_id in extract_notion_relation_ids(properties, book_property):
            book_id = compact_notion_id(relation_id)
            if book_id:
                signatures.add((book_id, quote_key))
    return signatures


def build_book_quote_database_row_properties(schema, quote_entry, book_entry):
    properties = schema.get("properties", {}) or {}
    payload = {}

    quote_property_name = schema.get("quote_property", "")
    book_property_name = schema.get("book_property", "")
    author_property_name = schema.get("author_property", "")
    page_property_name = schema.get("page_property", "")
    chapter_property_name = schema.get("chapter_property", "")
    favorite_property_name = schema.get("favorite_property", "")

    if quote_property_name:
        property_payload = build_notion_property_payload(
            properties.get(quote_property_name, {}),
            quote_entry.get("quote", ""),
        )
        if property_payload:
            payload[quote_property_name] = property_payload

    if book_property_name:
        property_payload = build_notion_property_payload(
            properties.get(book_property_name, {}),
            [compact_notion_id(book_entry.get("notion_page_id", ""))],
        )
        if property_payload:
            payload[book_property_name] = property_payload

    if author_property_name:
        author_value = book_entry.get("authors_display", "")
        schema_type = str((properties.get(author_property_name, {}) or {}).get("type") or "").strip().lower()
        property_value = split_director_names(author_value) if schema_type == "multi_select" else author_value
        property_payload = build_notion_property_payload(properties.get(author_property_name, {}), property_value)
        if property_payload:
            payload[author_property_name] = property_payload

    if page_property_name and quote_entry.get("page"):
        property_payload = build_notion_property_payload(
            properties.get(page_property_name, {}),
            quote_entry.get("page", ""),
        )
        if property_payload:
            payload[page_property_name] = property_payload

    if chapter_property_name and quote_entry.get("chapter"):
        property_payload = build_notion_property_payload(
            properties.get(chapter_property_name, {}),
            quote_entry.get("chapter", ""),
        )
        if property_payload:
            payload[chapter_property_name] = property_payload

    if favorite_property_name:
        property_payload = build_notion_property_payload(
            properties.get(favorite_property_name, {}),
            bool(quote_entry.get("favorite")),
        )
        if property_payload:
            payload[favorite_property_name] = property_payload

    return payload


def create_notion_database_page(database_id, properties_payload):
    response = requests.post(
        "https://api.notion.com/v1/pages",
        headers=notion_api_headers(),
        json={
            "parent": {"database_id": database_id},
            "properties": properties_payload,
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json() or {}


def build_book_quotes_migration_report(
    schema,
    books_scanned,
    books_with_quote_content,
    extracted_quotes,
    duplicates,
    rows_to_create,
    parsing_failures,
    blocked_books,
    created_rows,
    dry_run=False,
):
    return {
        "ok": True,
        "dry_run": bool(dry_run),
        "target_database_id": schema.get("database_id", "") if isinstance(schema, dict) else "",
        "target_database_title": schema.get("database_title", "") if isinstance(schema, dict) else "",
        "books_scanned": int(books_scanned or 0),
        "books_with_quote_content": int(books_with_quote_content or 0),
        "extracted_quote_count": len(list(extracted_quotes or [])),
        "duplicates_count": len(list(duplicates or [])),
        "rows_to_create_count": len(list(rows_to_create or [])),
        "created_rows_count": len(list(created_rows or [])),
        "parsing_failure_count": len(list(parsing_failures or [])),
        "blockers": list(blocked_books or []),
        "rows_to_create": list(rows_to_create or []),
        "created_rows": list(created_rows or []),
        "duplicates": list(duplicates or []),
        "parsing_failures": list(parsing_failures or []),
    }


def migrate_book_quotes_rich_text_to_database(dry_run=True, database_id=""):
    if not (NOTION_TOKEN or "").strip():
        raise RuntimeError("Missing NOTION_TOKEN.")
    if not (NOTION_BOOKS_DATABASE_ID or "").strip():
        raise RuntimeError("Missing NOTION_BOOKS_DATABASE_ID.")

    books_database = retrieve_notion_database(NOTION_BOOKS_DATABASE_ID)
    book_quotes_property_name_value, book_quotes_property_schema_value = books_quotes_property_schema(books_database)
    if not book_quotes_property_name_value:
        raise RuntimeError("The books database does not contain a usable Book Quotes rich-text property.")
    if str((book_quotes_property_schema_value or {}).get("type") or "").strip().lower() != "rich_text":
        raise RuntimeError("The books database Book Quotes property must be rich_text.")

    quotes_database = resolve_book_quotes_database(database_id=database_id)
    schema = build_book_quotes_database_schema(quotes_database)
    schema_blockers = validate_book_quotes_database_schema(schema)
    if schema_blockers and not dry_run:
        raise RuntimeError("; ".join(schema_blockers))

    books_payload = fetch_books_entries(force_refresh=True)
    books = list(books_payload.get("entries", []) or [])
    raw_book_pages = fetch_all_notion_database_pages(database_id=NOTION_BOOKS_DATABASE_ID)
    raw_books_by_page_id = {compact_notion_id(page.get("id", "")): page for page in raw_book_pages}

    existing_signatures = set()
    if schema and schema.get("database_id") and not schema_blockers:
        existing_signatures = fetch_existing_structured_book_quote_signatures(schema.get("database_id", ""), schema)

    books_scanned = 0
    books_with_quote_content = 0
    extracted_quotes = []
    duplicates = []
    rows_to_create = []
    parsing_failures = []
    created_rows = []
    seen_signatures = set(existing_signatures)

    for book in books:
        books_scanned += 1
        book_id = compact_notion_id(book.get("notion_page_id", ""))
        raw_book_page = raw_books_by_page_id.get(book_id, {})
        properties = raw_book_page.get("properties", {}) or {}
        source_text = notion_property_text_value(properties.get(book_quotes_property_name_value, {}))
        if not str(source_text or "").strip():
            continue
        books_with_quote_content += 1

        parsed = parse_quotes_from_books_rich_text(source_text)
        for failure in list(parsed.get("failures", []) or []):
            parsing_failures.append({
                "book_title": book.get("title", ""),
                "book_id": book_id,
                **failure,
            })

        for quote_entry in list(parsed.get("entries", []) or []):
            quote_text = normalize_quote_text(quote_entry.get("quote", ""))
            if not quote_text:
                parsing_failures.append({
                    "book_title": book.get("title", ""),
                    "book_id": book_id,
                    "raw_excerpt": str(quote_entry)[:240],
                    "reason": "missing_quote_text",
                })
                continue

            quote_record = {
                "book_title": book.get("title", ""),
                "book_id": book_id,
                "quote": quote_text,
                "page": str(quote_entry.get("page", "") or "").strip(),
                "chapter": str(quote_entry.get("chapter", "") or "").strip(),
                "favorite": bool(quote_entry.get("favorite")),
            }
            extracted_quotes.append(quote_record)
            signature = (book_id, normalize_quotes_text_block(quote_text))
            if signature in seen_signatures:
                duplicates.append(quote_record)
                continue
            seen_signatures.add(signature)
            rows_to_create.append(quote_record)

    if not dry_run and schema and schema.get("database_id") and not schema_blockers:
        for quote_record in rows_to_create:
            book_entry = next(
                (item for item in books if compact_notion_id(item.get("notion_page_id", "")) == quote_record.get("book_id", "")),
                {},
            )
            properties_payload = build_book_quote_database_row_properties(schema, quote_record, book_entry)
            if not properties_payload:
                parsing_failures.append({
                    "book_title": quote_record.get("book_title", ""),
                    "book_id": quote_record.get("book_id", ""),
                    "raw_excerpt": quote_record.get("quote", "")[:240],
                    "reason": "empty_properties_payload",
                })
                continue
            create_notion_database_page(schema.get("database_id", ""), properties_payload)
            created_rows.append(quote_record)

    return build_book_quotes_migration_report(
        schema=schema,
        books_scanned=books_scanned,
        books_with_quote_content=books_with_quote_content,
        extracted_quotes=extracted_quotes,
        duplicates=duplicates,
        rows_to_create=rows_to_create,
        parsing_failures=parsing_failures,
        blocked_books=schema_blockers,
        created_rows=created_rows,
        dry_run=dry_run,
    )


def retrieve_notion_database(database_id):
    response = requests.get(
        f"https://api.notion.com/v1/databases/{database_id}",
        headers=notion_api_headers(),
        timeout=30
    )
    response.raise_for_status()
    return response.json() or {}


def notion_search_databases(query):
    payload = {
        "query": query,
        "filter": {
            "property": "object",
            "value": "database"
        },
        "page_size": 20
    }
    response = requests.post(
        "https://api.notion.com/v1/search",
        headers=notion_api_headers(),
        json=payload,
        timeout=30
    )
    response.raise_for_status()
    return (response.json() or {}).get("results", [])


def notion_search_pages(query):
    payload = {
        "query": query,
        "filter": {
            "property": "object",
            "value": "page"
        },
        "page_size": 20
    }
    response = requests.post(
        "https://api.notion.com/v1/search",
        headers=notion_api_headers(),
        json=payload,
        timeout=30
    )
    response.raise_for_status()
    return (response.json() or {}).get("results", [])


def find_directors_parent_page():
    exact_matches = []
    for item in notion_search_pages(DIRECTORS_PARENT_PAGE_TITLE):
        properties = item.get("properties", {}) or {}
        title = extract_notion_page_title(properties)
        if title == DIRECTORS_PARENT_PAGE_TITLE:
            exact_matches.append(item)
    return exact_matches[0] if exact_matches else None


def create_directors_parent_page():
    payload = {
        "parent": {"type": "workspace", "workspace": True},
        "properties": {
            "title": {"title": notion_title_text(DIRECTORS_PARENT_PAGE_TITLE)}
        }
    }
    response = requests.post(
        "https://api.notion.com/v1/pages",
        headers=notion_api_headers(),
        json=payload,
        timeout=30
    )
    response.raise_for_status()
    return response.json() or {}


def ensure_directors_parent_page():
    preferred_id = str(NOTION_DIRECTORS_PARENT_PAGE_ID or "").strip()
    if preferred_id:
        return {"id": preferred_id}
    existing = find_directors_parent_page()
    if existing:
        return existing
    raise RuntimeError(
        "Missing a shared parent page for the Directors database. "
        "Share a Notion page with the integration and set NOTION_DIRECTORS_PARENT_PAGE_ID in .env, "
        f'or create/share a page titled "{DIRECTORS_PARENT_PAGE_TITLE}" first.'
    )


def find_directors_database():
    preferred_id = str(NOTION_DIRECTORS_DATABASE_ID or "").strip()
    if preferred_id:
        try:
            database = retrieve_notion_database(preferred_id)
        except requests.RequestException:
            database = None
        if database:
            return database

    exact_matches = []
    for item in notion_search_databases(DIRECTORS_DATABASE_TITLE):
        title = notion_database_title_text(item)
        if title == DIRECTORS_DATABASE_TITLE:
            exact_matches.append(item)
    if len(exact_matches) == 1:
        return exact_matches[0]
    if exact_matches:
        for item in exact_matches:
            properties = item.get("properties", {}) or {}
            if "Name" in properties and DIRECTOR_KEY_PROPERTY in properties:
                return item
        return exact_matches[0]
    return None


def create_directors_database():
    parent_page = ensure_directors_parent_page()
    payload = {
        "parent": {"type": "page_id", "page_id": parent_page.get("id", "")},
        "title": notion_title_text(DIRECTORS_DATABASE_TITLE),
        "properties": {
            "Name": {"title": {}},
            DIRECTOR_KEY_PROPERTY: {"rich_text": {}},
            DIRECTOR_ALIASES_PROPERTY: {"rich_text": {}},
        }
    }
    response = requests.post(
        "https://api.notion.com/v1/databases",
        headers=notion_api_headers(),
        json=payload,
        timeout=60
    )
    response.raise_for_status()
    return response.json() or {}


def ensure_directors_database():
    existing = find_directors_database()
    if existing:
        return existing
    return create_directors_database()


def ensure_movie_director_relation_property(directors_database_id):
    movie_database = retrieve_notion_database(NOTION_DATABASE_ID)
    properties = (movie_database or {}).get("properties", {}) or {}
    if DIRECTOR_RELATION_PROPERTY in properties:
        return properties
    response = requests.patch(
        f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}",
        headers=notion_api_headers(),
        json={
            "properties": {
                DIRECTOR_RELATION_PROPERTY: {
                    "relation": {
                        "database_id": directors_database_id,
                        "type": "single_property",
                        "single_property": {},
                    }
                }
            }
        },
        timeout=60
    )
    response.raise_for_status()
    return (response.json() or {}).get("properties", properties)


def ensure_directors_database_properties(directors_database_id):
    database = retrieve_notion_database(directors_database_id)
    properties = (database or {}).get("properties", {}) or {}
    desired = {}
    if DIRECTOR_IMAGE_PROPERTY not in properties:
        desired[DIRECTOR_IMAGE_PROPERTY] = {"files": {}}
    if DIRECTOR_TMDB_PERSON_ID_PROPERTY not in properties:
        desired[DIRECTOR_TMDB_PERSON_ID_PROPERTY] = {"number": {"format": "number"}}
    if not desired:
        return properties
    response = requests.patch(
        f"https://api.notion.com/v1/databases/{directors_database_id}",
        headers=notion_api_headers(),
        json={"properties": desired},
        timeout=60
    )
    response.raise_for_status()
    return (response.json() or {}).get("properties", properties)


def build_director_page_record(page):
    properties = page.get("properties", {}) or {}
    display_name = extract_notion_page_title(properties)
    key = extract_notion_rich_text_value(properties, DIRECTOR_KEY_PROPERTY) or normalized_person_key(display_name)
    aliases = extract_notion_rich_text_value(properties, DIRECTOR_ALIASES_PROPERTY)
    alias_values = [item.strip() for item in aliases.split("|") if item.strip()]
    if display_name and display_name not in alias_values:
        alias_values.append(display_name)
    return {
        "page_id": page.get("id", ""),
        "display_name": display_name,
        "director_key": key,
        "aliases": alias_values,
        "image_url": notion_files_first_url(properties.get(DIRECTOR_IMAGE_PROPERTY)),
        "tmdb_person_id": notion_number_value(properties.get(DIRECTOR_TMDB_PERSON_ID_PROPERTY)),
        "page_url": page.get("url", ""),
        "created_time": page.get("created_time", ""),
        "page": page,
    }


def fetch_director_page_records(directors_database_id):
    records = []
    for page in fetch_all_notion_database_pages(directors_database_id):
        record = build_director_page_record(page)
        if record["page_id"] and record["director_key"]:
            records.append(record)
    return records


def build_director_catalog():
    directors_database = find_directors_database()
    if not directors_database:
        return {
            "database": None,
            "records": [],
            "records_by_page_id": {},
            "canonical_by_key": {},
            "duplicates_by_key": {},
        }

    records = fetch_director_page_records(directors_database.get("id"))
    records_by_page_id = {record["page_id"]: record for record in records if record.get("page_id")}
    grouped = {}
    for record in records:
        grouped.setdefault(record["director_key"], []).append(record)

    canonical_by_key = {}
    duplicates_by_key = {}
    for director_key, items in grouped.items():
        ordered = sorted(
            items,
            key=lambda item: (
                item.get("created_time", ""),
                item.get("page_id", "")
            )
        )
        canonical_by_key[director_key] = ordered[0]
        if len(ordered) > 1:
            duplicates_by_key[director_key] = ordered

    return {
        "database": directors_database,
        "records": records,
        "records_by_page_id": records_by_page_id,
        "canonical_by_key": canonical_by_key,
        "duplicates_by_key": duplicates_by_key,
    }


def find_genres_database():
    preferred_id = str(NOTION_GENRES_DATABASE_ID or "").strip()
    if preferred_id:
        try:
            database = retrieve_notion_database(preferred_id)
        except requests.RequestException:
            database = None
        if database:
            return database

    exact_matches = []
    for item in notion_search_databases(GENRES_DATABASE_TITLE):
        title = notion_database_title_text(item)
        if title == GENRES_DATABASE_TITLE:
            exact_matches.append(item)
    if len(exact_matches) == 1:
        return exact_matches[0]
    if exact_matches:
        for item in exact_matches:
            properties = item.get("properties", {}) or {}
            if "Name" in properties and GENRE_KEY_PROPERTY in properties:
                return item
        return exact_matches[0]
    return None


def create_genres_database():
    parent_page = ensure_directors_parent_page()
    payload = {
        "parent": {"type": "page_id", "page_id": parent_page.get("id", "")},
        "title": notion_title_text(GENRES_DATABASE_TITLE),
        "properties": {
            "Name": {"title": {}},
            GENRE_KEY_PROPERTY: {"rich_text": {}},
            GENRE_ALIASES_PROPERTY: {"rich_text": {}},
        }
    }
    response = requests.post(
        "https://api.notion.com/v1/databases",
        headers=notion_api_headers(),
        json=payload,
        timeout=60
    )
    response.raise_for_status()
    return response.json() or {}


def ensure_genres_database():
    existing = find_genres_database()
    if existing:
        return existing
    return create_genres_database()


def ensure_movie_genre_relation_property(genres_database_id):
    movie_database = retrieve_notion_database(NOTION_DATABASE_ID)
    properties = (movie_database or {}).get("properties", {}) or {}
    if GENRE_RELATION_PROPERTY in properties:
        return properties
    response = requests.patch(
        f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}",
        headers=notion_api_headers(),
        json={
            "properties": {
                GENRE_RELATION_PROPERTY: {
                    "relation": {
                        "database_id": genres_database_id,
                        "type": "single_property",
                        "single_property": {},
                    }
                }
            }
        },
        timeout=60
    )
    response.raise_for_status()
    return (response.json() or {}).get("properties", properties)


def build_genre_page_record(page):
    properties = page.get("properties", {}) or {}
    display_name = extract_notion_page_title(properties)
    key = extract_notion_rich_text_value(properties, GENRE_KEY_PROPERTY) or normalized_genre_key(display_name)
    aliases = extract_notion_rich_text_value(properties, GENRE_ALIASES_PROPERTY)
    alias_values = [item.strip() for item in aliases.split("|") if item.strip()]
    if display_name and display_name not in alias_values:
        alias_values.append(display_name)
    return {
        "page_id": page.get("id", ""),
        "display_name": display_name,
        "genre_key": key,
        "aliases": alias_values,
        "page_url": page.get("url", ""),
        "created_time": page.get("created_time", ""),
        "page": page,
    }


def fetch_genre_page_records(genres_database_id):
    records = []
    for page in fetch_all_notion_database_pages(genres_database_id):
        record = build_genre_page_record(page)
        if record["page_id"] and record["genre_key"]:
            records.append(record)
    return records


def build_genre_catalog():
    genres_database = find_genres_database()
    if not genres_database:
        return {
            "database": None,
            "records": [],
            "records_by_page_id": {},
            "canonical_by_key": {},
            "duplicates_by_key": {},
        }

    records = fetch_genre_page_records(genres_database.get("id"))
    records_by_page_id = {record["page_id"]: record for record in records if record.get("page_id")}
    grouped = {}
    for record in records:
        grouped.setdefault(record["genre_key"], []).append(record)

    canonical_by_key = {}
    duplicates_by_key = {}
    for genre_key, items in grouped.items():
        ordered = sorted(
            items,
            key=lambda item: (
                item.get("created_time", ""),
                item.get("page_id", "")
            )
        )
        canonical_by_key[genre_key] = ordered[0]
        if len(ordered) > 1:
            duplicates_by_key[genre_key] = ordered

    return {
        "database": genres_database,
        "records": records,
        "records_by_page_id": records_by_page_id,
        "canonical_by_key": canonical_by_key,
        "duplicates_by_key": duplicates_by_key,
    }


def save_director_migration_report(report_payload, suffix):
    CORRECTION_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    target = CORRECTION_REPORTS_DIR / f"{DIRECTOR_MIGRATION_REPORT_PREFIX}-{suffix}-{stamp}.json"
    save_json_file(target, report_payload)
    return str(target)


def build_director_migration_preview():
    movie_database = retrieve_notion_database(NOTION_DATABASE_ID)
    movie_properties = (movie_database or {}).get("properties", {}) or {}
    catalog = build_director_catalog()
    directors_database = catalog["database"]
    directors_database_id = (directors_database or {}).get("id", "")
    existing_director_records = catalog["records"]
    canonical_by_key = catalog["canonical_by_key"]
    duplicate_director_keys = {
        director_key: [item["display_name"] for item in records]
        for director_key, records in catalog["duplicates_by_key"].items()
    }

    movies_to_link = []
    directors_to_create = {}
    skipped_movies = []
    unique_director_keys = {}
    for page in fetch_all_notion_database_pages():
        properties = page.get("properties", {}) or {}
        page_id = page.get("id", "")
        title = extract_notion_page_title(properties)
        director_text = extract_notion_rich_text_value(properties, "Director")
        current_relation_ids = extract_notion_relation_ids(properties, DIRECTOR_RELATION_PROPERTY)
        director_names = split_director_names(director_text)
        if not director_names:
            skipped_movies.append({
                "title": title,
                "page_id": page_id,
                "reason": "missing_director_text",
            })
            continue

        desired_relation_ids = []
        desired_directors = []
        ambiguous = []
        for name in director_names:
            director_key = normalized_person_key(name)
            if not director_key:
                continue
            unique_director_keys.setdefault(director_key, set()).add(name)
            matching_record = canonical_by_key.get(director_key)
            if matching_record:
                desired_relation_ids.append(matching_record["page_id"])
                desired_directors.append(matching_record["display_name"] or name)
            else:
                create_item = directors_to_create.setdefault(director_key, {
                    "display_name": name,
                    "director_key": director_key,
                    "aliases": set(),
                })
                create_item["display_name"] = choose_director_display_name(create_item.get("display_name", ""), name)
                create_item["aliases"].add(name)
                desired_directors.append(name)

        desired_relation_ids = list(dict.fromkeys(desired_relation_ids))
        current_sorted = sorted(current_relation_ids)
        desired_sorted = sorted(desired_relation_ids)
        if current_sorted != desired_sorted:
            movies_to_link.append({
                "title": title,
                "page_id": page_id,
                "director_text": director_text,
                "director_names": director_names,
                "current_relation_ids": current_relation_ids,
                "desired_relation_ids": desired_relation_ids,
                "desired_directors": desired_directors,
                "needs_create": [normalized_person_key(name) for name in director_names if normalized_person_key(name) not in directors_by_key],
            })

    directors_to_create_payload = []
    for director_key, item in sorted(directors_to_create.items(), key=lambda pair: pair[1]["display_name"].lower()):
        aliases = sorted(item["aliases"])
        directors_to_create_payload.append({
            "display_name": item["display_name"],
            "director_key": director_key,
            "aliases": aliases,
        })

    return {
        "summary": {
            "directors_database_exists": bool(directors_database_id),
            "directors_database_id": directors_database_id,
            "movie_relation_exists": DIRECTOR_RELATION_PROPERTY in movie_properties,
            "existing_director_pages": len(existing_director_records),
            "duplicate_director_keys": len(duplicate_director_keys),
            "directors_to_create": len(directors_to_create_payload),
            "movies_to_link": len(movies_to_link),
            "movies_skipped": len(skipped_movies),
            "unique_director_keys_in_movies": len(unique_director_keys),
        },
        "structure_changes_needed": {
            "create_directors_database": not bool(directors_database_id),
            "add_movie_director_relation": DIRECTOR_RELATION_PROPERTY not in movie_properties,
        },
        "directors_to_create": directors_to_create_payload,
        "movies_to_link": movies_to_link,
        "skipped_movies": skipped_movies,
        "duplicate_director_keys": duplicate_director_keys,
    }


def create_director_page(directors_database_id, display_name, director_key, aliases):
    payload = {
        "parent": {"database_id": directors_database_id},
        "properties": {
            "Name": {"title": notion_title_text(display_name)},
            DIRECTOR_KEY_PROPERTY: {"rich_text": notion_rich_text(director_key)},
            DIRECTOR_ALIASES_PROPERTY: {"rich_text": notion_rich_text(" | ".join(sorted(set(aliases))))},
        }
    }
    response = requests.post(
        "https://api.notion.com/v1/pages",
        headers=notion_api_headers(),
        json=payload,
        timeout=30
    )
    response.raise_for_status()
    return response.json() or {}


def create_genre_page(genres_database_id, display_name, genre_key, aliases):
    payload = {
        "parent": {"database_id": genres_database_id},
        "properties": {
            "Name": {"title": notion_title_text(display_name)},
            GENRE_KEY_PROPERTY: {"rich_text": notion_rich_text(genre_key)},
            GENRE_ALIASES_PROPERTY: {"rich_text": notion_rich_text(" | ".join(sorted(set(aliases))))},
        }
    }
    response = requests.post(
        "https://api.notion.com/v1/pages",
        headers=notion_api_headers(),
        json=payload,
        timeout=30
    )
    response.raise_for_status()
    return response.json() or {}


def update_movie_director_relations(page_id, relation_ids):
    response = requests.patch(
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=notion_api_headers(),
        json={
            "properties": {
                DIRECTOR_RELATION_PROPERTY: {
                    "relation": [{"id": relation_id} for relation_id in relation_ids]
                }
            }
        },
        timeout=30
    )
    response.raise_for_status()
    return response.json() or {}


def update_movie_genre_relations(page_id, relation_ids):
    response = requests.patch(
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=notion_api_headers(),
        json={
            "properties": {
                GENRE_RELATION_PROPERTY: {
                    "relation": [{"id": relation_id} for relation_id in relation_ids]
                }
            }
        },
        timeout=30
    )
    response.raise_for_status()
    return response.json() or {}


def update_notion_page_properties(page_id, properties_payload):
    if not properties_payload:
        return {}
    response = requests.patch(
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=notion_api_headers(),
        json={"properties": properties_payload},
        timeout=30
    )
    response.raise_for_status()
    return response.json() or {}


def register_director_record_in_catalog(director_catalog, record):
    if not isinstance(director_catalog, dict) or not record:
        return record
    director_catalog.setdefault("records", []).append(record)
    page_id = record.get("page_id", "")
    director_key = record.get("director_key", "")
    if page_id:
        director_catalog.setdefault("records_by_page_id", {})[page_id] = record
    if director_key and director_key not in director_catalog.setdefault("canonical_by_key", {}):
        director_catalog["canonical_by_key"][director_key] = record
    return record


def register_genre_record_in_catalog(genre_catalog, record):
    if not isinstance(genre_catalog, dict) or not record:
        return record
    genre_catalog.setdefault("records", []).append(record)
    page_id = record.get("page_id", "")
    genre_key = record.get("genre_key", "")
    if page_id:
        genre_catalog.setdefault("records_by_page_id", {})[page_id] = record
    if genre_key and genre_key not in genre_catalog.setdefault("canonical_by_key", {}):
        genre_catalog["canonical_by_key"][genre_key] = record
    return record


def ensure_director_pages_for_names(directors_database_id, director_catalog, director_names):
    linked_records = []
    created_records = []
    skipped_names = []

    for name in director_names:
        director_key = normalized_person_key(name)
        if not director_key:
            skipped_names.append({"name": name, "reason": "invalid_director_name"})
            continue

        canonical = (director_catalog.get("canonical_by_key", {}) or {}).get(director_key)
        if not canonical:
            page = create_director_page(directors_database_id, name, director_key, [name])
            canonical = build_director_page_record(page)
            register_director_record_in_catalog(director_catalog, canonical)
            created_records.append(canonical)

        linked_records.append(canonical)

    unique_records = []
    seen_page_ids = set()
    for record in linked_records:
        page_id = record.get("page_id", "")
        if not page_id or page_id in seen_page_ids:
            continue
        seen_page_ids.add(page_id)
        unique_records.append(record)

    return {
        "records": unique_records,
        "created_records": created_records,
        "skipped_names": skipped_names,
    }


def ensure_genre_pages_for_names(genres_database_id, genre_catalog, genre_names):
    linked_records = []
    created_records = []
    skipped_names = []

    for name in genre_names:
        genre_key = normalized_genre_key(name)
        if not genre_key:
            skipped_names.append({"name": name, "reason": "invalid_genre_name"})
            continue

        canonical = (genre_catalog.get("canonical_by_key", {}) or {}).get(genre_key)
        if not canonical:
            page = create_genre_page(genres_database_id, name, genre_key, [name])
            canonical = build_genre_page_record(page)
            register_genre_record_in_catalog(genre_catalog, canonical)
            created_records.append(canonical)

        linked_records.append(canonical)

    unique_records = []
    seen_page_ids = set()
    for record in linked_records:
        page_id = record.get("page_id", "")
        if not page_id or page_id in seen_page_ids:
            continue
        seen_page_ids.add(page_id)
        unique_records.append(record)

    return {
        "records": unique_records,
        "created_records": created_records,
        "skipped_names": skipped_names,
    }


def enrich_director_record_image_if_missing(record):
    display_name = record.get("display_name", "")
    if not display_name:
        return {"status": "skipped", "reason": "missing_name"}
    if record.get("image_url"):
        return {"status": "unchanged", "reason": "image_already_present"}

    tmdb_person = fetch_tmdb_person_profile(display_name)
    if not tmdb_person:
        return {"status": "skipped", "reason": "no_tmdb_person_match"}
    if tmdb_person.get("confidence") == "low":
        return {
            "status": "skipped",
            "reason": "low_confidence_tmdb_match",
            "matched_name": tmdb_person.get("matched_name", ""),
        }

    profile_url = tmdb_person.get("profile_url", "")
    if not profile_url:
        return {
            "status": "skipped",
            "reason": "tmdb_match_missing_profile_image",
            "matched_name": tmdb_person.get("matched_name", ""),
        }

    update_director_page_tmdb_profile(
        record["page_id"],
        tmdb_person.get("tmdb_person_id"),
        profile_url,
    )
    record["image_url"] = profile_url
    record["tmdb_person_id"] = tmdb_person.get("tmdb_person_id")
    return {
        "status": "updated",
        "matched_name": tmdb_person.get("matched_name", ""),
        "tmdb_person_id": tmdb_person.get("tmdb_person_id"),
        "profile_url": profile_url,
        "confidence": tmdb_person.get("confidence", ""),
    }


def sync_movie_directors(page, tmdb_data, directors_database_id, director_catalog):
    properties = page.get("properties", {}) or {}
    page_id = page.get("id", "")
    title = extract_notion_page_title(properties)
    director_text = extract_notion_rich_text_value(properties, "Director")
    if not director_text:
        director_text = str((tmdb_data or {}).get("director") or "").strip()
    director_names = split_director_names(director_text)
    if not director_names:
        return {
            "title": title,
            "page_id": page_id,
            "status": "skipped",
            "reason": "missing_director_text",
            "director_names": [],
            "linked_directors": [],
            "created_directors": [],
            "director_images_updated": [],
            "director_images_skipped": [],
            "movie_link_updated": False,
        }

    page_result = ensure_director_pages_for_names(directors_database_id, director_catalog, director_names)
    linked_records = page_result["records"]
    desired_relation_ids = [record["page_id"] for record in linked_records if record.get("page_id")]
    current_relation_ids = extract_notion_relation_ids(properties, DIRECTOR_RELATION_PROPERTY)
    movie_link_updated = False

    if sorted(current_relation_ids) != sorted(desired_relation_ids):
        update_movie_director_relations(page_id, desired_relation_ids)
        movie_link_updated = True

    director_images_updated = []
    director_images_skipped = []
    for record in linked_records:
        image_result = enrich_director_record_image_if_missing(record)
        if image_result.get("status") == "updated":
            director_images_updated.append({
                "page_id": record.get("page_id", ""),
                "display_name": record.get("display_name", ""),
                **image_result,
            })
        elif image_result.get("status") == "skipped":
            director_images_skipped.append({
                "page_id": record.get("page_id", ""),
                "display_name": record.get("display_name", ""),
                **image_result,
            })

    return {
        "title": title,
        "page_id": page_id,
        "status": "linked",
        "reason": "",
        "director_names": director_names,
        "linked_directors": linked_records,
        "created_directors": page_result["created_records"],
        "director_creation_skipped": page_result["skipped_names"],
        "director_images_updated": director_images_updated,
        "director_images_skipped": director_images_skipped,
        "movie_link_updated": movie_link_updated,
    }


def sync_movie_genres(page, tmdb_data, genres_database_id, genre_catalog):
    properties = page.get("properties", {}) or {}
    page_id = page.get("id", "")
    title = extract_notion_page_title(properties)
    genres_text = extract_notion_genres_value(properties)
    if not genres_text:
        genres_text = str((tmdb_data or {}).get("genres") or "").strip()
    genre_names = split_genre_names(genres_text)
    if not genre_names:
        return {
            "title": title,
            "page_id": page_id,
            "status": "skipped",
            "reason": "missing_genres_text",
            "genre_names": [],
            "linked_genres": [],
            "created_genres": [],
            "movie_link_updated": False,
        }

    page_result = ensure_genre_pages_for_names(genres_database_id, genre_catalog, genre_names)
    linked_records = page_result["records"]
    desired_relation_ids = [record["page_id"] for record in linked_records if record.get("page_id")]
    current_relation_ids = extract_notion_relation_ids(properties, GENRE_RELATION_PROPERTY)
    movie_link_updated = False

    if sorted(current_relation_ids) != sorted(desired_relation_ids):
        update_movie_genre_relations(page_id, desired_relation_ids)
        movie_link_updated = True

    return {
        "title": title,
        "page_id": page_id,
        "status": "linked",
        "reason": "",
        "genre_names": genre_names,
        "linked_genres": linked_records,
        "created_genres": page_result["created_records"],
        "genre_creation_skipped": page_result["skipped_names"],
        "movie_link_updated": movie_link_updated,
    }


def apply_director_migration():
    preview_before = build_director_migration_preview()
    directors_database = ensure_directors_database()
    directors_database_id = directors_database.get("id", "")
    ensure_movie_director_relation_property(directors_database_id)

    backup_payload = {
        "created_at": current_timestamp(),
        "preview_before": preview_before,
        "movie_pages": [
            {
                "page_id": page.get("id", ""),
                "title": extract_notion_page_title(page.get("properties", {}) or {}),
                "director_text": extract_notion_rich_text_value(page.get("properties", {}) or {}, "Director"),
                "director_relation_ids": extract_notion_relation_ids(page.get("properties", {}) or {}, DIRECTOR_RELATION_PROPERTY),
            }
            for page in fetch_all_notion_database_pages()
        ],
        "director_pages_before": fetch_director_page_records(directors_database_id),
    }
    backup_path = save_director_migration_report(backup_payload, "backup")

    preview = build_director_migration_preview()
    created_directors = []
    failed_directors = []
    for item in preview["directors_to_create"]:
        try:
            page = create_director_page(
                directors_database_id,
                item["display_name"],
                item["director_key"],
                item["aliases"],
            )
            created_directors.append({
                "page_id": page.get("id", ""),
                "display_name": item["display_name"],
                "director_key": item["director_key"],
            })
        except requests.RequestException as exc:
            failed_directors.append({
                "display_name": item["display_name"],
                "director_key": item["director_key"],
                "reason": str(exc),
            })

    refreshed_directors = fetch_director_page_records(directors_database_id)
    directors_by_key = {}
    for record in refreshed_directors:
        directors_by_key.setdefault(record["director_key"], []).append(record)

    linked_movies = []
    failed_movies = []
    skipped_movies = list(preview["skipped_movies"])
    for item in preview["movies_to_link"]:
        target_relation_ids = []
        unresolved_keys = []
        for name in item.get("director_names", []):
            director_key = normalized_person_key(name)
            records = directors_by_key.get(director_key, [])
            if len(records) != 1:
                unresolved_keys.append({"name": name, "director_key": director_key, "matches": [record["display_name"] for record in records]})
                continue
            target_relation_ids.append(records[0]["page_id"])
        if unresolved_keys:
            skipped_movies.append({
                "title": item.get("title", ""),
                "page_id": item.get("page_id", ""),
                "reason": "unresolved_director_relation_after_create",
                "details": unresolved_keys,
            })
            continue
        target_relation_ids = list(dict.fromkeys(target_relation_ids))
        try:
            update_movie_director_relations(item["page_id"], target_relation_ids)
            linked_movies.append({
                "title": item.get("title", ""),
                "page_id": item.get("page_id", ""),
                "director_names": item.get("director_names", []),
                "director_relation_ids": target_relation_ids,
            })
        except requests.RequestException as exc:
            failed_movies.append({
                "title": item.get("title", ""),
                "page_id": item.get("page_id", ""),
                "reason": str(exc),
            })

    clear_runtime_cache()
    refresh_film_cache_from_source()

    result = {
        "status": "applied",
        "created_at": current_timestamp(),
        "backup_path": backup_path,
        "directors_database_id": directors_database_id,
        "created_directors": created_directors,
        "failed_directors": failed_directors,
        "linked_movies": linked_movies,
        "failed_movies": failed_movies,
        "skipped_movies": skipped_movies,
        "summary": {
            "created_directors": len(created_directors),
            "failed_directors": len(failed_directors),
            "linked_movies": len(linked_movies),
            "failed_movies": len(failed_movies),
            "skipped_movies": len(skipped_movies),
        },
    }
    result["report_path"] = save_director_migration_report(result, "applied")
    return result


def update_director_page_tmdb_profile(page_id, tmdb_person_id, profile_url):
    payload = {
        DIRECTOR_IMAGE_PROPERTY: {
            "files": [{
                "name": "tmdb-profile",
                "type": "external",
                "external": {"url": profile_url}
            }]
        }
    }
    if tmdb_person_id not in (None, ""):
        payload[DIRECTOR_TMDB_PERSON_ID_PROPERTY] = {"number": int(tmdb_person_id)}
    response = requests.patch(
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=notion_api_headers(),
        json={"properties": payload},
        timeout=30
    )
    response.raise_for_status()
    return response.json() or {}


def complete_directors_integration():
    directors_database = ensure_directors_database()
    directors_database_id = directors_database.get("id", "")
    ensure_movie_director_relation_property(directors_database_id)
    ensure_directors_database_properties(directors_database_id)

    catalog = build_director_catalog()
    canonical_by_key = catalog["canonical_by_key"]
    movies = fetch_all_notion_database_pages()
    linked_movies = []
    failed_movies = []
    skipped_movies = []
    updated_movie_links = 0

    for page in movies:
        properties = page.get("properties", {}) or {}
        title = extract_notion_page_title(properties)
        page_id = page.get("id", "")
        director_text = extract_notion_rich_text_value(properties, "Director")
        director_names = split_director_names(director_text)
        if not director_names:
            skipped_movies.append({
                "title": title,
                "page_id": page_id,
                "reason": "missing_director_text",
            })
            continue

        desired_relation_ids = []
        unresolved = []
        for name in director_names:
            director_key = normalized_person_key(name)
            canonical = canonical_by_key.get(director_key)
            if not canonical:
                unresolved.append({"name": name, "director_key": director_key, "reason": "director_page_missing"})
                continue
            desired_relation_ids.append(canonical["page_id"])

        if unresolved:
            skipped_movies.append({
                "title": title,
                "page_id": page_id,
                "reason": "unresolved_director_pages",
                "details": unresolved,
            })
            continue

        desired_relation_ids = list(dict.fromkeys(desired_relation_ids))
        current_relation_ids = extract_notion_relation_ids(properties, DIRECTOR_RELATION_PROPERTY)
        if sorted(current_relation_ids) != sorted(desired_relation_ids):
            try:
                update_movie_director_relations(page_id, desired_relation_ids)
                updated_movie_links += 1
            except requests.RequestException as exc:
                failed_movies.append({
                    "title": title,
                    "page_id": page_id,
                    "reason": str(exc),
                })
                continue

        linked_movies.append({
            "title": title,
            "page_id": page_id,
            "director_names": director_names,
            "director_relation_ids": desired_relation_ids,
        })

    catalog = build_director_catalog()
    enriched_directors = []
    skipped_directors = []
    failed_directors = []
    image_updates = 0
    for record in catalog["records"]:
        display_name = record.get("display_name", "")
        if not display_name:
            skipped_directors.append({
                "page_id": record.get("page_id", ""),
                "display_name": display_name,
                "reason": "missing_name",
            })
            continue
        try:
            tmdb_person = fetch_tmdb_person_profile(display_name)
        except (requests.RequestException, RuntimeError) as exc:
            failed_directors.append({
                "page_id": record.get("page_id", ""),
                "display_name": display_name,
                "reason": str(exc),
            })
            continue
        if not tmdb_person:
            skipped_directors.append({
                "page_id": record.get("page_id", ""),
                "display_name": display_name,
                "reason": "no_tmdb_person_match",
            })
            continue
        if tmdb_person.get("confidence") == "low":
            skipped_directors.append({
                "page_id": record.get("page_id", ""),
                "display_name": display_name,
                "reason": "low_confidence_tmdb_match",
                "matched_name": tmdb_person.get("matched_name", ""),
            })
            continue
        profile_url = tmdb_person.get("profile_url", "")
        if not profile_url:
            skipped_directors.append({
                "page_id": record.get("page_id", ""),
                "display_name": display_name,
                "reason": "tmdb_match_missing_profile_image",
                "matched_name": tmdb_person.get("matched_name", ""),
            })
            continue

        current_tmdb_person_id = record.get("tmdb_person_id")
        current_image_url = record.get("image_url", "")
        needs_update = (
            current_image_url != profile_url
            or int(current_tmdb_person_id or 0) != int(tmdb_person.get("tmdb_person_id") or 0)
        )
        if needs_update:
            try:
                update_director_page_tmdb_profile(
                    record["page_id"],
                    tmdb_person.get("tmdb_person_id"),
                    profile_url,
                )
                image_updates += 1
            except requests.RequestException as exc:
                failed_directors.append({
                    "page_id": record.get("page_id", ""),
                    "display_name": display_name,
                    "reason": str(exc),
                })
                continue

        enriched_directors.append({
            "page_id": record.get("page_id", ""),
            "display_name": display_name,
            "tmdb_person_id": tmdb_person.get("tmdb_person_id"),
            "matched_name": tmdb_person.get("matched_name", ""),
            "confidence": tmdb_person.get("confidence", ""),
            "profile_url": profile_url,
        })

    clear_runtime_cache()
    refreshed_films = refresh_film_cache_from_source()
    site_context_director_count = sum(1 for film in refreshed_films if film.get("director_entries"))

    result = {
        "status": "completed",
        "created_at": current_timestamp(),
        "directors_database_id": directors_database_id,
        "summary": {
            "director_pages_exist": len(catalog["records"]),
            "canonical_director_keys": len(catalog["canonical_by_key"]),
            "duplicate_director_keys": len(catalog["duplicates_by_key"]),
            "movies_linked": len(linked_movies),
            "movie_link_updates": updated_movie_links,
            "movies_skipped": len(skipped_movies),
            "movies_failed": len(failed_movies),
            "directors_with_tmdb_images": len(enriched_directors),
            "director_image_updates": image_updates,
            "directors_skipped": len(skipped_directors),
            "directors_failed": len(failed_directors),
            "site_context_movies_with_linked_directors": site_context_director_count,
        },
        "sample_linked_movies": linked_movies[:10],
        "sample_enriched_directors": enriched_directors[:10],
        "skipped_movies": skipped_movies,
        "failed_movies": failed_movies,
        "skipped_directors": skipped_directors,
        "failed_directors": failed_directors,
        "site_context_sees_linked_directors": bool(site_context_director_count),
    }
    result["report_path"] = save_director_migration_report(result, "complete")
    return result


def extract_notion_select_value(properties, key):
    props = properties or {}
    select_value = (props.get(key, {}) or {}).get("select")
    return select_value.get("name", "").strip() if isinstance(select_value, dict) else ""


def extract_notion_number_value(properties, key):
    props = properties or {}
    value = (props.get(key, {}) or {}).get("number")
    return "" if value in (None, "") else value


def extract_notion_rich_text_value(properties, key):
    props = properties or {}
    prop = props.get(key, {}) or {}
    if prop.get("type") != "rich_text":
        return ""
    return "".join(part.get("plain_text", "") for part in prop.get("rich_text", [])).strip()


def build_tmdb_notion_update_payload(page_properties, tmdb_data):
    payload = {}
    if not tmdb_data:
        return payload

    if notion_property_is_empty(page_properties.get("Year")) and tmdb_data.get("year") is not None:
        payload["Year"] = {"number": tmdb_data["year"]}
    if notion_property_is_empty(page_properties.get("Director")) and tmdb_data.get("director"):
        payload["Director"] = {"rich_text": [{"type": "text", "text": {"content": tmdb_data["director"][:2000]}}]}
    rating_value = tmdb_data.get("rating")
    if rating_value not in (None, ""):
        next_rating = round(float(rating_value), 1)
        current_rating = notion_number_value(page_properties.get("Rating"))
        if current_rating is None or round(float(current_rating), 1) != next_rating:
            payload["Rating"] = {"number": next_rating}
    if notion_property_is_empty(page_properties.get("Poster URL")) and tmdb_data.get("poster_url"):
        payload["Poster URL"] = {"url": tmdb_data["poster_url"]}
    if notion_property_is_empty(page_properties.get("Genres")) and tmdb_data.get("genres"):
        genres_payload = build_notion_genres_property_payload(page_properties.get("Genres"), tmdb_data["genres"])
        if genres_payload:
            payload["Genres"] = genres_payload
    if notion_property_is_empty(page_properties.get("poster ")) and tmdb_data.get("poster_url"):
        payload["poster "] = {
            "files": [{
                "name": "tmdb-poster",
                "type": "external",
                "external": {"url": tmdb_data["poster_url"]}
            }]
        }
    payload.update(build_tmdb_overview_update_payload(page_properties, tmdb_data))
    return payload


def build_tmdb_overview_update_payload(page_properties, tmdb_data):
    payload = {}
    overview_prop = page_properties.get("Overview")
    if not isinstance(overview_prop, dict):
        return payload
    overview_text = str((tmdb_data or {}).get("overview") or "").strip()
    if not overview_text or not notion_property_is_empty(overview_prop):
        return payload
    payload["Overview"] = {
        "rich_text": [{
            "type": "text",
            "text": {"content": overview_text[:2000]}
        }]
    }
    return payload


def apply_tmdb_missing_overviews():
    TMDB_LOOKUP_CACHE.clear()
    pages = fetch_all_notion_database_pages()
    updated = 0
    skipped_existing = 0
    skipped_missing_property = 0
    skipped_no_match = 0
    failed = 0
    updated_titles = []

    for page in pages:
        page_id = page.get("id", "")
        properties = page.get("properties", {}) or {}
        movie_title = extract_notion_page_title(properties)
        category = extract_notion_select_value(properties, "category")
        current_year_value = extract_notion_number_value(properties, "Year")

        if not isinstance(properties.get("Overview"), dict):
            skipped_missing_property += 1
            continue
        if extract_notion_rich_text_value(properties, "Overview"):
            skipped_existing += 1
            continue
        if not movie_title or movie_title == "Untitled":
            skipped_no_match += 1
            continue

        try:
            tmdb_data = fetch_tmdb_enrichment(movie_title, category=category, year=current_year_value)
        except (requests.RequestException, RuntimeError):
            failed += 1
            continue
        if not tmdb_data or not str(tmdb_data.get("overview") or "").strip():
            skipped_no_match += 1
            continue

        payload = build_tmdb_overview_update_payload(properties, tmdb_data)
        if not payload:
            skipped_existing += 1
            continue

        try:
            response = requests.patch(
                f"https://api.notion.com/v1/pages/{page_id}",
                headers=notion_api_headers(),
                json={"properties": payload},
                timeout=30
            )
            response.raise_for_status()
            updated += 1
            updated_titles.append(movie_title)
        except requests.RequestException:
            failed += 1

    if updated:
        clear_runtime_cache()
        refresh_film_cache_from_source()

    return {
        "updated": updated,
        "skipped_existing": skipped_existing,
        "skipped_missing_property": skipped_missing_property,
        "skipped_no_match": skipped_no_match,
        "failed": failed,
        "titles": updated_titles,
    }


def apply_tmdb_poster_and_overview_cleanup():
    TMDB_LOOKUP_CACHE.clear()
    pages = fetch_all_notion_database_pages()
    updated = 0
    skipped = 0
    failed = 0
    poster_updates = 0
    overview_updates = 0
    changed_titles = []

    for page in pages:
        page_id = page.get("id", "")
        properties = page.get("properties", {}) or {}
        movie_title = extract_notion_page_title(properties)
        category = extract_notion_select_value(properties, "category")
        current_year_value = extract_notion_number_value(properties, "Year")
        if not movie_title or movie_title == "Untitled":
            skipped += 1
            continue

        try:
            tmdb_data = fetch_tmdb_enrichment(movie_title, category=category, year=current_year_value)
        except (requests.RequestException, RuntimeError):
            failed += 1
            continue
        if not tmdb_data:
            skipped += 1
            continue

        match_score = int(tmdb_data.get("match_score") or 0)
        matched_year = normalize_year_value(tmdb_data.get("matched_year", ""))
        current_year = normalize_year_value(current_year_value)
        if match_score < 100:
            skipped += 1
            continue
        if current_year and matched_year and current_year != matched_year:
            skipped += 1
            continue

        payload = {}
        field_changes = []

        current_poster_url = notion_url_value(properties.get("Poster URL"))
        next_poster_url = str(tmdb_data.get("poster_url") or "").strip()
        if next_poster_url and current_poster_url != next_poster_url:
            payload["Poster URL"] = {"url": next_poster_url}
            poster_files_prop = properties.get("poster ") or {}
            if isinstance(poster_files_prop, dict) and poster_files_prop.get("type") == "files":
                payload["poster "] = {
                    "files": [{
                        "name": "tmdb-poster",
                        "type": "external",
                        "external": {"url": next_poster_url}
                    }]
                }
            poster_updates += 1
            field_changes.append("Poster URL")

        overview_prop = properties.get("Overview")
        next_overview = str(tmdb_data.get("overview") or "").strip()
        if isinstance(overview_prop, dict) and next_overview and notion_property_is_empty(overview_prop):
            payload["Overview"] = {
                "rich_text": [{
                    "type": "text",
                    "text": {"content": next_overview[:2000]}
                }]
            }
            overview_updates += 1
            field_changes.append("Overview")

        if not payload:
            skipped += 1
            continue

        try:
            response = requests.patch(
                f"https://api.notion.com/v1/pages/{page_id}",
                headers=notion_api_headers(),
                json={"properties": payload},
                timeout=30
            )
            response.raise_for_status()
            updated += 1
            changed_titles.append({"title": movie_title, "fields": field_changes})
        except requests.RequestException:
            failed += 1

    if updated:
        clear_runtime_cache()
        refresh_film_cache_from_source()

    return {
        "updated": updated,
        "skipped": skipped,
        "failed": failed,
        "poster_updates": poster_updates,
        "overview_updates": overview_updates,
        "changed_titles": changed_titles,
    }


def notion_url_value(prop):
    if not isinstance(prop, dict) or prop.get("type") != "url":
        return ""
    return (prop.get("url") or "").strip()


def notion_number_value(prop):
    if not isinstance(prop, dict) or prop.get("type") != "number":
        return None
    return prop.get("number")


def notion_rich_text_value(prop):
    if not isinstance(prop, dict) or prop.get("type") != "rich_text":
        return ""
    return "".join(item.get("plain_text", "") for item in prop.get("rich_text", [])).strip()


def notion_multi_select_value(prop):
    if not isinstance(prop, dict) or prop.get("type") != "multi_select":
        return ""
    return ", ".join(item.get("name", "").strip() for item in prop.get("multi_select", []) if item.get("name"))


def build_tmdb_correction_payload(page_properties, tmdb_data):
    payload = {}
    if not tmdb_data:
        return payload

    current_poster_url = notion_url_value(page_properties.get("Poster URL"))
    next_poster_url = (tmdb_data.get("poster_url") or "").strip()
    if next_poster_url and current_poster_url != next_poster_url:
        payload["Poster URL"] = {"url": next_poster_url}

    current_rating = notion_number_value(page_properties.get("Rating"))
    next_rating_raw = tmdb_data.get("rating")
    next_rating = round(float(next_rating_raw), 1) if next_rating_raw not in (None, "") else None
    if next_rating is not None and current_rating != next_rating:
        payload["Rating"] = {"number": next_rating}

    return payload


def fetch_targeted_tmdb_metadata(title):
    config = TARGETED_TMDB_OVERRIDES.get(title)
    if not config:
        return None
    kind = config.get("kind")
    if kind == "movie":
        movie_id = config.get("tmdb_id")
        details = tmdb_request(f"/movie/{movie_id}", {"language": "en-US"})
        credits = tmdb_request(f"/movie/{movie_id}/credits", {"language": "en-US"})
        director = next((
            person.get("name", "").strip()
            for person in credits.get("crew", [])
            if person.get("job") == "Director" and person.get("name")
        ), "")
        return {
            "title": details.get("title", "").strip(),
            "tmdb_type": "movie",
            "year": int(details.get("release_date", "")[:4]) if str(details.get("release_date", ""))[:4].isdigit() else "",
            "director": director,
            "genres": ", ".join(item.get("name", "").strip() for item in details.get("genres", []) if item.get("name")),
            "rating": round(float(details.get("vote_average")), 1) if details.get("vote_average") not in (None, "") else "",
            "poster_url": f"https://image.tmdb.org/t/p/w500{details.get('poster_path')}" if details.get("poster_path") else "",
            "overview": str(details.get("overview") or "").strip(),
            "source_used": f"TMDb movie id {movie_id}: {details.get('title', '').strip()}",
            "confidence": config.get("confidence", "high"),
            "reason": "Exact title-specific TMDb movie override.",
            "fields": tuple(config.get("fields", ())),
        }
    if kind == "tv_season":
        show_id = config.get("tmdb_id")
        season_number = int(config.get("season_number") or 0)
        show_details = tmdb_request(f"/tv/{show_id}", {"language": "en-US"})
        season_details = tmdb_request(f"/tv/{show_id}/season/{season_number}", {"language": "en-US"})
        creators = ", ".join(item.get("name", "").strip() for item in show_details.get("created_by", []) if item.get("name"))
        air_date = str(season_details.get("air_date") or "")
        return {
            "title": f"{show_details.get('name', '').strip()} Season {season_number}",
            "tmdb_type": "tv",
            "year": int(air_date[:4]) if air_date[:4].isdigit() else "",
            "director": creators,
            "genres": ", ".join(item.get("name", "").strip() for item in show_details.get("genres", []) if item.get("name")),
            "rating": round(float(season_details.get("vote_average")), 1) if season_details.get("vote_average") not in (None, "") else "",
            "poster_url": f"https://image.tmdb.org/t/p/w500{season_details.get('poster_path')}" if season_details.get("poster_path") else "",
            "overview": str(season_details.get("overview") or "").strip(),
            "source_used": f"TMDb TV season id {show_id} season {season_number}: {show_details.get('name', '').strip()}",
            "confidence": config.get("confidence", "high"),
            "reason": "Exact title-specific TMDb TV season override.",
            "fields": tuple(config.get("fields", ())),
        }
    return None


def build_targeted_metadata_payload(page_properties, proposed):
    payload = {}
    if "year" in proposed and isinstance(page_properties.get("Year"), dict):
        payload["Year"] = {"number": int(proposed["year"])}
    if "director" in proposed and isinstance(page_properties.get("Director"), dict):
        payload["Director"] = {"rich_text": [{"type": "text", "text": {"content": str(proposed["director"])[:2000]}}]}
    if "genres" in proposed and isinstance(page_properties.get("Genres"), dict):
        genres_prop = page_properties.get("Genres") or {}
        genres_value = str(proposed["genres"] or "").strip()
        if genres_prop.get("type") == "multi_select":
            payload["Genres"] = {"multi_select": [{"name": item.strip()} for item in genres_value.split(",") if item.strip()]}
        else:
            payload["Genres"] = {"rich_text": [{"type": "text", "text": {"content": genres_value[:2000]}}]}
    if "rating" in proposed and isinstance(page_properties.get("Rating"), dict):
        payload["Rating"] = {"number": float(proposed["rating"])}
    if "overview" in proposed and isinstance(page_properties.get("Overview"), dict):
        payload["Overview"] = {"rich_text": [{"type": "text", "text": {"content": str(proposed["overview"])[:2000]}}]}
    if "poster" in proposed:
        poster_url = str(proposed["poster"] or "").strip()
        if poster_url and isinstance(page_properties.get("Poster URL"), dict):
            payload["Poster URL"] = {"url": poster_url}
        poster_file_prop = page_properties.get("poster ") or {}
        if poster_url and isinstance(poster_file_prop, dict) and poster_file_prop.get("type") == "files":
            payload["poster "] = {
                "files": [{
                    "name": "tmdb-poster",
                    "type": "external",
                    "external": {"url": poster_url}
                }]
            }
    return payload


def build_targeted_movie_correction_preview():
    films = {film.get("name", ""): build_film_entry(film) for film in fetch_all_films()}
    pages = {extract_notion_page_title(page.get("properties", {}) or {}): page for page in fetch_all_notion_database_pages()}
    preview_items = []

    for title in TARGETED_MOVIE_TITLES:
        film = films.get(title)
        page = pages.get(title)
        if not film or not page:
            preview_items.append({
                "title": title,
                "status": "skipped",
                "reason": "Title was not found exactly in the live Notion movie dataset.",
                "confidence": "low",
                "source_used": "Notion exact-title lookup",
                "current_values": {},
                "proposed_values": {},
                "suspected_fields": [],
                "page_id": "",
                "payload": {},
            })
            continue

        tmdb_data = fetch_targeted_tmdb_metadata(title)
        if not tmdb_data:
            preview_items.append({
                "title": title,
                "status": "skipped",
                "reason": "No targeted TMDb override is configured for this title yet.",
                "confidence": "low",
                "source_used": "Targeted override lookup",
                "current_values": {},
                "proposed_values": {},
                "suspected_fields": [],
                "page_id": page.get("id", ""),
                "payload": {},
            })
            continue

        fields_to_check = tmdb_data.get("fields", ())
        current_values = {}
        proposed_values = {}
        suspected_fields = []

        if "year" in fields_to_check and normalize_year_value(film.get("year", "")) != normalize_year_value(tmdb_data.get("year", "")):
            current_values["year"] = normalize_year_value(film.get("year", ""))
            proposed_values["year"] = normalize_year_value(tmdb_data.get("year", ""))
            suspected_fields.append("year")
        if "director" in fields_to_check and values_differ(film.get("director", ""), tmdb_data.get("director", "")):
            current_values["director"] = film.get("director", "")
            proposed_values["director"] = tmdb_data.get("director", "")
            suspected_fields.append("director")
        if "genres" in fields_to_check and values_differ(film.get("genres", ""), tmdb_data.get("genres", "")):
            current_values["genres"] = film.get("genres", "")
            proposed_values["genres"] = tmdb_data.get("genres", "")
            suspected_fields.append("genres")
        if "rating" in fields_to_check and rating_values_differ(film.get("tmdb_rating", ""), tmdb_data.get("rating", "")):
            current_values["TMDb rating"] = film.get("tmdb_rating", "") or "Missing"
            proposed_values["TMDb rating"] = tmdb_data.get("rating", "")
            suspected_fields.append("TMDb rating")
        if "overview" in fields_to_check and values_differ(film.get("overview", ""), tmdb_data.get("overview", "")):
            current_values["overview"] = (str(film.get("overview") or "")[:220] + "...") if str(film.get("overview") or "")[220:] else (film.get("overview", "") or "Missing")
            proposed_values["overview"] = (str(tmdb_data.get("overview") or "")[:220] + "...") if str(tmdb_data.get("overview") or "")[220:] else tmdb_data.get("overview", "")
            suspected_fields.append("overview")
        if "poster" in fields_to_check and poster_urls_differ(film.get("poster", ""), tmdb_data.get("poster_url", "")):
            current_values["poster"] = film.get("poster", "") or "Missing"
            proposed_values["poster"] = tmdb_data.get("poster_url", "")
            suspected_fields.append("poster")

        proposed_payload = {}
        if suspected_fields and tmdb_data.get("confidence") == "high":
            payload_source = {}
            if "year" in suspected_fields:
                payload_source["year"] = tmdb_data.get("year")
            if "director" in suspected_fields:
                payload_source["director"] = tmdb_data.get("director")
            if "genres" in suspected_fields:
                payload_source["genres"] = tmdb_data.get("genres")
            if "TMDb rating" in suspected_fields:
                payload_source["rating"] = tmdb_data.get("rating")
            if "overview" in suspected_fields:
                payload_source["overview"] = tmdb_data.get("overview")
            if "poster" in suspected_fields:
                payload_source["poster"] = tmdb_data.get("poster_url")
            proposed_payload = build_targeted_metadata_payload(page.get("properties", {}) or {}, payload_source)

        preview_items.append({
            "title": title,
            "status": "ready" if proposed_payload else "skipped",
            "reason": tmdb_data.get("reason", ""),
            "confidence": tmdb_data.get("confidence", "low"),
            "source_used": tmdb_data.get("source_used", "TMDb targeted lookup"),
            "current_values": current_values,
            "proposed_values": proposed_values,
            "suspected_fields": suspected_fields,
            "page_id": page.get("id", ""),
            "payload": proposed_payload,
        })

    return preview_items


def apply_targeted_movie_corrections():
    preview_items = build_targeted_movie_correction_preview()
    updated = 0
    skipped = 0
    failed = 0
    changed_titles = []

    for item in preview_items:
        payload = item.get("payload", {})
        if item.get("status") != "ready" or not payload or item.get("confidence") != "high":
            skipped += 1
            continue
        try:
            response = requests.patch(
                f"https://api.notion.com/v1/pages/{item['page_id']}",
                headers=notion_api_headers(),
                json={"properties": payload},
                timeout=30
            )
            response.raise_for_status()
            updated += 1
            changed_titles.append({"title": item["title"], "fields": list(item.get("proposed_values", {}).keys())})
        except requests.RequestException:
            failed += 1

    clear_runtime_cache()
    refresh_film_cache_from_source()
    return {
        "preview": preview_items,
        "updated": updated,
        "skipped": skipped,
        "failed": failed,
        "changed_titles": changed_titles,
    }


def build_tmdb_correction_plan():
    TMDB_LOOKUP_CACHE.clear()
    ensure_tmdb_enrichment_properties()
    pages = fetch_all_notion_database_pages()
    plan_items = []
    skipped = 0
    poster_changes = 0
    rating_changes = 0

    for page in pages:
        page_id = page.get("id", "")
        properties = page.get("properties", {}) or {}
        movie_title = extract_notion_page_title(properties)
        category = extract_notion_select_value(properties, "category")
        current_year_value = extract_notion_number_value(properties, "Year")
        if not movie_title or movie_title == "Untitled":
            skipped += 1
            continue
        try:
            tmdb_data = fetch_tmdb_enrichment(
                movie_title,
                category=category,
                year=current_year_value,
            )
        except (requests.RequestException, RuntimeError):
            skipped += 1
            continue
        if not tmdb_data:
            skipped += 1
            continue

        payload = build_tmdb_correction_payload(properties, tmdb_data)
        if not payload:
            skipped += 1
            continue

        current_poster_url = notion_url_value(properties.get("Poster URL"))
        current_rating = notion_number_value(properties.get("Rating"))
        next_poster_url = (tmdb_data.get("poster_url") or "").strip()
        next_rating_raw = tmdb_data.get("rating")
        next_rating = round(float(next_rating_raw), 1) if next_rating_raw not in (None, "") else None

        if "Poster URL" in payload:
            poster_changes += 1
        if "Rating" in payload:
            rating_changes += 1

        plan_items.append({
            "title": movie_title,
            "category": category,
            "page_id": page_id,
            "current_poster_url": current_poster_url,
            "next_poster_url": next_poster_url,
            "current_rating": current_rating,
            "next_rating": next_rating,
            "tmdb_type": tmdb_data.get("tmdb_type", ""),
            "payload": payload,
        })

    return {
        "summary": {
            "poster_changes": poster_changes,
            "rating_changes": rating_changes,
            "skipped": skipped,
            "planned_updates": len(plan_items),
        },
        "items": plan_items,
    }


def apply_tmdb_corrections():
    plan = build_tmdb_correction_plan()
    updated = 0
    skipped = 0
    failed = 0
    changed_titles = []

    for item in plan["items"]:
        try:
            response = requests.patch(
                f"https://api.notion.com/v1/pages/{item['page_id']}",
                headers=notion_api_headers(),
                json={"properties": item["payload"]},
                timeout=30
            )
            response.raise_for_status()
            updated += 1
            changed_titles.append(item["title"])
        except requests.RequestException:
            failed += 1

    skipped = plan["summary"]["skipped"]
    clear_runtime_cache()
    refresh_film_cache_from_source()
    return {
        "dry_run": plan["summary"],
        "updated": updated,
        "skipped": skipped,
        "failed": failed,
        "titles": changed_titles,
    }


def run_tmdb_sync_enrichment():
    if not (NOTION_TOKEN or "").strip():
        return {"status": "error", "message": "Missing NOTION_TOKEN."}, 500
    if not (NOTION_DATABASE_ID or "").strip():
        return {"status": "error", "message": "Missing NOTION_DATABASE_ID."}, 500
    if not (TMDB_API_KEY or "").strip():
        return {"status": "error", "message": "Missing TMDB_API_KEY."}, 500

    try:
        ensure_tmdb_enrichment_properties()
    except requests.RequestException as exc:
        return {"status": "error", "message": f"Failed to prepare Notion enrichment fields: {exc}"}, 502

    try:
        directors_database = ensure_directors_database()
        directors_database_id = directors_database.get("id", "")
        ensure_movie_director_relation_property(directors_database_id)
        ensure_directors_database_properties(directors_database_id)
        director_catalog = build_director_catalog()
    except (requests.RequestException, RuntimeError) as exc:
        return {"status": "error", "message": f"Failed to prepare Directors integration: {exc}"}, 502

    try:
        genres_database = ensure_genres_database()
        genres_database_id = genres_database.get("id", "")
        ensure_movie_genre_relation_property(genres_database_id)
        genre_catalog = build_genre_catalog()
    except (requests.RequestException, RuntimeError) as exc:
        return {"status": "error", "message": f"Failed to prepare Genres integration: {exc}"}, 502

    try:
        pages = fetch_all_notion_database_pages()
    except requests.RequestException as exc:
        return {"status": "error", "message": f"Failed to query Notion database: {exc}"}, 502

    TMDB_LOOKUP_CACHE.clear()
    TMDB_PERSON_LOOKUP_CACHE.clear()

    matched = []
    skipped = []
    updated = 0
    linked_movies = 0
    director_pages_created = []
    director_images_updated = []
    director_images_skipped = []
    movie_link_updates = 0
    genre_pages_created = []
    genre_link_updates = 0

    for page in pages:
        page_id = page.get("id", "")
        properties = page.get("properties", {}) or {}
        movie_title = extract_notion_page_title(properties)
        category = extract_notion_select_value(properties, "category")
        current_year_value = extract_notion_number_value(properties, "Year")
        if not movie_title or movie_title == "Untitled":
            skipped.append({"title": movie_title or "Untitled", "reason": "missing_title"})
            print("[sync-tmdb] skipped missing title")
            continue

        try:
            tmdb_data = fetch_tmdb_enrichment(movie_title, category=category, year=current_year_value)
        except requests.RequestException as exc:
            try:
                director_sync = sync_movie_directors(page, None, directors_database_id, director_catalog)
                if director_sync.get("status") == "linked":
                    linked_movies += 1
                    if director_sync.get("movie_link_updated"):
                        movie_link_updates += 1
                    director_pages_created.extend(director_sync.get("created_directors", []))
                    director_images_updated.extend(director_sync.get("director_images_updated", []))
                    director_images_skipped.extend(director_sync.get("director_images_skipped", []))
            except requests.RequestException:
                pass
            try:
                genre_sync = sync_movie_genres(page, None, genres_database_id, genre_catalog)
                if genre_sync.get("status") == "linked":
                    if genre_sync.get("movie_link_updated"):
                        genre_link_updates += 1
                    genre_pages_created.extend(genre_sync.get("created_genres", []))
            except requests.RequestException:
                pass
            skipped.append({"title": movie_title, "reason": f"tmdb_error: {exc}"})
            print(f"[sync-tmdb] skipped {safe_console_text(movie_title)}: TMDb request error")
            continue
        except RuntimeError as exc:
            return {"status": "error", "message": str(exc)}, 500

        if not tmdb_data:
            try:
                director_sync = sync_movie_directors(page, None, directors_database_id, director_catalog)
                if director_sync.get("status") == "linked":
                    linked_movies += 1
                    if director_sync.get("movie_link_updated"):
                        movie_link_updates += 1
                    director_pages_created.extend(director_sync.get("created_directors", []))
                    director_images_updated.extend(director_sync.get("director_images_updated", []))
                    director_images_skipped.extend(director_sync.get("director_images_skipped", []))
            except requests.RequestException as exc:
                skipped.append({"title": movie_title, "reason": f"director_sync_error: {exc}"})
            try:
                genre_sync = sync_movie_genres(page, None, genres_database_id, genre_catalog)
                if genre_sync.get("status") == "linked":
                    if genre_sync.get("movie_link_updated"):
                        genre_link_updates += 1
                    genre_pages_created.extend(genre_sync.get("created_genres", []))
            except requests.RequestException as exc:
                skipped.append({"title": movie_title, "reason": f"genre_sync_error: {exc}"})
            skipped.append({"title": movie_title, "reason": "no_tmdb_match"})
            print(f"[sync-tmdb] skipped {safe_console_text(movie_title)}: no TMDb match")
            continue

        update_payload = build_tmdb_notion_update_payload(properties, tmdb_data)
        if update_payload:
            try:
                update_notion_page_properties(page_id, update_payload)
                updated += 1
            except requests.RequestException as exc:
                skipped.append({"title": movie_title, "reason": f"notion_update_error: {exc}"})
                print(f"[sync-tmdb] skipped {safe_console_text(movie_title)}: Notion update error")
                continue

        try:
            director_sync = sync_movie_directors(page, tmdb_data, directors_database_id, director_catalog)
        except requests.RequestException as exc:
            skipped.append({"title": movie_title, "reason": f"director_sync_error: {exc}"})
            print(f"[sync-tmdb] skipped {safe_console_text(movie_title)}: director sync error")
            continue
        try:
            genre_sync = sync_movie_genres(page, tmdb_data, genres_database_id, genre_catalog)
        except requests.RequestException as exc:
            skipped.append({"title": movie_title, "reason": f"genre_sync_error: {exc}"})
            print(f"[sync-tmdb] skipped {safe_console_text(movie_title)}: genre sync error")
            continue

        if director_sync.get("status") == "linked":
            linked_movies += 1
            if director_sync.get("movie_link_updated"):
                movie_link_updates += 1
            director_pages_created.extend(director_sync.get("created_directors", []))
            director_images_updated.extend(director_sync.get("director_images_updated", []))
            director_images_skipped.extend(director_sync.get("director_images_skipped", []))
        else:
            skipped.append({"title": movie_title, "reason": director_sync.get("reason", "director_sync_skipped")})
        if genre_sync.get("status") == "linked":
            if genre_sync.get("movie_link_updated"):
                genre_link_updates += 1
            genre_pages_created.extend(genre_sync.get("created_genres", []))
        else:
            skipped.append({"title": movie_title, "reason": genre_sync.get("reason", "genre_sync_skipped")})

        matched.append({
            "title": movie_title,
            "year": tmdb_data.get("year"),
            "director": tmdb_data.get("director"),
            "rating": tmdb_data.get("rating"),
            "genres": tmdb_data.get("genres"),
            "overview_present": bool(str(tmdb_data.get("overview") or "").strip()),
            "top_billed_cast_count": len(tmdb_data.get("top_billed_cast") or []),
        })
        print(f"[sync-tmdb] matched {safe_console_text(movie_title)}")

    if updated or movie_link_updates or genre_link_updates or director_pages_created or genre_pages_created or director_images_updated:
        clear_runtime_cache()
        refresh_film_cache_from_source()

    return {
        "status": "success",
        "updated": updated,
        "movies_linked": linked_movies,
        "movie_link_updates": movie_link_updates,
        "director_pages_created": len(director_pages_created),
        "director_images_updated": len(director_images_updated),
        "director_images_skipped": len(director_images_skipped),
        "genre_pages_created": len(genre_pages_created),
        "genre_link_updates": genre_link_updates,
        "matched": matched,
        "skipped": skipped,
    }, 200

# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  YOUTUBE HELPERS
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
def extract_playlist_id(url):
    if "playlist?list=" in url: return url.split("list=")[-1].split("&")[0]
    return url

def load_duration_cache():
    global YOUTUBE_DURATION_CACHE
    if not DURATION_CACHE_PATH.exists():
        YOUTUBE_DURATION_CACHE = {}
        return
    try:
        YOUTUBE_DURATION_CACHE = json.loads(DURATION_CACHE_PATH.read_text(encoding="utf-8"))
    except Exception:
        YOUTUBE_DURATION_CACHE = {}

def save_duration_cache():
    try:
        DURATION_CACHE_PATH.write_text(json.dumps(YOUTUBE_DURATION_CACHE, indent=2), encoding="utf-8")
    except Exception:
        pass

def parse_iso8601_duration(value):
    match = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", value or "")
    if not match:
        return 0
    hours = int(match.group(1) or 0)
    minutes = int(match.group(2) or 0)
    seconds = int(match.group(3) or 0)
    return hours * 3600 + minutes * 60 + seconds

def format_duration(total_seconds):
    if not total_seconds:
        return "0:00"
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}:{minutes:02}:{seconds:02}"
    return f"{minutes}:{seconds:02}"


def _normalize_pockettube_feed_video_type(value, default="videos"):
    normalized = re.sub(r"\s+", " ", str(value or "").strip().lower())
    if normalized in {"videos", "live", "upcoming", "shorts"}:
        return normalized
    return default


def _normalize_pockettube_feed_order(value, default="normal"):
    normalized = re.sub(r"\s+", " ", str(value or "").strip().lower())
    if normalized in {"normal", "shuffle"}:
        return normalized
    return default


def _resolve_pockettube_feed_video_types(value, default=None):
    allowed = {"videos", "live", "upcoming", "shorts"}
    if isinstance(value, (list, tuple, set)):
        raw_values = list(value)
    else:
        raw_values = str(value or "").split(",")
    resolved = []
    for raw_value in raw_values:
        normalized = _normalize_pockettube_feed_video_type(raw_value, default="")
        if normalized in allowed and normalized not in resolved:
            resolved.append(normalized)
    if not resolved:
        fallback = default or ["videos"]
        return [item for item in fallback if item in allowed]
    return resolved


def _classify_pockettube_feed_video_type(video, cached_meta=None):
    cached_meta = cached_meta if isinstance(cached_meta, dict) else {}
    video = video if isinstance(video, dict) else {}
    cached_type = _normalize_pockettube_feed_video_type(cached_meta.get("video_type", ""), default="")
    if cached_type:
        return cached_type
    live_broadcast = _normalize_pockettube_feed_video_type(
        cached_meta.get("live_broadcast_content", "") or video.get("live_broadcast_content", ""),
        default="",
    )
    if live_broadcast in {"live", "upcoming"}:
        return live_broadcast
    title = str(video.get("title", "") or "").strip().lower()
    url = str(video.get("url", "") or "").strip().lower()
    duration_seconds = int(cached_meta.get("seconds", video.get("duration_seconds", 0)) or 0)
    if "/shorts/" in url or "#shorts" in title or (duration_seconds and duration_seconds <= 60 and "short" in title):
        return "shorts"
    return "videos"


def _pockettube_feed_sort_key(item):
    item = item if isinstance(item, dict) else {}
    published_at = parse_timestamp(str(item.get("published_at", "") or "").strip())
    published_value = published_at.timestamp() if published_at else None
    feed_order_index = int(item.get("feed_order_index", 0) or 0)
    channel_key = normalize_section_name(item.get("channel_key", "") or item.get("channel_name", "") or "")
    video_id = str(item.get("video_id", "") or "").strip()
    entry_id = str(item.get("entry_id", "") or "").strip()
    return (
        0 if published_value is not None else 1,
        -(published_value or 0),
        feed_order_index,
        channel_key,
        video_id,
        entry_id,
    )


def _build_pockettube_shuffle_pool(feed_context):
    context = feed_context if isinstance(feed_context, dict) else {}
    channels = list(context.get("channels", []) or [])
    pool = []
    for index, channel in enumerate(channels):
        channel = channel if isinstance(channel, dict) else {}
        latest_video = channel.get("latest_video") if isinstance(channel.get("latest_video"), dict) else {}
        if not latest_video:
            videos = channel.get("videos", []) if isinstance(channel.get("videos", []), list) else []
            latest_video = videos[0] if videos and isinstance(videos[0], dict) else {}
        if not latest_video:
            continue
        item = dict(latest_video)
        item.setdefault("section_name", context.get("name", ""))
        item.setdefault("section_kind", context.get("section_kind", ""))
        item.setdefault("section_scope", context.get("section_scope", ""))
        item.setdefault("channel_group_key", context.get("channel_group_key", ""))
        item.setdefault("channel_group_label", context.get("channel_group_label", ""))
        item.setdefault("group_name", context.get("group_name", ""))
        item.setdefault("group_key", context.get("group_key", ""))
        item.setdefault("channel_key", channel.get("channel_key", ""))
        item.setdefault("channel_id", channel.get("channel_id", ""))
        item.setdefault("tier", channel.get("tier", ""))
        item.setdefault("notes", channel.get("notes", ""))
        item.setdefault("source", channel.get("source", item.get("source", "pockettube")))
        entry_id = str(item.get("entry_id", "") or "").strip()
        if not entry_id:
            playlist_item_id = str(item.get("playlist_item_id", "") or "").strip()
            video_id = str(item.get("video_id", "") or "").strip()
            entry_id = f"yt-{playlist_item_id or video_id}" if (playlist_item_id or video_id) else ""
            if entry_id:
                item["entry_id"] = entry_id
        if entry_id and not item.get("detail_url"):
            item["detail_url"] = f"/video/{entry_id}"
        item.setdefault("feed_order_index", index)
        pool.append(item)
    if pool:
        return pool
    fallback = []
    for index, item in enumerate(list(context.get("feed_all", []) or context.get("feed_preview", []) or context.get("feed_items", []) or [])):
        if not isinstance(item, dict):
            continue
        clone = dict(item)
        clone.setdefault("feed_order_index", index)
        fallback.append(clone)
    return fallback


def _enrich_pockettube_feed_videos(videos, fetch_missing=True):
    items = [dict(video) for video in (videos or []) if isinstance(video, dict)]
    video_ids = [str(item.get("video_id", "") or "").strip() for item in items if str(item.get("video_id", "") or "").strip()]
    if fetch_missing and video_ids and YOUTUBE_API_KEY:
        missing_video_ids = [
            video_id for video_id in video_ids
            if not isinstance(YOUTUBE_DURATION_CACHE.get(video_id, {}), dict)
            or "live_broadcast_content" not in (YOUTUBE_DURATION_CACHE.get(video_id, {}) or {})
        ]
        if missing_video_ids:
            fetch_youtube_video_metadata(missing_video_ids)
    enriched = []
    for item in items:
        video_id = str(item.get("video_id", "") or "").strip()
        cached_meta = YOUTUBE_DURATION_CACHE.get(video_id, {}) if video_id else {}
        duration_info = cached_meta if isinstance(cached_meta, dict) else {}
        thumb = str(item.get("thumb", "") or "").strip()
        thumbnail_url = str(item.get("thumbnail_url", "") or "").strip() or thumb
        thumbnail = str(item.get("thumbnail", "") or "").strip() or thumbnail_url
        image_url = str(item.get("image_url", "") or "").strip() or thumbnail_url
        item["thumb"] = thumb
        item["thumbnail_url"] = thumbnail_url
        item["thumbnail"] = thumbnail
        item["image_url"] = image_url
        item["duration_seconds"] = duration_info.get("seconds", item.get("duration_seconds", 0))
        if not item.get("duration"):
            item["duration"] = duration_info.get("display", item.get("duration", "0:00"))
        published_at = str(item.get("published_at", "") or "").strip()
        item["published_at"] = published_at
        item["published_display"] = str(item.get("published_display", "") or "").strip() or format_timestamp_label(published_at, default="")
        if not item.get("entry_id"):
            playlist_item_id = str(item.get("playlist_item_id", "") or "").strip()
            item["entry_id"] = f"yt-{playlist_item_id or video_id}" if (playlist_item_id or video_id) else ""
        item["watch_key"] = video_id or str(item.get("playlist_item_id", "") or "").strip() or item.get("entry_id", "")
        item["state_key"] = item["watch_key"]
        if not item.get("detail_url") and item.get("entry_id"):
            item["detail_url"] = f"/video/{item['entry_id']}"
        item["live_broadcast_content"] = str(
            duration_info.get("live_broadcast_content", item.get("live_broadcast_content", "")) or ""
        ).strip().lower()
        item["video_type"] = _classify_pockettube_feed_video_type(item, duration_info)
        enriched.append(item)
    return enriched


def fetch_youtube_video_metadata(video_ids):
    missing = [video_id for video_id in video_ids if video_id and video_id not in YOUTUBE_DURATION_CACHE]
    if not missing:
        return
    for start in range(0, len(missing), 50):
        chunk = missing[start:start + 50]
        params = {
            "part": "contentDetails,snippet,liveStreamingDetails",
            "id": ",".join(chunk),
            "key": YOUTUBE_API_KEY
        }
        try:
            response = requests.get("https://www.googleapis.com/youtube/v3/videos", params=params, timeout=15)
        except Exception:
            continue
        if response.status_code != 200:
            continue
        data = response.json()
        found = set()
        for item in data.get("items", []):
            snippet = item.get("snippet", {}) or {}
            live_details = item.get("liveStreamingDetails", {}) or {}
            duration_seconds = parse_iso8601_duration(item.get("contentDetails", {}).get("duration", ""))
            YOUTUBE_DURATION_CACHE[item["id"]] = {
                "seconds": duration_seconds,
                "display": format_duration(duration_seconds),
                "live_broadcast_content": str(snippet.get("liveBroadcastContent", "") or "").strip().lower(),
                "published_at": str(snippet.get("publishedAt", "") or "").strip(),
                "scheduled_start_time": str(live_details.get("scheduledStartTime", "") or "").strip(),
                "actual_start_time": str(live_details.get("actualStartTime", "") or "").strip(),
                "video_type": _classify_pockettube_feed_video_type(
                    {
                        "title": snippet.get("title", ""),
                        "url": "",
                    },
                    {
                        "seconds": duration_seconds,
                        "live_broadcast_content": snippet.get("liveBroadcastContent", ""),
                    },
                ),
            }
            found.add(item["id"])
        for video_id in chunk:
            if video_id not in found:
                YOUTUBE_DURATION_CACHE[video_id] = {"seconds": 0, "display": "0:00"}
    save_duration_cache()

def get_youtube_duration(video_id):
    if video_id not in YOUTUBE_DURATION_CACHE:
        fetch_youtube_video_metadata([video_id])
    return YOUTUBE_DURATION_CACHE.get(video_id, {"seconds": 0, "display": "0:00"})

def build_pagination(current_page, total_pages, window=2):
    pages = []
    if total_pages <= 1:
        return [1]
    for number in range(1, total_pages + 1):
        if number == 1 or number == total_pages or abs(number - current_page) <= window:
            pages.append(number)
    compact = []
    previous = None
    for number in pages:
        if previous and number - previous > 1:
            compact.append("...")
        compact.append(number)
        previous = number
    return compact

def paginate_items(items, page, per_page):
    total = len(items)
    total_pages = max(math.ceil(total / per_page), 1)
    page = min(max(page, 1), total_pages)
    start = (page - 1) * per_page
    return {
        "items": items[start:start + per_page],
        "page": page,
        "total": total,
        "total_pages": total_pages,
        "pagination": build_pagination(page, total_pages)
    }

load_duration_cache()


def build_shuffled_related_entries(entries, seed_value):
    shuffled = list(entries)
    rng = random.Random(str(seed_value))
    for index in range(len(shuffled) - 1, 0, -1):
        swap_index = rng.randint(0, index)
        shuffled[index], shuffled[swap_index] = shuffled[swap_index], shuffled[index]
    return shuffled


def build_related_video_detail_url(entry_id, related_order="normal", related_seed=""):
    route_args = {"entry_id": entry_id}
    if related_order == "shuffle":
        route_args["related_order"] = "shuffle"
        if related_seed:
            route_args["related_seed"] = related_seed
    return url_for("video_detail", **route_args)


def adapt_german_playlist_for_shared_render(item):
    if not isinstance(item, dict):
        return {"name": "", "id": "", "url": ""}
    adapted = dict(item)
    source = str(adapted.get("source", "") or "").strip().lower()
    category_key = str(adapted.get("source_category_key", "") or "").strip()
    name = str(adapted.get("name", "") or "").strip()
    if normalize_section_name(adapted.get("source_section_key", "")) == normalize_section_name("German") and source == "admin":
        category_label = category_key or "Admin"
        adapted["source_category_key"] = category_label
        if name and ":" not in name:
            adapted["name"] = f"{category_label}: {name}"
    return adapted

def load_playlists():
    default = {"German": [], "Chess": [], "Library": [], "YouTube Watch Later": []}
    playlists = {}
    for section in build_combined_sections():
        display_name = section.get("name", "")
        wanted = normalize_section_name(display_name)
        playlists[display_name] = [
            {
                "name": adapted_item.get("name", ""),
                "id": adapted_item.get("id", ""),
                "url": adapted_item.get("url", ""),
                "source": adapted_item.get("source", ""),
                "source_section_key": adapted_item.get("source_section_key", ""),
                "source_category_key": adapted_item.get("source_category_key", ""),
            }
            for item in section.get("playlists", [])
            for adapted_item in [adapt_german_playlist_for_shared_render(item) if wanted == normalize_section_name("German") else item]
        ]
    for key in default:
        if key not in playlists:
            playlists[key] = []
    return playlists


def fetch_all_films(force_refresh=False):
    with RUNTIME_CACHE_LOCK:
        if RUNTIME_CACHE["films"] is not None and not force_refresh:
            films = clone_film_rows(RUNTIME_CACHE["films"])
            print(f"[movie-source-cache] key=all source=runtime rows={len(films)}")
            return films

    cached_result = get_persisted_film_cache_entry("all", force_refresh=force_refresh, allow_stale=True)
    cached_films, cached_stale = cached_result if isinstance(cached_result, tuple) else (None, None)
    if cached_films is not None:
        with RUNTIME_CACHE_LOCK:
            RUNTIME_CACHE["films"] = clone_film_rows(cached_films)
        if cached_stale:
            schedule_movie_cache_refresh("films", refresh_film_cache_from_source)
        return clone_film_rows(cached_films)

    films = fetch_all_films_from_notion()
    clear_film_cache_entry()
    clear_runtime_film_cache_keys()
    set_persisted_film_cache_entry("all", films)
    print(f"[movie-source-cache] key=all source=fresh rows={len(films)}")
    with RUNTIME_CACHE_LOCK:
        RUNTIME_CACHE["films"] = clone_film_rows(films)
    return clone_film_rows(films)


def movie_audit_title_from_row(row):
    if not isinstance(row, dict):
        return ""
    for key in ("name", "title", "Name", "sheet_title", "db_title", "matched_title"):
        value = str(row.get(key, "") or "").strip()
        if value:
            return value
    return ""


def movie_audit_count_values(rows, field):
    counts = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        value = str(row.get(field, "") or "").strip() or "(blank)"
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0].lower())))


def movie_audit_rows_from_json_payload(payload):
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if not isinstance(payload, dict):
        return []
    rows = []
    main_keys = ("items", "movie_pages", "updated_items", "created_items", "already_existing_items", "weak_or_unclear_items")
    sample_keys = ("sample_created", "sample_my_library", "sample_eberts_library", "sample_my_library_and_eberts")
    keys_to_scan = main_keys if any(isinstance(payload.get(key), list) for key in main_keys) else sample_keys
    for key in keys_to_scan:
        value = payload.get(key)
        if not isinstance(value, list):
            continue
        for item in value:
            if isinstance(item, dict):
                rows.append(item)
            elif isinstance(item, str) and item.strip():
                rows.append({"title": item.strip()})
    return rows


def movie_audit_load_rows(path):
    try:
        if path.suffix.lower() == ".json":
            return movie_audit_rows_from_json_payload(load_json_file(path, {}))
        if path.suffix.lower() == ".csv":
            with path.open("r", encoding="utf-8-sig", newline="") as handle:
                return [row for row in csv.DictReader(handle)]
    except Exception as exc:
        return [{"title": "", "load_error": f"{type(exc).__name__}: {exc}"}]
    return []


def movie_audit_candidate_files():
    candidates = []
    explicit_paths = [
        EXPORTS_DIR / "movies_export.json",
        EXPORTS_DIR / "movies_export.csv",
        BASE_DIR / "cache_data.json",
        BASE_DIR / "backups" / "notion_movies_export.json",
        BASE_DIR / "backups" / "notion_movies_export.csv",
    ]
    for path in explicit_paths:
        if path.exists() and path.is_file():
            candidates.append(path)
    if CORRECTION_REPORTS_DIR.exists():
        candidates.extend(
            path for path in sorted(CORRECTION_REPORTS_DIR.glob("*.json"))
            if not path.name.startswith("movie-source-comparison-audit-")
        )
    unique_paths = []
    seen = set()
    for path in candidates:
        resolved = str(path.resolve())
        if resolved in seen:
            continue
        seen.add(resolved)
        unique_paths.append(path)
    return sorted(unique_paths, key=lambda item: item.stat().st_mtime, reverse=True)
def compact_notion_id(value):
    return re.sub(r"[^a-f0-9]+", "", str(value or "").strip().lower())


def feature_flag_enabled(value):
    return str(value or "").strip().lower() in {"1", "true", "yes", "on", "enabled"}


def movie_want_to_union_fetch_enabled():
    return feature_flag_enabled(config_value(MOVIE_WANT_TO_UNION_FETCH_FLAG_NAME, MOVIE_WANT_TO_UNION_FETCH_ENABLED))


def notion_property_display_value(prop):
    if not isinstance(prop, dict):
        return ""
    ptype = prop.get("type", "")
    if ptype == "title":
        return "".join(part.get("plain_text", "") for part in prop.get("title", [])).strip()
    if ptype == "rich_text":
        return "".join(part.get("plain_text", "") for part in prop.get("rich_text", [])).strip()
    if ptype == "select":
        select_value = prop.get("select")
        return select_value.get("name", "").strip() if isinstance(select_value, dict) else ""
    if ptype == "number":
        number_value = prop.get("number")
        return "" if number_value in (None, "") else number_value
    if ptype == "multi_select":
        return ", ".join(item.get("name", "").strip() for item in prop.get("multi_select", []) if item.get("name"))
    if ptype == "url":
        return prop.get("url") or ""
    return ""


def query_notion_movie_pages_experiment(query_payload=None):
    payload = json.loads(json.dumps(query_payload or {}))
    payload.setdefault("page_size", 100)
    pages = []
    page_count = 0
    error = ""
    exhausted = False
    try:
        while True:
            page_count += 1
            response = requests.post(
                f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query",
                headers=notion_api_headers(),
                json=payload,
                timeout=30
            )
            try:
                data = response.json() or {}
            except ValueError as exc:
                error = f"Invalid JSON on page {page_count}: {exc}"
                break
            if response.status_code >= 400:
                error = data.get("message", response.text[:500])
                break
            pages.extend(data.get("results", []) or [])
            if not data.get("has_more"):
                exhausted = True
                break
            next_cursor = data.get("next_cursor")
            if not next_cursor:
                error = f"Stopped on page {page_count}: has_more was true but next_cursor was missing."
                break
            payload["start_cursor"] = next_cursor
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
    distinct_ids = {compact_notion_id(page.get("id")) for page in pages if compact_notion_id(page.get("id"))}
    return {
        "pages": pages,
        "total_rows": len(pages),
        "distinct_page_count": len(distinct_ids),
        "page_count": page_count,
        "has_more_exhausted": exhausted,
        "error": error,
    }


def movie_fetch_experiment_page_summary(page):
    props = (page or {}).get("properties", {}) or {}
    return {
        "page_id": page.get("id", ""),
        "title": notion_property_display_value(props.get("Name")),
        "status": notion_property_display_value(props.get("Status")),
        "source": notion_property_display_value(props.get("source")),
        "category": notion_property_display_value(props.get("category")),
        "director": notion_property_display_value(props.get("Director")),
        "year": notion_property_display_value(props.get("Year")),
        "created_time": page.get("created_time", ""),
        "last_edited_time": page.get("last_edited_time", ""),
    }


def notion_movie_page_to_film_row(page, directors_by_page_id=None, genres_by_page_id=None):
    props = (page or {}).get("properties", {}) or {}
    directors_by_page_id = directors_by_page_id or {}
    genres_by_page_id = genres_by_page_id or {}

    def _prop(key):
        return props.get(key, {}) or {}

    def _text(key):
        prop = _prop(key)
        ptype = prop.get("type", "")
        if ptype == "title":
            parts = prop.get("title", [])
        elif ptype == "rich_text":
            parts = prop.get("rich_text", [])
        elif ptype == "url":
            return prop.get("url") or ""
        else:
            parts = []
        return "".join(t.get("plain_text", "") for t in parts).strip()

    def _number(key):
        prop = _prop(key)
        if prop.get("type") != "number":
            return None
        return prop.get("number")

    def _select(key):
        select_value = _prop(key).get("select")
        return select_value.get("name", "").strip() if isinstance(select_value, dict) else ""

    def _multi_select(key):
        return ", ".join(
            item.get("name", "").strip()
            for item in _prop(key).get("multi_select", []) or []
            if item.get("name")
        )

    def _date(key):
        date_value = _prop(key).get("date")
        return date_value.get("start", "") if isinstance(date_value, dict) else ""

    def _files_url(key):
        file_prop = _prop(key)
        if file_prop.get("type") != "files":
            return ""
        files = file_prop.get("files", []) or []
        if not files:
            return ""
        first_file = files[0] or {}
        if first_file.get("type") == "external":
            return first_file.get("external", {}).get("url", "")
        if first_file.get("type") == "file":
            return first_file.get("file", {}).get("url", "")
        return ""

    def _relation_ids(key):
        prop = _prop(key)
        if prop.get("type") != "relation":
            return []
        return [
            str(item.get("id") or "").strip()
            for item in prop.get("relation", []) or []
            if str(item.get("id") or "").strip()
        ]

    name = _text("Name")
    if not name:
        return {}

    year_value = _number("Year")
    runtime_value = _number("Runtime")
    director_relation_ids = _relation_ids(DIRECTOR_RELATION_PROPERTY)
    genre_relation_ids = _relation_ids(GENRE_RELATION_PROPERTY)
    director_entries = []
    for relation_id in director_relation_ids:
        director_record = directors_by_page_id.get(relation_id)
        if director_record:
            director_entries.append({
                "page_id": director_record.get("page_id", ""),
                "name": director_record.get("display_name", ""),
                "image_url": director_record.get("image_url", ""),
                "page_url": director_record.get("page_url", ""),
            })
    genre_entries = []
    for relation_id in genre_relation_ids:
        genre_record = genres_by_page_id.get(relation_id)
        if genre_record:
            genre_entries.append({
                "page_id": genre_record.get("page_id", ""),
                "name": genre_record.get("display_name", ""),
                "page_url": genre_record.get("page_url", ""),
            })

    return {
        "notion_page_id": page.get("id", ""),
        "name": name,
        "poster": _files_url("poster ") or _text("Poster URL"),
        "score": _select("Score /5"),
        "score_num": SCORE_ORDER.get(_select("Score /5"), 0),
        "status": _select("Status"),
        "source": normalize_movie_source(_select("source")),
        "category": _select("category"),
        "watch_date": _date("watching history"),
        "finish_date": _date("finishing history"),
        "rewatch": _select("I will watch it again"),
        "trailer": _text("Trailer"),
        "year": int(year_value) if isinstance(year_value, (int, float)) else (str(year_value).strip() if year_value not in (None, "") else ""),
        "director": _text("Director"),
        "director_relation_ids": director_relation_ids,
        "director_entries": director_entries,
        "genre_relation_ids": genre_relation_ids,
        "genre_entries": genre_entries,
        "genres": _multi_select("Genres") or _text("Genres"),
        "runtime": int(runtime_value) if isinstance(runtime_value, (int, float)) else (str(runtime_value).strip() if runtime_value not in (None, "") else ""),
        "overview": _text("Overview") or _text("Synopsis") or _text("Description"),
        "tmdb_rating": _number("Rating") if _number("Rating") not in (None, "") else "",
    }


def movie_fetch_count_values(rows, field):
    counts = {}
    for row in rows:
        value = str((row or {}).get(field, "") or "").strip() or "(blank)"
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0].lower())))


def movie_fetch_distribution_summary(rows):
    return {
        "status": movie_fetch_count_values(rows, "status"),
        "source": movie_fetch_count_values(rows, "source"),
        "category": movie_fetch_count_values(rows, "category"),
    }


def movie_fetch_experiment_strategies():
    strategies = [
        {"name": "production_unfiltered_no_sort", "group": "baseline", "payload": {}},
        {"name": "created_time_ascending", "group": "sort", "payload": {"sorts": [{"timestamp": "created_time", "direction": "ascending"}]}},
        {"name": "created_time_descending", "group": "sort", "payload": {"sorts": [{"timestamp": "created_time", "direction": "descending"}]}},
        {"name": "last_edited_time_ascending", "group": "sort", "payload": {"sorts": [{"timestamp": "last_edited_time", "direction": "ascending"}]}},
        {"name": "last_edited_time_descending", "group": "sort", "payload": {"sorts": [{"timestamp": "last_edited_time", "direction": "descending"}]}},
        {"name": "title_ascending", "group": "sort", "payload": {"sorts": [{"property": "Name", "direction": "ascending"}]}},
        {"name": "title_descending", "group": "sort", "payload": {"sorts": [{"property": "Name", "direction": "descending"}]}},
        {"name": "year_ascending", "group": "sort", "payload": {"sorts": [{"property": "Year", "direction": "ascending"}]}},
        {"name": "year_descending", "group": "sort", "payload": {"sorts": [{"property": "Year", "direction": "descending"}]}},
    ]
    status_values = ("i want to", "Finished")
    source_values = ("Ebert's library", "My library", "My library and Ebert's")
    category_values = ("movie", "tv show", "anime", "short movie")
    for value in status_values:
        strategies.append({
            "name": f"status_equals_{normalized_match_key(value) or 'blank'}",
            "group": "status_partition",
            "payload": {"filter": {"property": "Status", "select": {"equals": value}}},
        })
    for value in source_values:
        strategies.append({
            "name": f"source_equals_{normalized_match_key(value) or 'blank'}",
            "group": "source_partition",
            "payload": {"filter": {"property": "source", "select": {"equals": value}}},
        })
    for value in category_values:
        strategies.append({
            "name": f"category_equals_{normalized_match_key(value) or 'blank'}",
            "group": "category_partition",
            "payload": {"filter": {"property": "category", "select": {"equals": value}}},
        })
    return strategies


def movie_fetch_anchor_coverage(page_summaries):
    title_lookup = {
        normalized_match_key(item.get("title", "")): item
        for item in page_summaries
        if normalized_match_key(item.get("title", ""))
    }
    results = []
    for title in MOVIE_FETCH_EXPERIMENT_ANCHOR_TITLES:
        title_key = normalized_match_key(title)
        results.append({
            "title": title,
            "included": title_key in title_lookup,
            "match": title_lookup.get(title_key, {}),
        })
    return results


def movie_summary_is_want_to(summary):
    return normalize_movie_status((summary or {}).get("status", "")) == "iwantto"


def run_movie_want_to_union_compare_once(strategy_names=None):
    wanted_strategy_names = set(strategy_names or MOVIE_WANT_TO_UNION_COMPARE_STRATEGIES)
    strategies = [
        strategy for strategy in movie_fetch_experiment_strategies()
        if strategy["name"] in wanted_strategy_names
    ]
    union_pages = {}
    strategy_reports = []
    for strategy in strategies:
        result = query_notion_movie_pages_experiment(strategy["payload"])
        pages = result.get("pages", []) or []
        new_ids = []
        for page in pages:
            page_id = compact_notion_id(page.get("id"))
            if page_id and page_id not in union_pages:
                union_pages[page_id] = page
                new_ids.append(page_id)
        strategy_reports.append({
            "name": strategy["name"],
            "group": strategy["group"],
            "total_rows": result["total_rows"],
            "distinct_page_count": result["distinct_page_count"],
            "new_pages_added_to_union": len(new_ids),
            "page_count": result["page_count"],
            "has_more_exhausted": result["has_more_exhausted"],
            "error": result["error"],
        })

    union_summaries = [movie_fetch_experiment_page_summary(page) for page in union_pages.values()]
    want_to_summaries = [summary for summary in union_summaries if movie_summary_is_want_to(summary)]
    return {
        "strategy_names": [strategy["name"] for strategy in strategies],
        "strategy_reports": strategy_reports,
        "union_total_after_dedupe": len(union_pages),
        "want_to_total_after_filter": len(want_to_summaries),
        "union_distributions": movie_fetch_distribution_summary(union_summaries),
        "want_to_distributions": movie_fetch_distribution_summary(want_to_summaries),
        "anchor_titles": movie_fetch_anchor_coverage(want_to_summaries),
        "want_to_sample_titles": [item.get("title", "") for item in want_to_summaries[:30]],
    }


def fetch_want_to_films_from_union_source(force_refresh=False, save_report=False):
    with RUNTIME_CACHE_LOCK:
        cached = RUNTIME_CACHE.get("want_to_union_films")
        if cached is not None and not force_refresh:
            films = clone_film_rows(cached)
            print(f"[movie-source-cache] key=want_to_union source=runtime rows={len(films)}")
            return films

    cached_result = get_persisted_film_cache_entry("want_to_union", force_refresh=force_refresh, allow_stale=True)
    cached_films, cached_stale = cached_result if isinstance(cached_result, tuple) else (None, None)
    if cached_films is not None and not save_report:
        with RUNTIME_CACHE_LOCK:
            RUNTIME_CACHE["want_to_union_films"] = clone_film_rows(cached_films)
        if cached_stale and not is_movie_cache_refresh_pending("films"):
            schedule_movie_cache_refresh(
                "want_to_union",
                lambda: fetch_want_to_films_from_union_source(force_refresh=True, save_report=save_report),
            )
        return clone_film_rows(cached_films)

    wanted_strategy_names = set(MOVIE_WANT_TO_UNION_COMPARE_STRATEGIES)
    strategies = [
        strategy for strategy in movie_fetch_experiment_strategies()
        if strategy["name"] in wanted_strategy_names
    ]
    union_pages = {}
    strategy_reports = []
    for strategy in strategies:
        result = query_notion_movie_pages_experiment(strategy["payload"])
        pages = result.get("pages", []) or []
        new_ids = []
        for page in pages:
            page_id = compact_notion_id(page.get("id"))
            if page_id and page_id not in union_pages:
                union_pages[page_id] = page
                new_ids.append(page_id)
        strategy_reports.append({
            "name": strategy["name"],
            "total_rows": result["total_rows"],
            "distinct_page_count": result["distinct_page_count"],
            "new_pages_added_to_union": len(new_ids),
            "error": result["error"],
        })

    director_catalog = build_director_catalog()
    genre_catalog = build_genre_catalog()
    directors_by_page_id = director_catalog.get("records_by_page_id", {})
    genres_by_page_id = genre_catalog.get("records_by_page_id", {})
    films = []
    for page in union_pages.values():
        film = notion_movie_page_to_film_row(
            page,
            directors_by_page_id=directors_by_page_id,
            genres_by_page_id=genres_by_page_id,
        )
        if not film:
            continue
        if normalize_movie_status(film.get("status", "")) != "iwantto":
            continue
        films.append(film)

    films.sort(key=lambda item: (str(item.get("name", "")).lower(), compact_notion_id(item.get("notion_page_id"))))
    clear_film_cache_entry(keys=("library_union_enabled", "library_union_disabled"))
    clear_runtime_film_cache_keys(keys=("library_union_enabled", "library_union_disabled"))
    set_persisted_film_cache_entry("want_to_union", films)
    print(f"[movie-source-cache] key=want_to_union source=fresh rows={len(films)}")
    with RUNTIME_CACHE_LOCK:
        RUNTIME_CACHE["want_to_union_films"] = clone_film_rows(films)

    if save_report:
        report = {
            "created_at": current_timestamp(),
            "feature_flag_name": MOVIE_WANT_TO_UNION_FETCH_FLAG_NAME,
            "feature_flag_enabled": movie_want_to_union_fetch_enabled(),
            "strategy_names": [strategy["name"] for strategy in strategies],
            "dedupe_rule": "compact Notion page ID",
            "filter_rule": 'normalize_movie_status(Status) == "iwantto"',
            "raw_union_total": len(union_pages),
            "filtered_want_to_total": len(films),
            "anchor_titles": movie_fetch_anchor_coverage([
                {
                    "title": film.get("name", ""),
                    "status": film.get("status", ""),
                    "source": film.get("source", ""),
                    "category": film.get("category", ""),
                    "page_id": film.get("notion_page_id", ""),
                }
                for film in films
            ]),
            "distributions": movie_fetch_distribution_summary([
                {
                    "status": film.get("status", ""),
                    "source": film.get("source", ""),
                    "category": film.get("category", ""),
                }
                for film in films
            ]),
            "strategy_reports": strategy_reports,
        }
        save_correction_report(report, "movie-want-to-union-fetch")

    return [dict(film) for film in films]


def fetch_want_to_films_for_flagged_paths(force_refresh=False):
    if movie_want_to_union_fetch_enabled():
        return fetch_want_to_films_from_union_source(force_refresh=force_refresh)
    return [
        dict(film) for film in fetch_all_films(force_refresh=force_refresh)
        if normalize_movie_status(film.get("status", "")) == "iwantto"
    ]


def fetch_library_films_for_flagged_paths(force_refresh=False):
    cache_key = "library_union_enabled" if movie_want_to_union_fetch_enabled() else "library_union_disabled"
    with RUNTIME_CACHE_LOCK:
        cached = RUNTIME_CACHE.get("library_films", {}).get(cache_key)
        if cached is not None and not force_refresh:
            films = clone_film_rows(cached)
            print(f"[movie-source-cache] key={cache_key} source=runtime rows={len(films)}")
            return films

    cached_result = get_persisted_film_cache_entry(cache_key, force_refresh=force_refresh, allow_stale=True)
    cached_films, cached_stale = cached_result if isinstance(cached_result, tuple) else (None, None)
    if cached_films is not None:
        with RUNTIME_CACHE_LOCK:
            RUNTIME_CACHE.setdefault("library_films", {})[cache_key] = clone_film_rows(cached_films)
        if cached_stale:
            schedule_movie_cache_refresh(
                "films",
                refresh_film_cache_from_source,
            )
        return clone_film_rows(cached_films)

    production_films = fetch_all_films(force_refresh=force_refresh)
    if not movie_want_to_union_fetch_enabled():
        films = clone_film_rows(production_films)
        set_persisted_film_cache_entry(cache_key, films)
        with RUNTIME_CACHE_LOCK:
            RUNTIME_CACHE.setdefault("library_films", {})[cache_key] = clone_film_rows(films)
        print(f"[movie-source-cache] key={cache_key} source=fresh rows={len(films)}")
        return clone_film_rows(films)

    merged = {}
    for film in production_films:
        page_id = compact_notion_id(film.get("notion_page_id"))
        key = page_id or normalized_match_key(movie_audit_title_from_row(film))
        if key:
            merged[key] = dict(film)
    for film in fetch_want_to_films_from_union_source(force_refresh=force_refresh):
        page_id = compact_notion_id(film.get("notion_page_id"))
        key = page_id or normalized_match_key(movie_audit_title_from_row(film))
        if key:
            merged[key] = dict(film)
    films = list(merged.values())
    set_persisted_film_cache_entry(cache_key, films)
    with RUNTIME_CACHE_LOCK:
        RUNTIME_CACHE.setdefault("library_films", {})[cache_key] = clone_film_rows(films)
    print(f"[movie-source-cache] key={cache_key} source=fresh rows={len(films)}")
    return clone_film_rows(films)


def build_movie_want_to_union_compare(runs=2, save_report=True):
    try:
        run_count = int(runs)
    except (TypeError, ValueError):
        run_count = 2
    run_count = max(1, min(run_count, 3))

    current_rows = fetch_all_films_from_notion()
    current_want_to_rows = [
        row for row in current_rows
        if normalize_movie_status(row.get("status", "")) == "iwantto"
    ]
    current_want_to_summaries = [
        {
            "page_id": row.get("notion_page_id", ""),
            "title": movie_audit_title_from_row(row),
            "status": str(row.get("status", "") or "").strip(),
            "source": str(row.get("source", "") or "").strip(),
            "category": str(row.get("category", "") or "").strip(),
            "director": str(row.get("director", "") or "").strip(),
            "year": row.get("year", ""),
        }
        for row in current_want_to_rows
    ]

    runs_report = []
    for index in range(run_count):
        run = run_movie_want_to_union_compare_once()
        run["run_number"] = index + 1
        runs_report.append(run)

    first_run = runs_report[0] if runs_report else {}
    want_counts = [run.get("want_to_total_after_filter", 0) for run in runs_report]
    union_counts = [run.get("union_total_after_dedupe", 0) for run in runs_report]
    anchor_stability = [
        all(item.get("included") for item in run.get("anchor_titles", []))
        for run in runs_report
    ]
    stable_across_repeated_runs = (
        bool(runs_report)
        and len(set(want_counts)) == 1
        and len(set(union_counts)) == 1
        and all(anchor_stability)
    )
    report = {
        "created_at": current_timestamp(),
        "notion_database_id": NOTION_DATABASE_ID,
        "mode": "admin_only_compare",
        "feature_flag_name": MOVIE_WANT_TO_UNION_FETCH_FLAG_NAME,
        "feature_flag_enabled": movie_want_to_union_fetch_enabled(),
        "flagged_usage": [
            "home want-to count",
            "watch-next / want-to recommendation pool",
            "Movies library/search merged production + union want-to source",
        ],
        "strategy_names": list(MOVIE_WANT_TO_UNION_COMPARE_STRATEGIES),
        "normalized_filter_rule": 'normalize_movie_status(Status) == "iwantto"',
        "dedupe_rule": "compact Notion page ID",
        "repeated_run_count": run_count,
        "current_production_total": len(current_rows),
        "current_production_want_to_count": len(current_want_to_rows),
        "flagged_want_to_count_when_enabled": first_run.get("want_to_total_after_filter", 0),
        "want_to_delta_when_enabled": first_run.get("want_to_total_after_filter", 0) - len(current_want_to_rows),
        "current_production_want_to_distributions": movie_fetch_distribution_summary(current_want_to_summaries),
        "union_total_after_dedupe": first_run.get("union_total_after_dedupe", 0),
        "union_filtered_want_to_count": first_run.get("want_to_total_after_filter", 0),
        "union_filtered_want_to_distributions": first_run.get("want_to_distributions", {}),
        "union_raw_distributions": first_run.get("union_distributions", {}),
        "anchor_titles": first_run.get("anchor_titles", []),
        "runs": [
            {
                "run_number": run.get("run_number"),
                "union_total_after_dedupe": run.get("union_total_after_dedupe"),
                "want_to_total_after_filter": run.get("want_to_total_after_filter"),
                "all_anchor_titles_included": all(item.get("included") for item in run.get("anchor_titles", [])),
                "strategy_errors": [
                    item for item in run.get("strategy_reports", [])
                    if item.get("error")
                ],
            }
            for run in runs_report
        ],
        "stable_across_repeated_runs": stable_across_repeated_runs,
        "count_remains_506": bool(want_counts) and all(count == DEFAULT_MOVIE_FETCH_EXPERIMENT_UI_COUNT for count in want_counts),
        "interpretation": (
            "The admin-only compare mode validates the intended visible want-to parity path: deterministic union, "
            "Notion page-ID de-duplication, then normalized Status == i want to filtering."
        ),
        "recommended_promotion_plan": (
            "Keep production unchanged, then add a feature-flagged fetch path that maps this filtered union into the existing film shape. "
            "Run it side-by-side against production for home/library diagnostics, verify anchors and 506 want-to count, then switch only the want-to source after one clean compare cycle."
        ),
    }
    if save_report:
        report["saved_report_path"] = save_correction_report(report, "movie-want-to-union-compare")
    return report

def get_section_playlists(section_name):
    # Reload playlists on each request so playlists.json changes appear immediately.
    playlists = load_playlists()
    if section_name in playlists:
        return playlists[section_name]
    wanted = normalize_section_name(section_name)
    for key, value in playlists.items():
        if normalize_section_name(key) == wanted or wanted in normalize_section_name(key):
            return value
    return []

def get_best_thumbnail(snippet, video_id=""):
    thumbnails = snippet.get("thumbnails") or {}
    for quality in ("maxres", "standard", "high", "medium", "default"):
        thumb = thumbnails.get(quality)
        if thumb and thumb.get("url"):
            return thumb["url"]
    if video_id:
        return f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"
    return ""

def fetch_playlist_videos_from_youtube(playlist_id, max_total=5000):
    """
    EXACT SAME LOGIC AS THE DIAGNOSTIC SCRIPT â€“ PROVEN TO WORK
    """
    if not playlist_id or playlist_id == "PASTE_PLAYLIST_ID_HERE":
        return []
    all_videos = []
    locally_deleted_items = load_deleted_video_lookup()
    next_page_token = None
    url = "https://www.googleapis.com/youtube/v3/playlistItems"
    while len(all_videos) < max_total:
        params = {
            "part": "snippet",
            "playlistId": playlist_id,
            "maxResults": max(1, min(50, max_total - len(all_videos))),
            "key": YOUTUBE_API_KEY,
            "pageToken": next_page_token
        }
        try:
            r = requests.get(url, params=params, timeout=10)
            if r.status_code != 200:
                print(f"API error {r.status_code}")
                break
            data = r.json()
            for item in data.get("items", []):
                playlist_item_id = item.get("id", "")
                if playlist_item_id and playlist_item_id in locally_deleted_items:
                    continue
                sn = item.get("snippet", {})
                resource = sn.get("resourceId") or {}
                vid = resource.get("videoId", "")
                if not vid:
                    continue
                thumb = get_best_thumbnail(sn, vid)
                all_videos.append({
                    "title": sn.get("title", "Unavailable video"),
                    "channel_name": sn.get("videoOwnerChannelTitle") or sn.get("channelTitle", ""),
                    "thumb": thumb,
                    "video_id": vid,
                    "playlist_id": playlist_id,
                    "playlist_item_id": playlist_item_id,
                    "published_at": sn.get("publishedAt", ""),
                    "url": f"https://www.youtube.com/watch?v={vid}&list={playlist_id}"
                })
            next_page_token = data.get("nextPageToken")
            print(f"[Playlist {playlist_id[:10]}...] Fetched {len(all_videos)} videos so far...")
            if not next_page_token:
                break
        except Exception as e:
            print(f"Exception: {e}")
            break
    print(f"Final count for {playlist_id}: {len(all_videos)} videos")
    return all_videos


def get_all_playlist_videos(playlist_id, max_total=5000, force_refresh=False):
    if not playlist_id or playlist_id == "PASTE_PLAYLIST_ID_HERE":
        return []

    cache_data = load_cache_data()
    playlist_entry = cache_data.get("youtube_playlists", {}).get(playlist_id, {})
    with RUNTIME_CACHE_LOCK:
        runtime_entry = RUNTIME_CACHE["youtube_playlists"].get(playlist_id)
        if runtime_entry is not None and not force_refresh and not is_cache_entry_stale(playlist_entry):
            return apply_cached_durations([dict(video) for video in runtime_entry])

    if not force_refresh and not is_cache_entry_stale(playlist_entry):
        videos = playlist_entry.get("data", []) or []
        with RUNTIME_CACHE_LOCK:
            RUNTIME_CACHE["youtube_playlists"][playlist_id] = [dict(video) for video in videos]
        return apply_cached_durations([dict(video) for video in videos])

    videos = fetch_playlist_videos_from_youtube(playlist_id, max_total=max_total)
    set_playlist_cache_entry(cache_data, playlist_id, videos)
    save_cache_data(cache_data)
    with RUNTIME_CACHE_LOCK:
        RUNTIME_CACHE["youtube_playlists"][playlist_id] = [dict(video) for video in videos]
    return apply_cached_durations([dict(video) for video in videos])

def yts_url(title):
    encoded = urllib.parse.quote(title.strip(), safe="")
    return f"https://www6.yts-official.to/browse-movies/{encoded}/all/all/0/latest/0/all"

def slugify(value):
    text = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return text or "entry"

def extract_youtube_video_id(url):
    if not url:
        return ""
    parsed = urllib.parse.urlparse(url)
    if parsed.netloc in {"youtu.be", "www.youtu.be"}:
        return parsed.path.strip("/")
    if "youtube.com" in parsed.netloc:
        if parsed.path == "/watch":
            return urllib.parse.parse_qs(parsed.query).get("v", [""])[0]
        if parsed.path.startswith("/embed/"):
            return parsed.path.split("/embed/", 1)[1].split("/")[0]
    return ""

def normalize_section_name(value):
    return "".join(ch.lower() for ch in value if ch.isalnum())


def normalize_pockettube_group_key(value):
    normalized = normalize_section_name(value)
    if normalized in {
        normalize_section_name("myfavoret"),
        normalize_section_name("myfavorite"),
        normalize_section_name("my favourite"),
    }:
        return normalize_section_name("myfavoret")
    return normalized


def build_film_id(film):
    return f"film-{slugify(film['name'])}"

def build_video_entry(video, playlist_name, playlist_url):
    item = dict(video)
    item["entry_id"] = f"yt-{video.get('playlist_item_id') or video['video_id']}"
    item["source_type"] = "youtube"
    item["playlist_name"] = playlist_name
    item["playlist_url"] = playlist_url
    item["url"] = video.get("url") or f"https://www.youtube.com/watch?v={video['video_id']}"
    return item


def build_youtube_channel_video_summary(video):
    entry_id = str(video.get("entry_id", "") or "").strip()
    if not entry_id:
        playlist_item_id = str(video.get("playlist_item_id", "") or "").strip()
        video_id = str(video.get("video_id", "") or "").strip()
        entry_id = f"yt-{playlist_item_id or video_id}" if (playlist_item_id or video_id) else ""
    video_id = str(video.get("video_id", "") or "").strip()
    playlist_item_id = str(video.get("playlist_item_id", "") or "").strip()
    watch_key = video_id or playlist_item_id or entry_id
    group_key = str(video.get("group_key", "") or "").strip()
    duration_seconds = int(video.get("duration_seconds", 0) or 0)
    duration_display = str(video.get("duration", "") or "").strip()
    if not duration_display and duration_seconds:
        duration_display = format_duration(duration_seconds)
    channel_name = str(video.get("channel_name", "") or "").strip() or "Unknown Channel"
    playlist_name = str(video.get("playlist_name", "") or "").strip()
    source_type = str(video.get("source_type", "") or "youtube").strip() or "youtube"
    thumb = str(video.get("thumb", "") or "").strip()
    thumbnail_url = str(video.get("thumbnail_url", "") or "").strip() or thumb
    thumbnail = str(video.get("thumbnail", "") or "").strip() or thumbnail_url
    image_url = str(video.get("image_url", "") or "").strip() or thumbnail_url
    return {
        "title": str(video.get("title", "") or "").strip(),
        "entry_id": entry_id,
        "video_id": video_id,
        "watch_key": watch_key,
        "group_key": group_key,
        "playlist_id": str(video.get("playlist_id", "") or "").strip(),
        "playlist_item_id": playlist_item_id,
        "playlist_name": playlist_name,
        "channel_name": channel_name,
        "thumb": thumb,
        "thumbnail_url": thumbnail_url,
        "thumbnail": thumbnail,
        "image_url": image_url,
        "duration": duration_display,
        "duration_seconds": duration_seconds,
        "published_at": str(video.get("published_at", "") or "").strip(),
        "published_display": format_timestamp_label(str(video.get("published_at", "") or "").strip(), default=""),
        "url": str(video.get("url", "") or "").strip(),
        "detail_url": str(video.get("detail_url", "") or "").strip() or (f"/video/{entry_id}" if entry_id else ""),
        "source_type": source_type,
        "category": str(video.get("category", "") or "").strip() or channel_name,
        "status": str(video.get("status", "") or "").strip() or playlist_name or duration_display,
    }


def build_youtube_channel_groups(videos, section_name="", section_profile=None, source_playlist=None):
    grouped = {}
    playlist_refs = {}
    playlist_name = str((source_playlist or {}).get("name", "") or "").strip()
    playlist_id = str((source_playlist or {}).get("id", "") or "").strip()
    section_profile = dict(section_profile or {})
    curated_lookup = section_profile.get("curated_channel_lookup", {}) if isinstance(section_profile.get("curated_channel_lookup", {}), dict) else {}
    for index, video in enumerate(videos or []):
        if not isinstance(video, dict):
            continue
        channel_name = str(video.get("channel_name", "") or "").strip() or "Unknown Channel"
        channel_key = normalize_section_name(channel_name) or "unknownchannel"
        curation_record = curated_lookup.get(channel_key, {})
        group = grouped.setdefault(channel_key, {
            "channel_key": channel_key,
            "channel_name": channel_name,
            "section_name": section_name,
            "section_kind": section_profile.get("section_kind", ""),
            "section_scope": section_profile.get("section_scope", ""),
            "channel_group_key": section_profile.get("channel_group_key", ""),
            "channel_group_label": section_profile.get("channel_group_label", ""),
            "is_curated": bool(curation_record),
            "curation_tier": curation_record.get("tier", ""),
            "curation_group_name": curation_record.get("group_name", ""),
            "curation_notes": curation_record.get("notes", ""),
            "curation_section_name": curation_record.get("section_name", ""),
            "curation_sort_rank": _youtube_channel_tier_rank(curation_record.get("tier", "")) if curation_record else 9,
            "video_count": 0,
            "representative_video": {},
            "latest_video": {},
            "_latest_sort_key": None,
            "_source_playlist_refs": set(),
        })
        group["video_count"] += 1
        summary = build_youtube_channel_video_summary(video)
        if not group["representative_video"]:
            group["representative_video"] = summary
        sort_key = _pockettube_feed_sort_key({**summary, "feed_order_index": index})
        if group["_latest_sort_key"] is None or sort_key < group["_latest_sort_key"]:
            group["_latest_sort_key"] = sort_key
            group["latest_video"] = summary
        video_playlist_id = summary.get("playlist_id", "") or playlist_id
        video_playlist_name = summary.get("playlist_name", "") or playlist_name
        if video_playlist_id or video_playlist_name:
            ref_key = video_playlist_id or video_playlist_name
            playlist_refs.setdefault(ref_key, {
                "playlist_id": video_playlist_id,
                "playlist_name": video_playlist_name,
                "video_count": 0,
            })
            playlist_refs[ref_key]["video_count"] += 1
            group["_source_playlist_refs"].add(ref_key)

    channel_groups = []
    for group in grouped.values():
        source_playlists = []
        for ref_key in sorted(group.pop("_source_playlist_refs", set()) or []):
            ref = playlist_refs.get(ref_key)
            if ref:
                source_playlists.append(dict(ref))
        group.pop("_latest_sort_key", None)
        group["source_playlists"] = source_playlists
        channel_groups.append(group)

    channel_groups.sort(
        key=lambda item: (
            -int(bool(item.get("is_curated"))),
            int(item.get("curation_sort_rank", 9) or 9),
            -int(item.get("video_count", 0) or 0),
            item.get("channel_name", "").lower(),
        )
    )
    return channel_groups

def build_film_entry(film):
    item = dict(film)
    item["entry_id"] = build_film_id(film)
    item["source_type"] = "film"
    item["video_id"] = extract_youtube_video_id(film.get("trailer", ""))
    director_relation_ids = list(item.get("director_relation_ids", []) or [])
    item["director_relation_ids"] = director_relation_ids
    item["director_page_id"] = director_relation_ids[0] if director_relation_ids else ""
    director_entries = []
    for director in list(item.get("director_entries", []) or []):
        director_copy = dict(director)
        if director_copy.get("page_id"):
            try:
                director_copy["detail_url"] = url_for("director_detail", director_page_id=director_copy["page_id"])
            except RuntimeError:
                director_copy["detail_url"] = f"/director/{director_copy['page_id']}"
        else:
            director_copy["detail_url"] = ""
        director_entries.append(director_copy)
    item["director_entries"] = director_entries
    item["genre_relation_ids"] = list(item.get("genre_relation_ids", []) or [])
    genre_entries = []
    for genre in list(item.get("genre_entries", []) or []):
        genre_copy = dict(genre)
        if genre_copy.get("page_id"):
            try:
                genre_copy["detail_url"] = url_for("library", genre=genre_copy.get("name", ""), genre_id=genre_copy["page_id"])
            except RuntimeError:
                genre_copy["detail_url"] = f"/library?genre={urllib.parse.quote(str(genre_copy.get('name', '') or ''))}&genre_id={urllib.parse.quote(genre_copy['page_id'])}"
        else:
            genre_copy["detail_url"] = ""
        genre_entries.append(genre_copy)
    item["genre_entries"] = genre_entries
    return item


def normalize_movie_category(value):
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def is_non_movie_category(value):
    category = normalize_movie_category(value)
    if not category or category in VALID_MOVIE_CATEGORIES:
        return False
    return any(marker in category for marker in NON_MOVIE_CATEGORY_MARKERS)


def movie_row_type_for_category(value):
    return "non_movie" if is_non_movie_category(value) else "real"


def normalize_movie_review_filter(value, allowed_values, default_value="all"):
    normalized = normalize_movie_category(value)
    return normalized if normalized in allowed_values else default_value


def filter_movie_review_items(items, confidence_filter="all", row_type_filter="all"):
    confidence_filter = normalize_movie_review_filter(confidence_filter, {"high", "medium", "all"})
    row_type_filter = normalize_movie_review_filter(row_type_filter, {"real", "non_movie", "all"})

    def matches(item):
        item_confidence = normalize_movie_category(item.get("confidence", item.get("confidence_level", "")))
        item_row_type = normalize_movie_category(item.get("row_type", "real"))
        if confidence_filter != "all" and item_confidence != confidence_filter:
            return False
        if row_type_filter != "all" and item_row_type != row_type_filter:
            return False
        return True

    return [item for item in items if matches(item)]


def count_movie_review_items(items, key):
    counts = {}
    for item in items:
        value = normalize_movie_category(item.get(key, ""))
        counts[value] = counts.get(value, 0) + 1
    return counts


def normalize_movie_status(value):
    cleaned = re.sub(r"\s+", " ", str(value or "").strip().lower())
    return re.sub(r"[^a-z]+", "", cleaned)


def normalize_genre_filter_value(value):
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def film_matches_genre_filter(film, genre_name="", genre_page_id=""):
    target_page_id = str(genre_page_id or "").strip()
    target_name = normalize_genre_filter_value(genre_name)
    if target_page_id:
        return target_page_id in list(film.get("genre_relation_ids", []) or [])
    if not target_name:
        return True
    for genre in list(film.get("genre_entries", []) or []):
        if normalize_genre_filter_value(genre.get("name", "")) == target_name:
            return True
    genres_text = str(film.get("genres") or "")
    for part in genres_text.split(","):
        if normalize_genre_filter_value(part) == target_name:
            return True
    return False


def resolve_genre_filter_label(films, genre_name="", genre_page_id=""):
    target_page_id = str(genre_page_id or "").strip()
    target_name = normalize_genre_filter_value(genre_name)
    for film in list(films or []):
        for genre in list(film.get("genre_entries", []) or []):
            if target_page_id and str(genre.get("page_id") or "").strip() == target_page_id:
                return str(genre.get("name") or "").strip()
            if target_name and normalize_genre_filter_value(genre.get("name", "")) == target_name:
                return str(genre.get("name") or "").strip()
    return str(genre_name or "").strip()


def normalize_director_filter_value(value):
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def film_matches_director_filter(film, director_name="", director_page_id=""):
    target_page_id = str(director_page_id or "").strip()
    target_name = normalize_director_filter_value(director_name)
    if target_page_id:
        return target_page_id in list(film.get("director_relation_ids", []) or [])
    if not target_name:
        return True
    for director in list(film.get("director_entries", []) or []):
        if normalize_director_filter_value(director.get("name", "")) == target_name:
            return True
    return normalize_director_filter_value(film.get("director", "")) == target_name


def resolve_director_filter_label(films, director_name="", director_page_id=""):
    target_page_id = str(director_page_id or "").strip()
    target_name = normalize_director_filter_value(director_name)
    for film in list(films or []):
        for director in list(film.get("director_entries", []) or []):
            if target_page_id and str(director.get("page_id") or "").strip() == target_page_id:
                return str(director.get("name") or "").strip()
            if target_name and normalize_director_filter_value(director.get("name", "")) == target_name:
                return str(director.get("name") or "").strip()
    return str(director_name or "").strip()


def is_primary_watch_next_status(film):
    return normalize_movie_status(film.get("status", "")) in PRIMARY_WATCH_NEXT_STATUSES


def title_match_mode(left, right):
    left_key = normalized_match_key(left)
    right_key = normalized_match_key(right)
    if not left_key or not right_key:
        return ""
    if left_key == right_key:
        return "exact"
    if left_key in right_key or right_key in left_key:
        return "normalized"
    return ""


def director_match_mode(left, right):
    left_key = normalized_person_key(left)
    right_key = normalized_person_key(right)
    if not left_key or not right_key:
        return ""
    if left_key == right_key:
        return "exact"
    if left_key in right_key or right_key in left_key:
        return "partial"
    return ""


def director_record_match_names(director_record):
    names = []
    seen = set()
    if not isinstance(director_record, dict):
        return names
    for value in [director_record.get("display_name", "")] + list(director_record.get("aliases", []) or []):
        key = normalized_person_key(value)
        if key and key not in seen:
            seen.add(key)
            names.append(str(value).strip())
    return names


def film_director_candidate_names(film):
    names = []
    seen = set()
    for director in list((film or {}).get("director_entries", []) or []):
        for value in (director.get("name", ""), director.get("page_id", "")):
            mode = normalized_person_key(value)
            if mode and mode not in seen:
                seen.add(mode)
                names.append(str(value).strip())
    for value in split_director_names((film or {}).get("director", "")):
        mode = normalized_person_key(value)
        if mode and mode not in seen:
            seen.add(mode)
            names.append(value)
    return names


def score_director_page_candidate(director_record, film):
    director_page_id = str((director_record or {}).get("page_id", "")).strip()
    relation_ids = list((film or {}).get("director_relation_ids", []) or [])
    candidate_names = film_director_candidate_names(film)
    record_names = director_record_match_names(director_record)
    relation_match = director_page_id and director_page_id in relation_ids
    name_mode = ""
    alias_mode = ""
    for record_name in record_names:
        for candidate_name in candidate_names:
            mode = director_match_mode(record_name, candidate_name)
            if mode == "exact":
                name_mode = "exact"
                break
            if mode == "partial" and not name_mode:
                name_mode = "partial"
        if name_mode == "exact":
            break
    if not name_mode:
        alias_mode = "alias" if any(normalized_person_key(alias) in {normalized_person_key(name) for name in candidate_names} for alias in (director_record or {}).get("aliases", []) or []) else ""

    if relation_match:
        match_mode = "relation"
        match_rank = 0
    elif name_mode == "exact":
        match_mode = "name-exact"
        match_rank = 1
    elif name_mode == "partial" or alias_mode:
        match_mode = "name-partial"
        match_rank = 2
    else:
        match_mode = ""
        match_rank = 3

    status_value = normalize_movie_status((film or {}).get("status", ""))
    if status_value in PRIMARY_WATCH_NEXT_STATUSES:
        status_rank = 0
    elif is_watched_film_status(film):
        status_rank = 1
    else:
        status_rank = 2

    source_priority = related_movie_source_priority((film or {}).get("source", ""))
    metadata = film_metadata_quality_summary(film)
    score_num = film_detail_score_num(film)
    year_value = film_detail_year_value(film)
    title_noise = film_title_has_noise((film or {}).get("name", ""))

    score = 0
    reasons = []
    if relation_match:
        score += 120
        reasons.append("Director relation")
    if name_mode == "exact":
        score += 90
        reasons.append("Director name")
    elif name_mode == "partial" or alias_mode:
        score += 60
        reasons.append("Director alias")

    score += source_priority * 12
    if source_priority >= 2:
        reasons.append("Library-backed source")
    if status_rank == 0:
        score += 24
        reasons.append("Want-to")
    elif status_rank == 1:
        score += 12
        reasons.append("Finished")

    if score_num > 0:
        score += min(score_num * 3, 24)
        reasons.append(f"Score signal: {film.get('score') or 'rated'}")
    score += metadata["score"] * 4
    if metadata["is_clean"]:
        score += 10
    if metadata["is_weak"]:
        score -= 16
        reasons.append("Weak metadata")
    if title_noise:
        score -= 14
        reasons.append("Noisy title")
    if year_value.isdigit():
        score += 6
    if not relation_match and match_rank >= 2 and not candidate_names:
        score -= 12

    return {
        "score": score,
        "match_rank": match_rank,
        "match_mode": match_mode,
        "status_rank": status_rank,
        "source_priority": source_priority,
        "metadata_quality_score": metadata["score"],
        "metadata_is_clean": metadata["is_clean"],
        "metadata_is_weak": metadata["is_weak"],
        "score_num": score_num,
        "year_value": year_value,
        "title_noise": title_noise,
        "reasons": reasons,
    }


def rank_director_page_films(director_record, films):
    ranked = []
    matched_relation = 0
    matched_name = 0
    matched_alias = 0
    watched_count = 0
    want_count = 0
    other_count = 0
    source_counts = {}
    for film in list(films or []):
        profile = score_director_page_candidate(director_record, film)
        if profile["match_rank"] >= 3:
            continue
        item = dict(film)
        item["director_match_rank"] = profile["match_rank"]
        item["director_match_mode"] = profile["match_mode"]
        item["director_status_rank"] = profile["status_rank"]
        item["director_source_priority"] = profile["source_priority"]
        item["director_metadata_quality_score"] = profile["metadata_quality_score"]
        item["director_metadata_is_clean"] = profile["metadata_is_clean"]
        item["director_metadata_is_weak"] = profile["metadata_is_weak"]
        item["director_reasons"] = profile["reasons"]
        item["director_ranking_score"] = profile["score"]
        item["director_score_num"] = profile["score_num"]
        item["director_year_value"] = profile["year_value"]
        item["director_title_noise"] = profile["title_noise"]
        ranked.append(item)
        source_key = normalize_movie_source(item.get("source", "")) or "unknown"
        source_counts[source_key] = source_counts.get(source_key, 0) + 1
        if profile["match_mode"] == "relation":
            matched_relation += 1
        elif profile["match_mode"] == "name-exact":
            matched_name += 1
        elif profile["match_mode"] == "name-partial":
            matched_alias += 1
        if profile["status_rank"] == 0:
            want_count += 1
        elif profile["status_rank"] == 1:
            watched_count += 1
        else:
            other_count += 1

    ranked.sort(
        key=lambda item: (
            item.get("director_status_rank", 2),
            item.get("director_match_rank", 3),
            -int(item.get("director_source_priority", 0)),
            -int(item.get("director_ranking_score", 0)),
            -int(item.get("director_score_num", 0)),
            -int(item.get("director_metadata_quality_score", 0)),
            int(item.get("director_title_noise", False)),
            int(not item.get("director_metadata_is_clean", False)),
            int(item.get("director_year_value") or 0),
            item.get("name", "").lower(),
        )
    )

    summary_parts = []
    if want_count or watched_count or other_count:
        summary_parts.append(f"{want_count} want-to")
        summary_parts.append(f"{watched_count} finished")
        if other_count:
            summary_parts.append(f"{other_count} other")
    source_summary = ", ".join(
        f"{name} ({count})"
        for name, count in sorted(source_counts.items(), key=lambda item: (-item[1], item[0]))
    )
    return {
        "films": ranked,
        "summary": " · ".join(summary_parts) if summary_parts else "",
        "source_summary": source_summary,
        "matched_relation_count": matched_relation,
        "matched_name_count": matched_name,
        "matched_alias_count": matched_alias,
        "watched_count": watched_count,
        "want_count": want_count,
        "other_count": other_count,
    }


def compute_match_confidence(current_title="", current_year="", current_director="", candidate_title="", candidate_year="", candidate_director="", source_label=""):
    score = 0
    reasons = []
    title_mode = title_match_mode(current_title, candidate_title)
    director_mode = director_match_mode(current_director, candidate_director)
    current_year_value = normalize_year_value(current_year)
    candidate_year_value = normalize_year_value(candidate_year)

    if title_mode == "exact":
        score += 60
        reasons.append("Exact title match")
    elif title_mode == "normalized":
        score += 38
        reasons.append("Normalized title match")

    if current_year_value and candidate_year_value:
        try:
            year_delta = abs(int(current_year_value) - int(candidate_year_value))
        except (TypeError, ValueError):
            year_delta = None
        if year_delta == 0:
            score += 25
            reasons.append("Same year")
        elif year_delta == 1:
            score += 12
            reasons.append("Â±1 year")

    if director_mode == "exact":
        score += 22
        reasons.append("Director match")
    elif director_mode == "partial":
        score += 10
        reasons.append("Partial director match")

    if title_mode == "exact" and current_year_value and candidate_year_value and current_year_value == candidate_year_value:
        level = "high"
    elif title_mode == "exact" and director_mode == "exact" and (not current_year_value or not candidate_year_value or abs(int(current_year_value) - int(candidate_year_value)) <= 1):
        level = "high"
    elif score >= 90:
        level = "high"
    elif score >= 65:
        level = "medium"
    else:
        level = "low"

    reason_text = " + ".join(reasons) if reasons else (source_label or "No strong signals")
    return {"score": score, "level": level, "reason": reason_text}


def build_csv_review_proposal(film, csv_item):
    proposed_title = clean_correction_text(csv_item.get("corrected_title", "")) or clean_correction_text(film.get("name", ""))
    proposed_director = clean_correction_text(csv_item.get("director", ""))
    proposed_year = normalize_year_value(csv_item.get("year", ""))
    has_difference = (
        (proposed_title and proposed_title != clean_correction_text(film.get("name", "")))
        or (proposed_director and proposed_director != clean_correction_text(film.get("director", "")))
        or (proposed_year and proposed_year != normalize_year_value(film.get("year", "")))
    )
    if not has_difference:
        return None
    reason = f"CSV strong match by {str(csv_item.get('match_rule') or 'page_id').replace('-', ' + ')}"
    return {
        "title": proposed_title,
        "director": proposed_director,
        "year": proposed_year,
        "source": "CSV strong-match review",
        "confidence_level": "high",
        "confidence_score": 100,
        "reason": reason,
    }


def build_tmdb_review_proposal(film, tmdb_data):
    proposed_title = clean_correction_text(tmdb_data.get("matched_title", ""))
    proposed_director = clean_correction_text(tmdb_data.get("director", ""))
    proposed_year = normalize_year_value(tmdb_data.get("year", ""))
    confidence = compute_match_confidence(
        current_title=film.get("name", ""),
        current_year=film.get("year", ""),
        current_director=film.get("director", ""),
        candidate_title=proposed_title,
        candidate_year=proposed_year,
        candidate_director=proposed_director,
        source_label="TMDb review proposal",
    )
    has_difference = (
        (proposed_title and proposed_title != clean_correction_text(film.get("name", "")))
        or (proposed_director and proposed_director != clean_correction_text(film.get("director", "")))
        or (proposed_year and proposed_year != normalize_year_value(film.get("year", "")))
    )
    if not has_difference or confidence["level"] != "high":
        return None
    return {
        "title": proposed_title,
        "director": proposed_director,
        "year": proposed_year,
        "source": "TMDb high-confidence match",
        "confidence_level": confidence["level"],
        "confidence_score": confidence["score"],
        "reason": confidence["reason"],
    }


def normalized_title_preview(value):
    return normalize_movie_title(value).strip()


def film_title_has_noise(title):
    lowered = str(title or "").lower()
    if any(token in lowered for token in TITLE_NOISE_TOKENS):
        return True
    if "[" in lowered or "]" in lowered or "{" in lowered or "}" in lowered:
        return True
    if re.search(r"\b(s\d{1,2}e\d{1,2}|season\s*\d+|episode\s*\d+|ep\.?\s*\d+)\b", lowered):
        return True
    return False


def film_metadata_mismatch_candidate(film):
    category = normalize_movie_category(film.get("category", ""))
    title = str(film.get("name", "") or "")
    lowered = title.lower()
    if re.search(r"\b(s\d{1,2}e\d{1,2}|season\s*\d+|episode\s*\d+|ep\.?\s*\d+)\b", lowered):
        return category not in {"tv show", "anime"}
    preview = normalized_title_preview(title)
    if preview and re.search(r"\(\d{4}\)$", title.strip()) and not film.get("year"):
        return True
    return False


def build_movies_review_queue():
    films = [build_film_entry(film) for film in fetch_all_films()]
    normalized_groups = {}
    for film in films:
        normalized_key = normalized_match_key(film.get("name", ""))
        if not normalized_key:
            continue
        normalized_groups.setdefault(normalized_key, []).append(film)

    review_items = []
    summary = {
        "total_flagged_items": 0,
        "missing_posters": 0,
        "invalid_categories": 0,
        "non_movie_items": 0,
        "missing_categories": 0,
        "possible_duplicates": 0,
        "weak_metadata": 0,
    }

    for film in films:
        category = str(film.get("category", "") or "").strip()
        normalized_category = normalize_movie_category(category)
        title = str(film.get("name", "") or "").strip()
        preview_title = normalized_title_preview(title)
        duplicate_group = normalized_groups.get(normalized_match_key(title), [])
        missing_metadata_fields = [
            key for key in MOVIE_REVIEW_IMPORTANT_FIELDS
            if not str(film.get(key, "") or "").strip()
        ]

        flags = []
        if not str(film.get("poster", "") or "").strip():
            flags.append("Missing poster")
        if not category:
            flags.append("Missing category")
        elif is_non_movie_category(category):
            flags.append("Non-movie / wrong-section item")
        elif normalized_category not in VALID_MOVIE_CATEGORIES:
            flags.append(f'Invalid category: "{category}"')
        if len(missing_metadata_fields) >= 4:
            flags.append("Very weak metadata")
        if film_title_has_noise(title):
            flags.append("Suspicious title formatting")
        if len(duplicate_group) > 1:
            flags.append("Possible duplicate title")
        if film_metadata_mismatch_candidate(film):
            flags.append("Metadata mismatch candidate")

        if not flags:
            continue

        if not str(film.get("poster", "") or "").strip():
            summary["missing_posters"] += 1
        if not category:
            summary["missing_categories"] += 1
        elif is_non_movie_category(category):
            summary["invalid_categories"] += 1
            summary["non_movie_items"] += 1
        elif normalized_category not in VALID_MOVIE_CATEGORIES:
            summary["invalid_categories"] += 1
        if len(duplicate_group) > 1:
            summary["possible_duplicates"] += 1
        if len(missing_metadata_fields) >= 4:
            summary["weak_metadata"] += 1

        review_items.append({
            "entry_id": film.get("entry_id", ""),
            "title": film.get("name", "Untitled"),
            "poster": film.get("poster", ""),
            "category": category,
            "status": film.get("status", ""),
            "score": film.get("score", ""),
            "flags": flags,
            "normalized_title_preview": preview_title if preview_title != title else "",
            "duplicate_count": len(duplicate_group),
            "missing_metadata_fields": missing_metadata_fields,
            "detail_url": url_for("video_detail", entry_id=film.get("entry_id", "")),
        })

    review_items.sort(key=lambda item: (-len(item["flags"]), item["title"].lower()))
    summary["total_flagged_items"] = len(review_items)
    return {"items": review_items, "summary": summary}


def build_admin_movie_review_queue():
    films = [build_film_entry(film) for film in fetch_all_films()]
    report = build_correction_preview_report()
    csv_proposals_by_page_id = {}
    for item in report.get("groups", {}).get("strong_matches", []):
        matched_entry = resolve_live_notion_film(item.get("matched_entry") or {}, films=films)
        notion_page_id = (matched_entry or {}).get("notion_page_id", "")
        if not notion_page_id:
            continue
        csv_proposals_by_page_id[notion_page_id] = item
    review_items = []
    summary = {
        "total_items": 0,
        "missing_tmdb_match": 0,
        "missing_year": 0,
        "suspicious_year": 0,
        "missing_director": 0,
        "missing_overview": 0,
    }
    current_year_limit = datetime.now().year + 1

    for film in films:
        reasons = []
        year_value = normalize_year_value(film.get("year", ""))
        tmdb_data = None
        tmdb_status = "Not checked in queue pass"
        tmdb_match_status = "unchecked"
        tmdb_error = ""
        proposed_correction = None

        if not year_value:
            reasons.append("Missing year")
            summary["missing_year"] += 1
        else:
            try:
                numeric_year = int(year_value)
                if numeric_year < 1888 or numeric_year > current_year_limit:
                    reasons.append(f"Suspicious year: {numeric_year}")
                    summary["suspicious_year"] += 1
            except (TypeError, ValueError):
                reasons.append(f"Suspicious year: {year_value}")
                summary["suspicious_year"] += 1

        if not (clean_correction_text(film.get("director", "")) or list(film.get("director_entries", []) or [])):
            reasons.append("Missing director")
            summary["missing_director"] += 1

        if not clean_correction_text(film.get("overview", "")):
            reasons.append("Missing overview")
            summary["missing_overview"] += 1

        should_check_tmdb = bool(reasons)
        if should_check_tmdb:
            try:
                tmdb_data = fetch_tmdb_enrichment(
                    film.get("name", ""),
                    category=film.get("category", ""),
                    year=year_value
                )
            except (requests.RequestException, RuntimeError) as exc:
                tmdb_error = str(exc)
                tmdb_data = None

            if not tmdb_data:
                reasons.append("Missing TMDb match")
                summary["missing_tmdb_match"] += 1
                tmdb_match_status = "missing"
                if tmdb_error:
                    tmdb_status = f"TMDb lookup error: {tmdb_error}"
                else:
                    tmdb_status = "No TMDb match"
            else:
                tmdb_match_status = tmdb_review_confidence(tmdb_data, film)
                matched_title = str(tmdb_data.get("matched_title") or "").strip() or "Unknown title"
                matched_year = normalize_year_value(tmdb_data.get("matched_year", ""))
                tmdb_status = f"Matched: {matched_title}"
                if matched_year:
                    tmdb_status += f" ({matched_year})"
                tmdb_status += f" | {tmdb_match_status}"

        csv_plan_item = csv_proposals_by_page_id.get(film.get("notion_page_id", ""))
        if csv_plan_item:
            proposed_correction = build_csv_review_proposal(film, csv_plan_item)
        elif tmdb_data:
            proposed_correction = build_tmdb_review_proposal(film, tmdb_data)

        if not reasons:
            continue

        review_items.append({
            "notion_page_id": film.get("notion_page_id", ""),
            "entry_id": film.get("entry_id", ""),
            "title": film.get("name", "Untitled"),
            "poster": film.get("poster", ""),
            "detail_url": "",
            "category": film.get("category", ""),
            "status": film.get("status", ""),
            "score": film.get("score", ""),
            "year": year_value,
            "director": film.get("director", ""),
            "overview": film.get("overview", ""),
            "tmdb_rating": film.get("tmdb_rating", ""),
            "reasons": reasons,
            "tmdb_status": tmdb_status,
            "tmdb_match_status": tmdb_match_status,
            "tmdb_matched_title": "" if not tmdb_data else str(tmdb_data.get("matched_title") or "").strip(),
            "tmdb_matched_year": "" if not tmdb_data else normalize_year_value(tmdb_data.get("matched_year", "")),
            "proposed_correction": proposed_correction,
        })
        try:
            review_items[-1]["detail_url"] = url_for("video_detail", entry_id=film.get("entry_id", ""))
        except RuntimeError:
            review_items[-1]["detail_url"] = f"/video/{film.get('entry_id', '')}"

    review_items.sort(key=lambda item: (-len(item["reasons"]), item["title"].lower()))
    summary["total_items"] = len(review_items)
    return {"items": review_items, "summary": summary}


def build_review_correction_payload(page_properties, review_item):
    proposed = dict((review_item or {}).get("proposed_correction") or {})
    if not proposed:
        return {}

    payload = {}
    props = page_properties or {}
    proposed_title = clean_correction_text(proposed.get("title", ""))
    proposed_director = clean_correction_text(proposed.get("director", ""))
    proposed_year = normalize_year_value(proposed.get("year", ""))

    if proposed_title and proposed_title != clean_correction_text((review_item or {}).get("title", "")):
        title_property = notion_title_property_name(props)
        payload[title_property] = {"title": notion_title_text(proposed_title[:2000])}

    if proposed_director and proposed_director != clean_correction_text((review_item or {}).get("director", "")):
        director_prop = props.get("Director", {})
        if director_prop.get("type") == "rich_text":
            payload["Director"] = {
                "rich_text": [{"type": "text", "text": {"content": proposed_director[:2000]}}]
            }

    if proposed_year and proposed_year != normalize_year_value((review_item or {}).get("year", "")):
        payload["Year"] = {"number": int(proposed_year)}

    return payload


def values_differ(left, right):
    return clean_correction_text(left) != clean_correction_text(right)


def poster_urls_differ(left, right):
    return clean_correction_text(left) != clean_correction_text(right)


def rating_values_differ(left, right):
    if left in (None, "") and right in (None, ""):
        return False
    if left in (None, "") or right in (None, ""):
        return True
    try:
        return round(float(left), 1) != round(float(right), 1)
    except (TypeError, ValueError):
        return str(left) != str(right)


def tmdb_review_confidence(tmdb_data, film):
    score = int((tmdb_data or {}).get("match_score") or 0)
    film_year = normalize_year_value(film.get("year", ""))
    matched_year = normalize_year_value((tmdb_data or {}).get("matched_year", ""))
    title_exact = normalized_match_key(film.get("name", "")) == normalized_match_key((tmdb_data or {}).get("matched_title", ""))
    if score >= 120 and title_exact and (not film_year or not matched_year or film_year == matched_year):
        return "high"
    if score >= 90:
        return "medium"
    return "low"


def tmdb_review_source_label(tmdb_data):
    media_type = (tmdb_data or {}).get("tmdb_type", "")
    matched_title = (tmdb_data or {}).get("matched_title", "") or "unknown title"
    matched_year = normalize_year_value((tmdb_data or {}).get("matched_year", ""))
    source = f"TMDb {'TV' if media_type == 'tv' else 'movie'} search"
    if matched_year:
        source += f" -> {matched_title} ({matched_year})"
    else:
        source += f" -> {matched_title}"
    return source


def build_targeted_movie_review():
    films = [build_film_entry(film) for film in fetch_all_films()]
    review_items = []
    summary = {
        "total_items": 0,
        "high_confidence": 0,
        "medium_confidence": 0,
        "low_confidence": 0,
        "wrong_match_suspicions": 0,
        "non_movie_items": 0,
    }

    for film in films:
        current_category = str(film.get("category") or "").strip()
        normalized_category = normalize_movie_category(current_category)
        current_year = normalize_year_value(film.get("year", ""))
        current_director = clean_correction_text(film.get("director", ""))
        current_poster = clean_correction_text(film.get("poster", ""))
        current_rating = film.get("tmdb_rating", "")
        current_overview = clean_correction_text(film.get("overview", ""))
        suspected_fields = []
        current_values = {}
        proposed_values = {}
        notes = []

        if is_non_movie_category(current_category):
            suspected_fields.append("category")
            current_values["category"] = current_category
            proposed_values["category"] = "Non-movie / wrong-section item"
            notes.append("This entry belongs to a non-movie section and should stay out of the movie correction/apply flow.")
            summary["low_confidence"] += 1
            summary["non_movie_items"] += 1
            review_items.append({
                "title": film.get("name", "Untitled"),
                "poster": film.get("poster", ""),
                "detail_url": film.get("detail_url", ""),
                "current_values": current_values,
                "suspected_fields": suspected_fields,
                "proposed_values": proposed_values,
                "source_used": "Notion category review",
                "confidence": "low",
                "row_type": "non_movie",
                "notes": notes,
            })
            continue

        if not current_category or normalized_category not in VALID_MOVIE_CATEGORIES:
            suspected_fields.append("category")
            current_values["category"] = current_category or "Missing"
            proposed_values["category"] = current_category or "Needs manual category review"
            notes.append("Current category is missing or outside the normalized movie category set.")

        try:
            tmdb_data = fetch_tmdb_enrichment(film.get("name", ""), category=current_category, year=current_year)
        except (requests.RequestException, RuntimeError):
            tmdb_data = None

        if not tmdb_data:
            if suspected_fields:
                review_items.append({
                    "title": film.get("name", "Untitled"),
                    "poster": film.get("poster", ""),
                    "detail_url": film.get("detail_url", ""),
                    "current_values": current_values,
                    "suspected_fields": suspected_fields,
                    "proposed_values": proposed_values,
                    "source_used": "Existing Notion review only",
                    "confidence": "low",
                    "notes": notes or ["Could not build a reliable TMDb comparison for this title."],
                })
            continue

        tmdb_year = normalize_year_value(tmdb_data.get("year", ""))
        tmdb_director = clean_correction_text(tmdb_data.get("director", ""))
        tmdb_poster = clean_correction_text(tmdb_data.get("poster_url", ""))
        tmdb_rating = "" if tmdb_data.get("rating") in (None, "") else round(float(tmdb_data.get("rating")), 1)
        tmdb_overview = clean_correction_text(tmdb_data.get("overview", ""))
        confidence = tmdb_review_confidence(tmdb_data, film)
        source_used = tmdb_review_source_label(tmdb_data)

        if current_year and tmdb_year and current_year != tmdb_year:
            suspected_fields.append("year")
            current_values["year"] = current_year
            proposed_values["year"] = tmdb_year
        if current_director and tmdb_director and values_differ(current_director, tmdb_director):
            suspected_fields.append("director")
            current_values["director"] = current_director
            proposed_values["director"] = tmdb_director
        if not current_poster and tmdb_poster:
            suspected_fields.append("poster")
            current_values["poster"] = "Missing"
            proposed_values["poster"] = tmdb_poster
        if not current_rating and tmdb_rating not in (None, ""):
            suspected_fields.append("TMDb rating")
            current_values["TMDb rating"] = "Missing"
            proposed_values["TMDb rating"] = tmdb_rating
        if not current_overview and tmdb_overview:
            suspected_fields.append("overview")
            current_values["overview"] = "Missing"
            proposed_values["overview"] = (tmdb_overview[:280] + "...") if len(tmdb_overview) > 280 else tmdb_overview

        if confidence == "low":
            suspected_fields.append("wrong TMDb match")
            current_values["matched title"] = film.get("name", "")
            proposed_values["matched title"] = (tmdb_data.get("matched_title") or "Unknown").strip()
            notes.append("TMDb match confidence is low, so this title may need a manual movie-vs-TV or same-title check.")

        if not suspected_fields:
            continue

        if confidence == "high":
            summary["high_confidence"] += 1
        elif confidence == "medium":
            summary["medium_confidence"] += 1
        else:
            summary["low_confidence"] += 1
        if "wrong TMDb match" in suspected_fields:
            summary["wrong_match_suspicions"] += 1

        review_items.append({
            "title": film.get("name", "Untitled"),
            "poster": film.get("poster", ""),
            "detail_url": film.get("detail_url", ""),
            "current_values": current_values,
            "suspected_fields": suspected_fields,
            "proposed_values": proposed_values,
            "source_used": source_used,
            "confidence": confidence,
            "row_type": "real",
            "notes": notes,
        })

    review_items.sort(
        key=lambda item: (
            {"high": 0, "medium": 1, "low": 2}.get(item.get("confidence", "low"), 3),
            -len(item.get("suspected_fields", [])),
            item.get("title", "").lower(),
        )
    )
    summary["total_items"] = len(review_items)
    return {"items": review_items, "summary": summary}


def film_metadata_quality_summary(film):
    present_fields = [
        field_name for field_name in CURATION_METADATA_FIELDS
        if str(film.get(field_name, "") or "").strip()
    ]
    missing_fields = [field_name for field_name in CURATION_METADATA_FIELDS if field_name not in present_fields]
    return {
        "score": len(present_fields),
        "max_score": len(CURATION_METADATA_FIELDS),
        "present_fields": present_fields,
        "missing_fields": missing_fields,
        "is_clean": len(missing_fields) <= 1,
        "is_weak": len(missing_fields) >= 4,
    }


def film_detail_director_keys(film):
    keys = []
    seen = set()
    for director in list((film or {}).get("director_entries", []) or []):
        for value in (director.get("page_id", ""), director.get("name", "")):
            key = normalized_person_key(value)
            if key and key not in seen:
                seen.add(key)
                keys.append(key)
    if not keys:
        for value in split_director_names((film or {}).get("director", "")):
            key = normalized_person_key(value)
            if key and key not in seen:
                seen.add(key)
                keys.append(key)
    return keys


def film_detail_genre_keys(film):
    keys = []
    seen = set()
    for genre in list((film or {}).get("genre_entries", []) or []):
        for value in (genre.get("page_id", ""), genre.get("name", "")):
            key = normalized_genre_key(value)
            if key and key not in seen:
                seen.add(key)
                keys.append(key)
    if not keys:
        for value in split_genre_names((film or {}).get("genres", "")):
            key = normalized_genre_key(value)
            if key and key not in seen:
                seen.add(key)
                keys.append(key)
    return keys


def film_detail_year_value(film):
    return normalize_year_value((film or {}).get("year", ""))


def film_detail_score_num(film):
    score_value = str((film or {}).get("score", "") or "").strip()
    return int((film or {}).get("score_num") or SCORE_ORDER.get(score_value, 0) or 0)


def related_movie_source_priority(source):
    normalized_source = normalize_movie_source(source)
    if normalized_source == "My library and Ebert's":
        return 3
    if normalized_source in {"My library", "Ebert's library"}:
        return 2
    return 1


def score_movie_related_candidate(detail, candidate):
    detail_source = normalize_movie_source((detail or {}).get("source", ""))
    candidate_source = normalize_movie_source((candidate or {}).get("source", ""))
    detail_category = normalize_movie_category((detail or {}).get("category", ""))
    candidate_category = normalize_movie_category((candidate or {}).get("category", ""))
    detail_status = normalize_movie_status((detail or {}).get("status", ""))
    candidate_status = normalize_movie_status((candidate or {}).get("status", ""))
    detail_directors = set(film_detail_director_keys(detail))
    candidate_directors = set(film_detail_director_keys(candidate))
    detail_genres = set(film_detail_genre_keys(detail))
    candidate_genres = set(film_detail_genre_keys(candidate))
    detail_year = film_detail_year_value(detail)
    candidate_year = film_detail_year_value(candidate)
    candidate_score_num = film_detail_score_num(candidate)
    metadata = film_metadata_quality_summary(candidate)
    title_value = str((candidate or {}).get("name", "") or "").strip()
    title_noise = film_title_has_noise(title_value)
    source_priority = related_movie_source_priority(candidate_source)

    shared_directors = detail_directors & candidate_directors
    shared_genres = detail_genres & candidate_genres
    same_source = bool(detail_source and candidate_source and detail_source == candidate_source)
    same_status = bool(detail_status and candidate_status and detail_status == candidate_status)
    year_delta = None
    if detail_year.isdigit() and candidate_year.isdigit():
        year_delta = abs(int(detail_year) - int(candidate_year))

    score = 0
    reasons = []

    if shared_directors:
        if len(shared_directors) == 1:
            score += 120
            reasons.append("Same director")
        else:
            score += 135
            reasons.append("Same directing team")
    elif detail.get("director") and candidate.get("director"):
        director_mode = director_match_mode(detail.get("director"), candidate.get("director"))
        if director_mode == "exact":
            score += 90
            reasons.append("Director match")
        elif director_mode == "partial":
            score += 60
            reasons.append("Director match")

    if shared_genres:
        score += 22 + (min(len(shared_genres), 3) * 10)
        reasons.append("Shared genre")

    if same_source:
        score += 28
        reasons.append("Same source")
    elif source_priority >= 2:
        score += 14
        reasons.append("Library-backed source")

    if detail_category and candidate_category and detail_category == candidate_category:
        score += 16
        reasons.append("Same category")

    if same_status and detail_status:
        score += 8
    elif detail_status in PRIMARY_WATCH_NEXT_STATUSES and candidate_status in PRIMARY_WATCH_NEXT_STATUSES:
        score += 6

    if year_delta == 0:
        score += 18
        reasons.append("Same year")
    elif year_delta == 1:
        score += 12
    elif year_delta in (2, 3):
        score += 6

    if candidate_score_num > 0:
        score += min(candidate_score_num * 3, 24)
        reasons.append(f"Score signal: {candidate.get('score') or 'rated'}")

    score += metadata["score"] * 5
    if metadata["is_clean"]:
        score += 12
    if metadata["is_weak"]:
        score -= 22
        reasons.append("Weak metadata")
    if not title_value:
        score -= 40
        reasons.append("Missing title")
    if title_noise:
        score -= 18
        reasons.append("Noisy title")

    if not shared_directors and not shared_genres and not same_source and not same_status:
        score -= 24
        reasons.append("Loose fallback")

    if candidate_source == "My library and Ebert's":
        score += 8
    elif candidate_source:
        score += 4

    if detail_category in VALID_MOVIE_CATEGORIES and candidate_category not in VALID_MOVIE_CATEGORIES:
        score -= 12

    tier = 3
    if shared_directors and source_priority >= 2 and metadata["score"] >= 3 and not title_noise:
        tier = 0
    elif shared_directors:
        tier = 1
    elif shared_genres and source_priority >= 2 and metadata["score"] >= 3 and not title_noise:
        tier = 1
    elif shared_genres or same_source or same_status:
        tier = 2

    if metadata["is_weak"] and tier < 3:
        tier = max(tier, 2)

    return {
        "score": score,
        "tier": tier,
        "reasons": reasons,
        "shared_director_count": len(shared_directors),
        "shared_genre_count": len(shared_genres),
        "same_source": same_source,
        "source_priority": source_priority,
        "same_status": same_status,
        "year_delta": year_delta if year_delta is not None else "",
        "metadata_quality_score": metadata["score"],
        "metadata_is_clean": metadata["is_clean"],
        "metadata_is_weak": metadata["is_weak"],
        "title_noise": title_noise,
    }


def rank_movie_detail_related_entries(detail, films, limit=24):
    ranked = []
    detail_page_id = compact_notion_id((detail or {}).get("notion_page_id"))
    detail_title = movie_audit_title_from_row(detail)
    seen = set()
    for film in list(films or []):
        candidate_page_id = compact_notion_id(film.get("notion_page_id"))
        candidate_title = movie_audit_title_from_row(film)
        candidate_key = candidate_page_id or normalized_match_key(candidate_title)
        if not candidate_key or candidate_key in seen:
            continue
        seen.add(candidate_key)
        if candidate_page_id and candidate_page_id == detail_page_id:
            continue
        if candidate_title and normalized_match_key(candidate_title) == normalized_match_key(detail_title):
            continue
        if not candidate_title:
            continue
        profile = score_movie_related_candidate(detail, film)
        if profile["tier"] >= 3 and profile["score"] < 10:
            continue
        item = dict(film)
        item["related_score"] = profile["score"]
        item["related_tier"] = profile["tier"]
        item["related_reasons"] = profile["reasons"]
        item["related_shared_director_count"] = profile["shared_director_count"]
        item["related_shared_genre_count"] = profile["shared_genre_count"]
        item["related_same_source"] = profile["same_source"]
        item["related_source_priority"] = profile["source_priority"]
        item["related_same_status"] = profile["same_status"]
        item["related_year_delta"] = profile["year_delta"]
        item["related_metadata_quality_score"] = profile["metadata_quality_score"]
        item["related_metadata_is_clean"] = profile["metadata_is_clean"]
        item["related_metadata_is_weak"] = profile["metadata_is_weak"]
        item["related_title_noise"] = profile["title_noise"]
        ranked.append(item)

    ranked.sort(
        key=lambda item: (
            item.get("related_tier", 3),
            -int(item.get("related_shared_director_count", 0)),
            -int(item.get("related_score", 0)),
            -int(item.get("related_shared_genre_count", 0)),
            0 if item.get("related_same_source") else 1,
            0 if item.get("related_metadata_is_clean") else 1,
            int(item.get("related_title_noise", False)),
            int(item.get("related_year_delta") or 99),
            item.get("name", "").lower(),
        )
    )

    if not ranked:
        return []

    tier_0 = [item for item in ranked if item.get("related_tier") == 0]
    tier_1 = [item for item in ranked if item.get("related_tier") == 1]
    tier_2 = [item for item in ranked if item.get("related_tier") == 2]
    tier_3 = [item for item in ranked if item.get("related_tier") >= 3]

    selected = []
    for bucket in (tier_0, tier_1, tier_2, tier_3):
        for item in bucket:
            if item not in selected:
                selected.append(item)
            if len(selected) >= max(int(limit or 0), 1):
                return selected[:limit]
    return selected[:limit]


def watch_next_source_priority(source):
    normalized_source = normalize_movie_source(source)
    if normalized_source == "My library and Ebert's":
        return 3
    if normalized_source in {"My library", "Ebert's library"}:
        return 2
    return 1


def watch_next_candidate_tier(primary_pool, metadata, valid_category, has_title_noise, source_priority):
    # Keep the visible queue focused on clean, library-backed want-to items.
    if primary_pool and metadata["is_clean"] and valid_category and not has_title_noise and source_priority >= 2:
        return 0
    if primary_pool and metadata["score"] >= 4 and valid_category and not has_title_noise:
        return 1
    if primary_pool and metadata["score"] >= 3 and valid_category:
        return 2
    return 3


def watch_next_candidate_score(metadata, personal_score_num, primary_pool, valid_category, has_title_noise, source_priority):
    score = 0
    score += 220 if primary_pool else 40
    score += source_priority * 14
    score += metadata["score"] * 12
    score += personal_score_num * 8
    if metadata["is_clean"]:
        score += 18
    if valid_category:
        score += 10
    if not has_title_noise:
        score += 12
    if metadata["is_weak"]:
        score -= 28
    return score


def build_movie_recommendation_profile(films):
    profile = {
        "liked_count": 0,
        "strong_count": 0,
        "director_buckets": {},
        "genre_buckets": {},
        "source_buckets": {},
    }
    for film in list(films or []):
        title = movie_audit_title_from_row(film)
        score_num = film_detail_score_num(film)
        if score_num < 5:
            continue
        profile["liked_count"] += 1
        if score_num >= 7:
            profile["strong_count"] += 1
        for key in film_detail_director_keys(film):
            bucket = profile["director_buckets"].setdefault(key, {"count": 0, "titles": []})
            bucket["count"] += 1
            if score_num >= 7 and title and title not in bucket["titles"]:
                bucket["titles"].append(title)
            if len(bucket["titles"]) > 3:
                bucket["titles"] = bucket["titles"][:3]
        for key in film_detail_genre_keys(film):
            bucket = profile["genre_buckets"].setdefault(key, {"count": 0, "titles": []})
            bucket["count"] += 1
            if score_num >= 6 and title and title not in bucket["titles"]:
                bucket["titles"].append(title)
            if len(bucket["titles"]) > 3:
                bucket["titles"] = bucket["titles"][:3]
        source_value = normalize_movie_source(film.get("source", ""))
        if source_value:
            profile["source_buckets"][source_value] = profile["source_buckets"].get(source_value, 0) + 1
    return profile


def explain_movie_recommendation(candidate, profile=None):
    profile = profile or {}
    signals = []
    confidence = "fallback"
    title = movie_audit_title_from_row(candidate)
    candidate_source = normalize_movie_source((candidate or {}).get("source", ""))
    candidate_score_label = str((candidate or {}).get("score", "") or "").strip()
    candidate_score_num = film_detail_score_num(candidate)
    candidate_metadata_score = int((candidate or {}).get("metadata_quality_score") or 0)
    candidate_metadata_clean = bool((candidate or {}).get("metadata_quality_score", 0) >= 4 and not (candidate or {}).get("has_title_noise"))
    director_keys = film_detail_director_keys(candidate)
    genre_keys = film_detail_genre_keys(candidate)
    candidate_genre_names = {
        normalized_genre_key(genre.get("name", "")): str(genre.get("name") or "").strip()
        for genre in list((candidate or {}).get("genre_entries", []) or [])
        if str(genre.get("name") or "").strip()
    }

    for key in director_keys:
        bucket = (profile.get("director_buckets") or {}).get(key)
        if not bucket:
            continue
        examples = [item for item in bucket.get("titles", []) if item and item != title][:2]
        if examples:
            signals.append({
                "type": "director",
                "text": f"Same director as {', '.join(examples)}.",
            })
        else:
            signals.append({
                "type": "director",
                "text": "Same director as one of your higher-rated library picks.",
            })
        confidence = "high"
        break

    if confidence != "high":
        best_genre_key = None
        best_genre_bucket = None
        for key in genre_keys:
            bucket = (profile.get("genre_buckets") or {}).get(key)
            if not bucket:
                continue
            if not best_genre_bucket or bucket.get("count", 0) > best_genre_bucket.get("count", 0):
                best_genre_key = key
                best_genre_bucket = bucket
        if best_genre_bucket:
            genre_titles = [item for item in best_genre_bucket.get("titles", []) if item and item != title][:2]
            genre_name = candidate_genre_names.get(best_genre_key, "genre")
            if genre_titles:
                signals.append({
                    "type": "genre",
                    "text": f"Matches your high-rated {genre_name} pattern around {', '.join(genre_titles)}.",
                })
            else:
                signals.append({
                    "type": "genre",
                    "text": f"Matches a {genre_name} pattern that appears often in your higher-rated library titles.",
                })
            confidence = "medium"

    if candidate_source:
        source_count = (profile.get("source_buckets") or {}).get(candidate_source, 0)
        if source_count:
            signals.append({
                "type": "source",
                "text": f"Source signal: {candidate_source} appears often in your library picks.",
            })
        else:
            signals.append({
                "type": "source",
                "text": f"Source signal: {candidate_source}.",
            })
        if confidence == "fallback":
            confidence = "medium"

    if candidate_score_num > 0:
        signals.append({
            "type": "score",
            "text": f"Library rating signal: {candidate_score_label or candidate_score_num}.",
        })
        if confidence == "fallback" and candidate_score_num >= 5:
            confidence = "medium"

    if candidate_metadata_clean:
        signals.append({
            "type": "metadata",
            "text": f"Clean metadata ({candidate_metadata_score}/5 fields).",
        })

    if not signals:
        signals.append({
            "type": "fallback",
            "text": "No strong pattern signal stood out, so this is a safe fallback from the eligible pool.",
        })

    summary_parts = [signals[0]["text"]]
    if len(signals) > 1:
        summary_parts.append(signals[1]["text"])
    summary = " ".join(summary_parts).strip()
    if not summary:
        summary = "Safe fallback from the eligible pool."

    return {
        "summary": summary,
        "signals": signals[:4],
        "confidence": confidence if confidence != "fallback" or len(signals) == 1 else "medium",
    }


def build_movie_curation_candidates(use_case="watch_next", category_filter="", source_filter="", limit=None):
    normalized_use_case = re.sub(r"[^a-z_]+", "_", str(use_case or "watch_next").strip().lower()).strip("_") or "watch_next"
    normalized_category_filter = normalize_movie_category(category_filter)
    normalized_source_filter = normalize_source_filter(source_filter)
    if normalized_use_case == "watch_next" and movie_want_to_union_fetch_enabled():
        source_films = [build_film_entry(film) for film in fetch_want_to_films_for_flagged_paths()]
    else:
        source_films = [build_film_entry(film) for film in fetch_all_films()]
    recommendation_profile = None
    if normalized_use_case == "watch_next":
        recommendation_profile = build_movie_recommendation_profile(
            [build_film_entry(film) for film in fetch_library_films_for_flagged_paths()]
        )
    candidates = []
    excluded_watched = 0
    excluded_category = 0
    excluded_source = 0
    excluded_weak = 0

    for film in source_films:
        if not movie_matches_source_filter(film, normalized_source_filter):
            excluded_source += 1
            continue

        title_value = str(film.get("name") or "").strip()
        if normalized_use_case == "watch_next" and not title_value:
            excluded_weak += 1
            continue

        category = normalize_movie_category(film.get("category", ""))
        if normalized_category_filter and category != normalized_category_filter:
            excluded_category += 1
            continue

        watched = is_watched_film_status(film)
        if normalized_use_case == "watch_next" and watched:
            excluded_watched += 1
            continue

        metadata = film_metadata_quality_summary(film)
        status_value = str(film.get("status") or "").strip()
        category_value = str(film.get("category") or "").strip()
        personal_score_num = int(film.get("score_num") or 0)
        primary_pool = is_primary_watch_next_status(film)
        valid_category = normalize_movie_category(category_value) in VALID_MOVIE_CATEGORIES
        source_value = normalize_movie_source(film.get("source", ""))
        source_priority = watch_next_source_priority(source_value)
        has_title_noise = film_title_has_noise(film.get("name", ""))
        reasons = []
        tags = []
        watch_next_tier = 3
        curation_score = watch_next_candidate_score(
            metadata=metadata,
            personal_score_num=personal_score_num,
            primary_pool=primary_pool,
            valid_category=valid_category,
            has_title_noise=has_title_noise,
            source_priority=source_priority,
        )

        if primary_pool:
            reasons.append(f"Primary watch-next status: {status_value or 'i want to'}")
            tags.append("primary-pool")
        else:
            reasons.append(f"Eligible unseen status: {status_value or 'unclassified'}")
            tags.append("secondary-pool")

        if personal_score_num > 0:
            reasons.append(f"Personal score signal: {film.get('score')}")
            tags.append("interest-signal")

        if metadata["is_clean"]:
            reasons.append(f"Clean metadata ({metadata['score']}/{metadata['max_score']})")
            tags.append("clean-metadata")
        else:
            reasons.append(f"Metadata depth {metadata['score']}/{metadata['max_score']}")
            if metadata["missing_fields"]:
                reasons.append("Missing: " + ", ".join(metadata["missing_fields"][:3]))

        if source_priority >= 2:
            reasons.append(f"Library-backed source: {source_value}")
            tags.append("library-backed")
        elif source_value:
            reasons.append(f"Source signal: {source_value}")

        watch_next_tier = watch_next_candidate_tier(
            primary_pool=primary_pool,
            metadata=metadata,
            valid_category=valid_category,
            has_title_noise=has_title_noise,
            source_priority=source_priority,
        )

        if not valid_category:
            reasons.append("Category needs review")
            tags.append("needs-category-review")

        if has_title_noise:
            reasons.append("Title formatting looks noisy")
            tags.append("needs-title-review")

        if metadata["is_weak"]:
            reasons.append("Weak metadata entry")
            tags.append("weak-metadata")

        explanation = explain_movie_recommendation(film, recommendation_profile) if normalized_use_case == "watch_next" else {
            "summary": "; ".join(reasons[:2]) if reasons else "Recommendation candidate.",
            "signals": [{"type": "status", "text": reason} for reason in reasons[:3]],
            "confidence": "medium" if reasons else "fallback",
        }

        candidates.append({
            "entry_id": film.get("entry_id", ""),
            "detail_url": url_for("video_detail", entry_id=film.get("entry_id", "")),
            "name": title_value or "Untitled",
            "category": category_value,
            "status": status_value,
            "source": source_value,
            "score": film.get("score", ""),
            "score_num": personal_score_num,
            "poster": film.get("poster", ""),
            "year": film.get("year", ""),
            "director": film.get("director", ""),
            "genres": film.get("genres", ""),
            "runtime": film.get("runtime", ""),
            "overview": film.get("overview", ""),
            "trailer": film.get("trailer", ""),
            "watch_date": film.get("watch_date", ""),
            "finish_date": film.get("finish_date", ""),
            "rewatch": film.get("rewatch", ""),
            "tmdb_rating": film.get("tmdb_rating", ""),
            "pool": "primary" if primary_pool else "secondary",
            "is_primary_pool": primary_pool,
            "is_watched": watched,
            "has_valid_category": valid_category,
            "has_title_noise": has_title_noise,
            "metadata_quality_score": metadata["score"],
            "metadata_quality_max": metadata["max_score"],
            "metadata_missing_fields": metadata["missing_fields"],
            "curation_score": curation_score,
            "watch_next_tier": watch_next_tier,
            "curation_reasons": reasons,
            "curation_tags": tags,
            "recommendation_explanation": explanation,
            "recommendation_reason": explanation.get("summary", ""),
        })

    if normalized_use_case == "watch_next":
        candidates = [item for item in candidates if item["watch_next_tier"] <= 2 or item["is_primary_pool"]]

    candidates.sort(
        key=lambda item: (
            item["watch_next_tier"],
            -int(item["is_primary_pool"]),
            -int(item["curation_score"]),
            -int(item["score_num"]),
            -int(item["metadata_quality_score"]),
            int(item["has_title_noise"]),
            0 if item["has_valid_category"] else 1,
            0 if item["source"] == "My library and Ebert's" else 1 if item["source"] in {"My library", "Ebert's library"} else 2,
            item["name"].lower(),
        )
    )

    for index, item in enumerate(candidates, start=1):
        item["rank"] = index

    if limit in (None, "", "All"):
        limited_candidates = candidates
    else:
        max_count = max(int(limit), 1)
        preferred_candidates = [item for item in candidates if item["watch_next_tier"] <= 1]
        if len(preferred_candidates) >= max_count:
            limited_candidates = preferred_candidates[:max_count]
        else:
            fallback_candidates = [item for item in candidates if item["watch_next_tier"] > 1]
            limited_candidates = preferred_candidates + fallback_candidates[: max_count - len(preferred_candidates)]

    primary_count = sum(1 for item in candidates if item["is_primary_pool"])
    clean_count = sum(1 for item in candidates if item["metadata_quality_score"] >= 4 and item["has_valid_category"] and not item["has_title_noise"])
    tier_counts = {
        "tier_0": sum(1 for item in candidates if item["watch_next_tier"] == 0),
        "tier_1": sum(1 for item in candidates if item["watch_next_tier"] == 1),
        "tier_2": sum(1 for item in candidates if item["watch_next_tier"] == 2),
        "tier_3": sum(1 for item in candidates if item["watch_next_tier"] == 3),
    }

    return {
        "use_case": normalized_use_case,
        "category_filter": normalized_category_filter,
        "source_filter": normalized_source_filter,
        "summary": {
            "total_source_titles": len(source_films),
            "total_candidates": len(candidates),
            "returned_candidates": len(limited_candidates),
            "primary_pool": primary_count,
            "secondary_pool": max(len(candidates) - primary_count, 0),
            "clean_candidates": clean_count,
            "tier_counts": tier_counts,
            "excluded_watched": excluded_watched,
            "excluded_category": excluded_category,
            "excluded_source": excluded_source,
            "excluded_weak": excluded_weak,
        },
        "items": limited_candidates,
        "recommendation_explanation": limited_candidates[0].get("recommendation_explanation", {}) if limited_candidates else {},
    }


def normalize_correction_header(value):
    return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())


def clean_correction_text(value):
    return re.sub(r"\s+", " ", str(value or "").strip())


def normalize_year_value(value):
    raw = clean_correction_text(value)
    if not raw:
        return ""
    if re.fullmatch(r"\d{4}\.0+", raw):
        return raw.split(".", 1)[0]
    if re.fullmatch(r"\d+(\.\d+)?", raw):
        try:
            numeric = float(raw)
        except ValueError:
            numeric = None
        if numeric is not None and numeric.is_integer():
            year = int(numeric)
            if 1800 <= year <= 2100:
                return str(year)
    match = re.search(r"\b(18|19|20)\d{2}\b", raw)
    return match.group(0) if match else ""


def normalized_person_key(value):
    return re.sub(r"[^a-z0-9]+", "", clean_correction_text(value).lower())


def normalize_correction_row(raw_row, source_file, row_number):
    normalized = {key: "" for key in CSV_CORRECTION_SCHEMA}
    extras = {}
    for key, value in (raw_row or {}).items():
        canonical_key = CSV_CORRECTION_HEADER_ALIASES.get(normalize_correction_header(key), "")
        cleaned_value = clean_correction_text(value)
        if canonical_key:
            normalized[canonical_key] = cleaned_value
        elif cleaned_value:
            extras[key] = cleaned_value

    normalized["year"] = normalize_year_value(normalized["year"])
    if not normalized["notes"]:
        extra_notes = []
        for extra_key, extra_value in extras.items():
            if normalize_correction_header(extra_key) in {"type", "source"}:
                extra_notes.append(f"{extra_key}: {extra_value}")
        normalized["notes"] = " | ".join(extra_notes)

    return {
        "source_file": source_file,
        "row_number": row_number,
        "original_title": normalized["original_title"],
        "corrected_title": normalized["corrected_title"],
        "director": normalized["director"],
        "year": normalized["year"],
        "notes": normalized["notes"],
    }


def load_csv_correction_rows():
    rows = []
    if not CSV_CORRECTIONS_DIR.exists():
        return rows
    for path in sorted(CSV_CORRECTIONS_DIR.rglob("*.csv")):
        try:
            with path.open("r", encoding="utf-8-sig", newline="") as handle:
                reader = csv.DictReader(handle)
                for row_number, raw_row in enumerate(reader, start=2):
                    normalized_row = normalize_correction_row(raw_row, path.name, row_number)
                    if not (normalized_row["corrected_title"] or normalized_row["original_title"]):
                        continue
                    rows.append(normalized_row)
        except Exception:
            continue
    return rows


def build_movie_match_indexes():
    films = [build_film_entry(film) for film in fetch_all_films()]
    title_year = {}
    title_director = {}
    title_only = {}
    for film in films:
        title_key = normalized_match_key(film.get("name", ""))
        year_key = normalize_year_value(film.get("year", ""))
        director_key = normalized_person_key(film.get("director", ""))
        if title_key:
            title_only.setdefault(title_key, []).append(film)
            if year_key:
                title_year.setdefault((title_key, year_key), []).append(film)
            if director_key:
                title_director.setdefault((title_key, director_key), []).append(film)
    return {
        "films": films,
        "title_year": title_year,
        "title_director": title_director,
        "title_only": title_only,
    }


def unique_title_candidates(correction_row):
    ordered = []
    for value in (correction_row.get("corrected_title", ""), correction_row.get("original_title", "")):
        cleaned = clean_correction_text(value)
        if cleaned and cleaned not in ordered:
            ordered.append(cleaned)
    return ordered


def load_exported_movies_csv_rows():
    csv_path = EXPORTS_DIR / "movies_export.csv"
    rows = []
    if not csv_path.exists():
        return rows
    try:
        with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row_number, raw_row in enumerate(reader, start=2):
                row = {}
                for field in MOVIE_EXPORT_FIELDS:
                    row[field] = clean_correction_text((raw_row or {}).get(field, ""))
                row["row_number"] = row_number
                rows.append(row)
    except Exception:
        return []
    return rows


def load_movie_metadata_mismatches():
    rows = []
    if not MISMATCH_CSV_PATH.exists():
        return rows
    try:
        with MISMATCH_CSV_PATH.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row_number, raw_row in enumerate(reader, start=2):
                movie_name = clean_correction_text((raw_row or {}).get("movie", ""))
                corrected_title = clean_correction_text((raw_row or {}).get("corrected_title", ""))
                corrected_director = clean_correction_text((raw_row or {}).get("corrected_director", ""))
                corrected_year = normalize_year_value((raw_row or {}).get("corrected_year", ""))
                current_director = clean_correction_text((raw_row or {}).get("current_director", ""))
                current_year = normalize_year_value((raw_row or {}).get("current_year", ""))
                issue = clean_correction_text((raw_row or {}).get("issue", ""))
                if not (movie_name or corrected_title):
                    continue
                rows.append({
                    "row_number": row_number,
                    "movie": movie_name,
                    "corrected_title": corrected_title,
                    "corrected_director": corrected_director,
                    "corrected_year": corrected_year,
                    "current_director": current_director,
                    "current_year": current_year,
                    "issue": issue,
                })
    except Exception:
        return []
    return rows


def build_mismatch_lookup(mismatch_rows):
    lookup = {}
    for row in mismatch_rows:
        keys = []
        for value in (row.get("corrected_title", ""), row.get("movie", "")):
            key = normalized_match_key(value)
            if key and key not in keys:
                keys.append(key)
        for key in keys:
            lookup.setdefault(key, []).append(row)
    return lookup


def build_export_movie_match_indexes(export_rows):
    title_year = {}
    title_director = {}
    title_only = {}
    for film in export_rows:
        title_key = normalized_match_key(film.get("name", ""))
        year_key = normalize_year_value(film.get("year", ""))
        director_key = normalized_person_key(film.get("director", ""))
        if title_key:
            title_only.setdefault(title_key, []).append(film)
            if year_key:
                title_year.setdefault((title_key, year_key), []).append(film)
            if director_key:
                title_director.setdefault((title_key, director_key), []).append(film)
    return {
        "films": export_rows,
        "title_year": title_year,
        "title_director": title_director,
        "title_only": title_only,
    }


def first_unique_match(candidates_map, keys):
    for key in keys:
        matches = candidates_map.get(key, [])
        if len(matches) == 1:
            return matches[0], None
        if len(matches) > 1:
            return None, matches
    return None, None


def build_correction_preview_report():
    correction_rows = load_csv_correction_rows()
    export_rows = load_exported_movies_csv_rows()
    indexes = build_export_movie_match_indexes(export_rows)
    report_rows = []
    summary = {
        "csv_files": len({row["source_file"] for row in correction_rows}),
        "csv_rows": len(correction_rows),
        "export_rows": len(export_rows),
        "strong_matches": 0,
        "possible_matches": 0,
        "conflicts": 0,
        "wrong_section_rows": 0,
        "no_match_rows": 0,
    }

    for row in correction_rows:
        candidate_titles = unique_title_candidates(row)
        title_keys = [normalized_match_key(value) for value in candidate_titles if normalized_match_key(value)]
        year_key = normalize_year_value(row.get("year", ""))
        director_key = normalized_person_key(row.get("director", ""))
        matched_entry = None
        conflict_matches = []
        confidence = "no-match"
        match_rule = "no_match"

        if title_keys and year_key:
            matched_entry, duplicate_matches = first_unique_match(
                indexes["title_year"],
                [(title_key, year_key) for title_key in title_keys]
            )
            if duplicate_matches:
                conflict_matches = duplicate_matches
                confidence = "conflict"
                match_rule = "title+year"
        if confidence == "no-match" and not matched_entry and title_keys and director_key:
            matched_entry, duplicate_matches = first_unique_match(
                indexes["title_director"],
                [(title_key, director_key) for title_key in title_keys]
            )
            if matched_entry:
                confidence = "strong"
                match_rule = "title+director"
            elif duplicate_matches:
                conflict_matches = duplicate_matches
                confidence = "conflict"
                match_rule = "title+director"
        if confidence == "no-match" and not matched_entry and title_keys:
            matched_entry, duplicate_matches = first_unique_match(indexes["title_only"], title_keys)
            if matched_entry:
                confidence = "possible"
                match_rule = "title-only"
            elif duplicate_matches:
                conflict_matches = duplicate_matches
                confidence = "conflict"
                match_rule = "title-only"

        if matched_entry and confidence == "no-match":
            confidence = "strong"
            match_rule = "title+year"
        row_type = "non_movie" if matched_entry and is_non_movie_category(matched_entry.get("category", "")) else "real"
        if row_type == "non_movie":
            confidence = "wrong-section"
            match_rule = "wrong-section"

        if confidence == "strong":
            summary["strong_matches"] += 1
        elif confidence == "possible":
            summary["possible_matches"] += 1
        elif confidence == "conflict":
            summary["conflicts"] += 1
        elif confidence == "wrong-section":
            summary["wrong_section_rows"] += 1
        else:
            summary["no_match_rows"] += 1

        report_rows.append({
            "source_file": row["source_file"],
            "row_number": row["row_number"],
            "original_title": row["original_title"],
            "corrected_title": row["corrected_title"],
            "director": row["director"],
            "year": row["year"],
            "notes": row["notes"],
            "confidence": confidence,
            "row_type": row_type,
            "match_rule": match_rule,
            "reason": csv_preview_reason(match_rule, confidence),
            "matched_entry": matched_entry,
            "matched_entry_title": matched_entry.get("name", "") if matched_entry else "",
            "matched_entry_detail_url": "",
            "current_title": matched_entry.get("name", "") if matched_entry else "",
            "current_director": matched_entry.get("director", "") if matched_entry else "",
            "current_year": normalize_year_value(matched_entry.get("year", "")) if matched_entry else "",
            "conflict_titles": [item.get("name", "") for item in conflict_matches[:5]],
        })

    grouped_rows = {
        "strong_matches": [row for row in report_rows if row["confidence"] == "strong"],
        "possible_matches": [row for row in report_rows if row["confidence"] == "possible"],
        "conflicts": [row for row in report_rows if row["confidence"] == "conflict"],
        "wrong_section_rows": [row for row in report_rows if row["confidence"] == "wrong-section"],
        "no_match_rows": [row for row in report_rows if row["confidence"] == "no-match"],
    }
    return {"summary": summary, "rows": report_rows, "groups": grouped_rows}


def notion_title_property_name(properties):
    props = properties or {}
    if props.get("Name", {}).get("type") == "title":
        return "Name"
    for key, prop in props.items():
        if prop.get("type") == "title":
            return key
    return "Name"


def notion_property_plain_text(prop_value):
    if not isinstance(prop_value, dict):
        return ""
    prop_type = prop_value.get("type", "")
    if prop_type == "title":
        return "".join(item.get("plain_text", "") for item in prop_value.get("title", [])).strip()
    if prop_type == "rich_text":
        return "".join(item.get("plain_text", "") for item in prop_value.get("rich_text", [])).strip()
    return ""


def csv_preview_reason(match_rule, confidence):
    rule = clean_correction_text(match_rule).lower()
    level = clean_correction_text(confidence).lower()
    if rule == "wrong-section":
        return "Non-movie / wrong-section item"
    if rule == "title+year":
        return "CSV strong match by title+year"
    if rule == "title+director":
        return "CSV strong match by title+director"
    if rule == "title-only":
        return "Normalized title match only"
    if level == "conflict":
        return "Multiple current entries matched this correction row"
    if level == "no-match":
        return "No safe current movie match found"
    return "CSV correction preview match"


def csv_apply_confidence_level(report_row):
    confidence = clean_correction_text((report_row or {}).get("confidence", "")).lower()
    if confidence == "strong":
        return "high"
    if confidence == "possible":
        return "medium"
    if confidence == "wrong-section":
        return "blocked"
    return confidence or "unknown"


def build_csv_correction_apply_key(item):
    return "|".join([
        clean_correction_text(item.get("notion_page_id", "")),
        clean_correction_text(item.get("source_file", "")),
        str(item.get("row_number", 0)),
    ])


def build_csv_correction_notion_payload(properties, correction_row):
    props = properties or {}
    payload = {}
    title_property = notion_title_property_name(props)
    corrected_title = clean_correction_text(correction_row.get("corrected_title", ""))
    corrected_director = clean_correction_text(correction_row.get("director", ""))
    corrected_year = normalize_year_value(correction_row.get("year", ""))

    if corrected_title:
        current_title = notion_property_plain_text(props.get(title_property, {}))
        if current_title != corrected_title:
            payload[title_property] = {"title": notion_title_text(corrected_title[:2000])}
    if corrected_director:
        current_director = notion_property_plain_text(props.get("Director", {}))
        if current_director != corrected_director:
            payload["Director"] = {
                "rich_text": [{"type": "text", "text": {"content": corrected_director[:2000]}}]
            }
    if corrected_year:
        current_year = props.get("Year", {}).get("number")
        current_year_value = str(int(current_year)) if isinstance(current_year, (int, float)) else ""
        if current_year_value != corrected_year:
            payload["Year"] = {"number": int(corrected_year)}
    return payload


def ensure_correction_target_properties():
    response = requests.get(
        f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}",
        headers=notion_api_headers(),
        timeout=20
    )
    response.raise_for_status()
    properties = (response.json() or {}).get("properties", {})
    desired = {}
    if "Director" not in properties:
        desired["Director"] = {"rich_text": {}}
    if "Year" not in properties:
        desired["Year"] = {"number": {"format": "number"}}
    if not desired:
        return properties
    patch_response = requests.patch(
        f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}",
        headers=notion_api_headers(),
        json={"properties": desired},
        timeout=20
    )
    patch_response.raise_for_status()
    return (patch_response.json() or {}).get("properties", properties)


def resolve_live_notion_film(export_row, films=None):
    current_title = clean_correction_text(export_row.get("name", ""))
    current_year = normalize_year_value(export_row.get("year", ""))
    current_director = normalized_person_key(export_row.get("director", ""))
    live_films = films if films is not None else [build_film_entry(film) for film in fetch_all_films(force_refresh=True)]
    title_key = normalized_match_key(current_title)
    if not title_key:
        return None
    if current_year:
        matches = [
            film for film in live_films
            if normalized_match_key(film.get("name", "")) == title_key
            and normalize_year_value(film.get("year", "")) == current_year
        ]
        if len(matches) == 1:
            return matches[0]
    if current_director:
        matches = [
            film for film in live_films
            if normalized_match_key(film.get("name", "")) == title_key
            and normalized_person_key(film.get("director", "")) == current_director
        ]
        if len(matches) == 1:
            return matches[0]
    matches = [film for film in live_films if normalized_match_key(film.get("name", "")) == title_key]
    return matches[0] if len(matches) == 1 else None


def build_strong_correction_apply_plan():
    report = build_correction_preview_report()
    live_films = [build_film_entry(film) for film in fetch_all_films(force_refresh=True)]
    mismatch_rows = load_movie_metadata_mismatches()
    mismatch_lookup = build_mismatch_lookup(mismatch_rows)
    plan_items = []
    seen_signatures = set()
    skipped_mismatch_rows = 0
    candidate_rows = list(report["groups"]["strong_matches"]) + list(report["groups"]["possible_matches"])
    for row in candidate_rows:
        mismatch_candidates = mismatch_lookup.get(normalized_match_key(row.get("matched_entry_title", "")), [])
        mismatch_row = mismatch_candidates[0] if len(mismatch_candidates) == 1 else {}
        if len(mismatch_candidates) > 1:
            skipped_mismatch_rows += 1
            continue
        matched_entry = resolve_live_notion_film(row.get("matched_entry") or {}, films=live_films)
        notion_page_id = (matched_entry or {}).get("notion_page_id", "")
        if not notion_page_id:
            continue
        changes = {}
        corrected_title = clean_correction_text(row.get("corrected_title", "")) or clean_correction_text(row.get("current_title", ""))
        corrected_director = clean_correction_text(row.get("director", ""))
        corrected_year = normalize_year_value(row.get("year", ""))
        mismatch_director = clean_correction_text(mismatch_row.get("corrected_director", ""))
        mismatch_year = normalize_year_value(mismatch_row.get("corrected_year", ""))
        if mismatch_director and corrected_director and mismatch_director != corrected_director:
            skipped_mismatch_rows += 1
            continue
        if mismatch_year and corrected_year and mismatch_year != corrected_year:
            skipped_mismatch_rows += 1
            continue
        if mismatch_director:
            corrected_director = mismatch_director
        if mismatch_year:
            corrected_year = mismatch_year
        current_title = clean_correction_text(row.get("current_title", ""))
        current_director = clean_correction_text(row.get("current_director", ""))
        current_year = normalize_year_value(row.get("current_year", ""))
        if corrected_title and corrected_title != current_title:
            changes["title"] = {"from": current_title, "to": corrected_title}
        if corrected_director and corrected_director != current_director:
            changes["director"] = {"from": current_director, "to": corrected_director}
        if corrected_year and corrected_year != current_year:
            changes["year"] = {"from": current_year, "to": corrected_year}
        if not changes:
            continue
        signature = (
            notion_page_id,
            corrected_title,
            corrected_director,
            corrected_year,
        )
        if signature in seen_signatures:
            continue
        seen_signatures.add(signature)
        confidence_level = csv_apply_confidence_level(row)
        plan_item = {
            "source_file": row.get("source_file", ""),
            "row_number": row.get("row_number", 0),
            "mismatch_row_number": mismatch_row.get("row_number", 0) if mismatch_row else 0,
            "confidence": row.get("confidence", ""),
            "confidence_level": confidence_level,
            "match_rule": row.get("match_rule", ""),
            "reason": csv_preview_reason(row.get("match_rule", ""), row.get("confidence", "")),
            "original_title": row.get("original_title", ""),
            "corrected_title": corrected_title,
            "director": corrected_director,
            "year": corrected_year,
            "notes": row.get("notes", ""),
            "mismatch_issue": mismatch_row.get("issue", "") if mismatch_row else "",
            "notion_page_id": notion_page_id,
            "current_title": current_title,
            "current_director": current_director,
            "current_year": current_year,
            "changes": changes,
        }
        plan_item["apply_key"] = build_csv_correction_apply_key(plan_item)
        plan_item["default_selected"] = confidence_level == "high"
        plan_items.append(plan_item)
    selected_default_count = sum(1 for item in plan_items if item.get("default_selected"))
    return {
        "summary": {
            "total_reviewed": len(report["rows"]),
            "mismatch_rows": len(mismatch_rows),
            "planned_updates": len(plan_items),
            "skipped_by_mismatch_filter": skipped_mismatch_rows,
            "selected_default": selected_default_count,
            "high_confidence": sum(1 for item in plan_items if item.get("confidence_level") == "high"),
            "medium_confidence": sum(1 for item in plan_items if item.get("confidence_level") == "medium"),
            "title_updates": sum(1 for item in plan_items if "title" in item["changes"]),
            "director_updates": sum(1 for item in plan_items if "director" in item["changes"]),
            "year_updates": sum(1 for item in plan_items if "year" in item["changes"]),
        },
        "items": plan_items,
    }


def save_correction_report(report_payload, prefix):
    CORRECTION_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    target = CORRECTION_REPORTS_DIR / f"{prefix}-{stamp}.json"
    save_json_file(target, report_payload)
    return str(target)


def load_correction_report(path_value):
    if not path_value:
        return {}
    try:
        path = Path(path_value)
    except Exception:
        return {}
    if not path.exists() or not path.is_file():
        return {}
    data = load_json_file(path, {})
    return data if isinstance(data, dict) else {}


def build_movie_export_rows():
    films = fetch_all_films_from_notion()
    export_rows = []
    for film in films:
        row = {}
        for field in MOVIE_EXPORT_FIELDS:
            value = film.get(field, "")
            row[field] = "" if value is None else value
        export_rows.append(row)
    return export_rows


def export_movies_data():
    export_rows = build_movie_export_rows()
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    json_path = EXPORTS_DIR / "movies_export.json"
    csv_path = EXPORTS_DIR / "movies_export.csv"

    save_json_file(json_path, export_rows)
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(MOVIE_EXPORT_FIELDS))
        writer.writeheader()
        writer.writerows(export_rows)

    summary = {
        "total_exported": len(export_rows),
        "json_path": str(json_path),
        "csv_path": str(csv_path),
    }
    print(f"[movies-export] total exported: {summary['total_exported']}")
    print(f"[movies-export] json: {summary['json_path']}")
    print(f"[movies-export] csv: {summary['csv_path']}")
    return summary


def apply_strong_csv_corrections(selected_keys=None):
    if not (NOTION_TOKEN or "").strip():
        raise RuntimeError("Missing NOTION_TOKEN.")
    if not (NOTION_DATABASE_ID or "").strip():
        raise RuntimeError("Missing NOTION_DATABASE_ID.")

    plan = build_strong_correction_apply_plan()
    if selected_keys is None:
        selected_items = list(plan["items"])
        selected_key_set = {
            clean_correction_text(item.get("apply_key", "")) for item in selected_items if clean_correction_text(item.get("apply_key", ""))
        }
    else:
        selected_key_set = {
            clean_correction_text(value) for value in selected_keys if clean_correction_text(value)
        }
        selected_items = [
            item for item in plan["items"]
            if clean_correction_text(item.get("apply_key", "")) in selected_key_set
        ]
    selected_summary = dict(plan["summary"])
    selected_summary["selected_items"] = len(selected_items)
    selected_summary["selected_high_confidence"] = sum(
        1 for item in selected_items if item.get("confidence_level") == "high"
    )
    selected_summary["selected_medium_confidence"] = sum(
        1 for item in selected_items if item.get("confidence_level") == "medium"
    )
    backup_report = {
        "status": "planned",
        "created_at": current_timestamp(),
        "summary": selected_summary,
        "selected_keys": sorted(selected_key_set),
        "items": selected_items,
    }
    backup_path = save_correction_report(backup_report, "csv-corrections-backup")

    if not selected_items:
        return {
            "backup_path": backup_path,
            "applied_path": "",
            "applied": 0,
            "skipped": 0,
            "failed": 0,
            "summary": selected_summary,
        }

    ensure_correction_target_properties()
    pages = {page.get("id", ""): page for page in fetch_all_notion_database_pages()}
    applied_items = []
    skipped_items = []
    failed_items = []

    for item in selected_items:
        page = pages.get(item["notion_page_id"])
        if not page:
            skipped_items.append({**item, "reason": "page_not_found"})
            continue
        properties = page.get("properties", {}) or {}
        payload = build_csv_correction_notion_payload(properties, item)
        if not payload:
            skipped_items.append({**item, "reason": "no_effective_change"})
            continue
        try:
            update_notion_page_properties(item["notion_page_id"], payload)
            applied_items.append({**item, "applied_properties": sorted(payload.keys())})
        except requests.RequestException as exc:
            failed_items.append({**item, "reason": str(exc), "attempted_properties": sorted(payload.keys())})

    clear_runtime_cache()
    refresh_film_cache_from_source()

    changed_fields = {
        "title": sum(1 for item in applied_items if "title" in item.get("changes", {})),
        "director": sum(1 for item in applied_items if "director" in item.get("changes", {})),
        "year": sum(1 for item in applied_items if "year" in item.get("changes", {})),
    }
    sample_changes = []
    for item in applied_items[:5]:
        sample_changes.append({
            "title": item.get("changes", {}).get("title", {}).get("from", item.get("current_title", "")),
            "title_before": item.get("changes", {}).get("title", {}).get("from", item.get("current_title", "")),
            "title_after": item.get("changes", {}).get("title", {}).get("to", item.get("current_title", "")),
            "director_before": item.get("changes", {}).get("director", {}).get("from", item.get("current_director", "")),
            "director_after": item.get("changes", {}).get("director", {}).get("to", item.get("current_director", "")),
            "year_before": item.get("changes", {}).get("year", {}).get("from", item.get("current_year", "")),
            "year_after": item.get("changes", {}).get("year", {}).get("to", item.get("current_year", "")),
        })

    applied_report = {
        "status": "applied",
        "created_at": current_timestamp(),
        "backup_path": backup_path,
        "summary": {
            **selected_summary,
            "applied": len(applied_items),
            "skipped": len(skipped_items),
            "failed": len(failed_items),
            "changed_fields": changed_fields,
        },
        "applied_items": applied_items,
        "skipped_items": skipped_items,
        "failed_items": failed_items,
        "sample_changes": sample_changes,
    }
    applied_path = save_correction_report(applied_report, "csv-corrections-applied")
    return {
        "backup_path": backup_path,
        "applied_path": applied_path,
        "applied": len(applied_items),
        "skipped": len(skipped_items),
        "failed": len(failed_items),
        "summary": applied_report["summary"],
    }


def collect_all_youtube_entries():
    sections = ["German", "Chess", "Library", "YouTube Watch Later"]
    entries = []
    for section in sections:
        for playlist in get_section_playlists(section):
            videos = get_all_playlist_videos(playlist["id"])
            for video in videos:
                entry = build_video_entry(video, playlist["name"], playlist["url"])
                entry["section"] = section
                entries.append(entry)
    return entries


def _iter_cached_pockettube_group_feeds():
    seen_section_keys = set()
    with RUNTIME_CACHE_LOCK:
        runtime_feeds = dict(RUNTIME_CACHE.get("youtube_section_feeds", {}) or {})
    for section_key, payload in runtime_feeds.items():
        if section_key in seen_section_keys or not isinstance(payload, dict):
            continue
        seen_section_keys.add(section_key)
        yield section_key, payload

    cache_data = load_cache_data()
    for section_key, payload in (cache_data.get("youtube_section_feeds", {}) or {}).items():
        if section_key in seen_section_keys or not isinstance(payload, dict):
            continue
        feed_payload = payload.get("data", payload)
        if not isinstance(feed_payload, dict):
            continue
        seen_section_keys.add(section_key)
        yield section_key, feed_payload


def _build_pockettube_group_detail_context(entry, feed_context):
    detail = dict(entry or {})
    video_id = str(detail.get("video_id", "") or "").strip()
    playlist_item_id = str(detail.get("playlist_item_id", "") or "").strip()
    entry_id = str(detail.get("entry_id", "") or "").strip() or f"yt-{playlist_item_id or video_id}"
    watch_key = video_id or playlist_item_id or entry_id
    state_key = watch_key
    group_key = str(feed_context.get("group_key", "") or feed_context.get("channel_group_key", "") or feed_context.get("playlist_id", "") or "").strip()
    section_name = str(feed_context.get("name", "") or feed_context.get("group_name", "") or "PocketTube Group Feed").strip() or "PocketTube Group Feed"
    section_slug_value = str(feed_context.get("slug", "") or section_slug(section_name)).strip() or section_slug(section_name)
    playlist_name = str(feed_context.get("group_name", "") or section_name).strip() or section_name
    playlist_url = f"/section/{section_slug_value}"
    source_name = str(feed_context.get("source_name", "PocketTube") or "PocketTube").strip() or "PocketTube"
    duration_seconds = int(detail.get("duration_seconds", 0) or 0)
    if not duration_seconds and video_id:
        try:
            duration_seconds = int(get_youtube_duration(video_id).get("seconds", 0) or 0)
        except Exception:
            duration_seconds = 0
    duration_display = str(detail.get("duration", "") or "").strip()
    if not duration_display and duration_seconds:
        duration_display = format_duration(duration_seconds)
    thumb = str(detail.get("thumb", "") or "").strip()

    detail["entry_id"] = entry_id
    detail["watch_key"] = watch_key
    detail["state_key"] = state_key
    detail["group_key"] = group_key
    detail["source_type"] = "youtube"
    detail["section"] = section_name
    detail["playlist_name"] = playlist_name
    detail["playlist_url"] = playlist_url
    detail["playlist_id"] = str(feed_context.get("group_key", "") or section_slug_value).strip()
    detail["group_name"] = str(feed_context.get("group_name", "") or playlist_name).strip() or playlist_name
    detail["group_key"] = group_key
    detail["source_name"] = source_name
    detail["url"] = detail.get("url") or f"https://www.youtube.com/watch?v={video_id}"
    detail["thumb"] = thumb
    detail["thumbnail_url"] = str(detail.get("thumbnail_url", "") or "").strip() or thumb
    detail["thumbnail"] = str(detail.get("thumbnail", "") or "").strip() or detail["thumbnail_url"]
    detail["image_url"] = str(detail.get("image_url", "") or "").strip() or detail["thumbnail_url"]
    detail["duration_seconds"] = duration_seconds
    detail["duration"] = duration_display or "0:00"
    detail["published_at"] = str(detail.get("published_at", "") or "").strip()
    detail["published_display"] = format_timestamp_label(detail["published_at"], default="")
    detail["category"] = str(detail.get("category", "") or "").strip() or str(detail.get("channel_name", "") or "").strip() or "Unknown Channel"
    detail["status"] = str(detail.get("status", "") or "").strip() or playlist_name or detail["duration"]
    detail["feed_source"] = "pockettube_group"
    detail["group_back_url"] = playlist_url
    detail["group_back_context"] = ""
    back_context_bits = []
    if feed_context.get("group_name"):
        back_context_bits.append(str(feed_context.get("group_name", "") or "").strip())
    if feed_context.get("feed_order"):
        back_context_bits.append("Latest per-channel shuffle" if feed_context.get("feed_order") == "shuffle" else "Normal feed")
    if feed_context.get("page") and feed_context.get("per_page"):
        back_context_bits.append(f"Page {feed_context.get('page')} · {feed_context.get('per_page')} per page")
    elif feed_context.get("page"):
        back_context_bits.append(f"Page {feed_context.get('page')}")
    if back_context_bits:
        detail["group_back_context"] = " · ".join(back_context_bits)
    back_query = {}
    for key in ("page", "per_page", "feed_order", "feed_shuffle_seed", "video_types_csv"):
        value = feed_context.get(key, "")
        if value not in ("", None, []):
            back_query[key] = value
    if back_query:
        back_query["video_types"] = back_query.pop("video_types_csv", feed_context.get("video_types_csv", ""))
        detail["group_back_url"] = f"{playlist_url}{build_query_url({}, **back_query)}"

    feed_items = _enrich_pockettube_feed_videos(
        feed_context.get("feed_all", []) or feed_context.get("feed_items", []) or [],
        fetch_missing=True,
    )

    playlist_entries = []
    for video in feed_items:
        if not isinstance(video, dict):
            continue
        summary = build_youtube_channel_video_summary(video)
        if not summary.get("entry_id"):
            continue
        summary["detail_url"] = f"/video/{summary.get('entry_id', '')}"
        playlist_entries.append(summary)

    current_index = next((index for index, video in enumerate(playlist_entries) if video.get("entry_id") == entry_id or video.get("video_id") == video_id), 0)
    prev_entry = playlist_entries[current_index - 1] if current_index > 0 else None
    next_entry = playlist_entries[current_index + 1] if current_index < len(playlist_entries) - 1 else None
    related_entries = [video for video in playlist_entries if video.get("entry_id") != entry_id]

    return {
        "entry": detail,
        "entry_type": "youtube",
        "player_video_id": video_id,
        "related_entries": related_entries,
        "related_title": f"{source_name} · {playlist_name}" if playlist_name else f"{source_name} Group Feed",
        "playlist_entries": playlist_entries,
        "prev_entry": prev_entry,
        "next_entry": next_entry,
    }


def _resolve_pockettube_group_detail_context(entry_id):
    lookup_entry_id = str(entry_id or "").strip()
    lookup_video_id = lookup_entry_id[3:] if lookup_entry_id.startswith("yt-") else lookup_entry_id
    lookup_playlist_item_id = lookup_video_id if lookup_entry_id.startswith("yt-") else ""
    if not lookup_entry_id:
        return None

    for _, feed_context in _iter_cached_pockettube_group_feeds():
        feed_items = list(feed_context.get("feed_all", []) or feed_context.get("feed_items", []) or feed_context.get("feed_preview", []) or [])
        for item in feed_items:
            if not isinstance(item, dict):
                continue
            item_entry_id = str(item.get("entry_id", "") or "").strip()
            item_video_id = str(item.get("video_id", "") or "").strip()
            item_playlist_item_id = str(item.get("playlist_item_id", "") or "").strip()
            if (
                item_entry_id == lookup_entry_id
                or (lookup_video_id and item_video_id == lookup_video_id)
                or (lookup_playlist_item_id and item_playlist_item_id == lookup_playlist_item_id)
            ):
                return _build_pockettube_group_detail_context(item, feed_context)
    return None

def get_video_detail_context(entry_id):
    if entry_id.startswith("film-"):
        films = [build_film_entry(film) for film in fetch_library_films_for_flagged_paths()]
        detail = next((film for film in films if film["entry_id"] == entry_id), None)
        if not detail:
            return None
        top_billed_cast = []
        try:
            tmdb_data = fetch_tmdb_enrichment(
                detail.get("name", ""),
                category=detail.get("category", ""),
                year=detail.get("year", ""),
            )
        except (requests.RequestException, RuntimeError):
            tmdb_data = None
        if tmdb_data and tmdb_review_confidence(tmdb_data, detail) != "low":
            top_billed_cast = tmdb_data.get("top_billed_cast", []) or []
        related = rank_movie_detail_related_entries(detail, films, limit=24)
        related_title_parts = []
        if detail.get("director_entries"):
            director_names = [
                str(director.get("name") or "").strip()
                for director in detail.get("director_entries", [])
                if str(director.get("name") or "").strip()
            ]
            if director_names:
                related_title_parts.append("Director: " + ", ".join(director_names[:2]))
        if detail.get("genre_entries"):
            genre_names = [
                str(genre.get("name") or "").strip()
                for genre in detail.get("genre_entries", [])
                if str(genre.get("name") or "").strip()
            ]
            if genre_names:
                related_title_parts.append("Genre: " + ", ".join(genre_names[:2]))
        if detail.get("source"):
            related_title_parts.append(f"Source: {detail.get('source')}")
        if detail.get("status"):
            related_title_parts.append(f"Status: {detail.get('status')}")
        return {
            "entry": detail,
            "entry_type": "film",
            "player_video_id": detail.get("video_id", ""),
            "tmdb_data": tmdb_data,
            "top_billed_cast": top_billed_cast,
            "related_entries": related,
            "related_title": " / ".join(related_title_parts) or detail.get("category") or "Related Films"
        }

    youtube_entries = collect_all_youtube_entries()
    detail = next((video for video in youtube_entries if video["entry_id"] == entry_id), None)
    if not detail and entry_id.startswith("yt-"):
        fallback_id = entry_id[3:]
        detail = next((video for video in youtube_entries if video.get("video_id") == fallback_id), None)
    if not detail:
        pockettube_context = _resolve_pockettube_group_detail_context(entry_id)
        if pockettube_context:
            return pockettube_context
    if not detail:
        return None
    playlist_entries = [
        video for video in youtube_entries
        if video.get("playlist_name") == detail.get("playlist_name")
    ]
    current_index = next((index for index, video in enumerate(playlist_entries) if video["entry_id"] == entry_id), 0)
    prev_entry = playlist_entries[current_index - 1] if current_index > 0 else None
    next_entry = playlist_entries[current_index + 1] if current_index < len(playlist_entries) - 1 else None
    related = [video for video in playlist_entries if video["entry_id"] != entry_id]
    return {
        "entry": detail,
        "entry_type": "youtube",
        "player_video_id": detail.get("video_id", ""),
        "related_entries": related,
        "related_title": detail.get("playlist_name") or "Related Playlist",
        "playlist_entries": playlist_entries,
        "prev_entry": prev_entry,
        "next_entry": next_entry
    }


def get_director_detail_context(director_page_id):
    catalog = build_director_catalog()
    director_record = catalog.get("records_by_page_id", {}).get(director_page_id)
    if not director_record:
        return None
    director_data = dict(director_record)
    director_data["aliases"] = list(director_record.get("aliases", []) or [])
    director_data["alias_count"] = max(len(director_data["aliases"]) - 1, 0)
    director_tmdb = None
    try:
        director_tmdb = fetch_tmdb_person_profile(
            director_data.get("display_name", ""),
            tmdb_person_id=director_data.get("tmdb_person_id")
        )
    except Exception:
        director_tmdb = None
    biography = str((director_tmdb or {}).get("biography") or "").strip()
    if biography:
        biography = biography[:800].rsplit(" ", 1)[0].strip() + "..." if len(biography) > 800 else biography
    director_data["biography"] = biography
    films = [build_film_entry(film) for film in fetch_library_films_for_flagged_paths()]
    ranked = rank_director_page_films(director_record, films)
    related_films = ranked["films"]
    watched_films = [film for film in related_films if film.get("director_status_rank") == 1]
    want_films = [film for film in related_films if film.get("director_status_rank") == 0]
    other_films = [film for film in related_films if film not in watched_films and film not in want_films]
    director_data["filmography_summary"] = ranked["summary"]
    director_data["filmography_source_summary"] = ranked["source_summary"]
    director_data["filmography_match_summary"] = "Relation {0} · Name {1} · Alias {2}".format(
        ranked["matched_relation_count"],
        ranked["matched_name_count"],
        ranked["matched_alias_count"],
    )
    return {
        "director": director_data,
        "films": related_films,
        "watched_films": watched_films,
        "want_films": want_films,
        "other_films": other_films,
    }


GENRE_STATUS_FILTER_OPTIONS = ["All", "Watched / Finished", "I Want To"]
GENRE_CATEGORY_FILTER_OPTIONS = ["All", "movie", "tv show", "anime", "short movie"]
GENRE_SORT_OPTIONS = ["Score â†“", "Score â†‘", "Title Aâ€“Z", "Title Zâ€“A", "Year â†“", "Year â†‘"]


def genre_page_matches_status(film, status_filter):
    normalized_filter = re.sub(r"[^a-z]+", "", str(status_filter or "").strip().lower())
    if normalized_filter in {"", "all"}:
        return True
    if normalized_filter in {"watchedfinished", "watched"}:
        return is_watched_film_status(film)
    if normalized_filter in {"iwantto", "wantto"}:
        return normalize_movie_status(film.get("status", "")) in PRIMARY_WATCH_NEXT_STATUSES
    return True


def genre_page_matches_category(film, category_filter):
    normalized_filter = normalize_movie_category(category_filter)
    if not normalized_filter or normalized_filter == "all":
        return True
    return normalize_movie_category(film.get("category", "")) == normalized_filter


def genre_sort_key_year(film):
    year_value = film.get("year")
    if isinstance(year_value, (int, float)):
        return int(year_value)
    year_text = str(year_value or "").strip()
    return int(year_text) if year_text.isdigit() else -1


def sort_genre_films(films, sort_value):
    ordered = list(films or [])
    if sort_value == "Score â†“":
        ordered.sort(key=lambda film: (-film.get("score_num", 0), film.get("name", "").lower()))
    elif sort_value == "Score â†‘":
        ordered.sort(key=lambda film: (film.get("score_num", 0), film.get("name", "").lower()))
    elif sort_value == "Title Aâ€“Z":
        ordered.sort(key=lambda film: film.get("name", "").lower())
    elif sort_value == "Title Zâ€“A":
        ordered.sort(key=lambda film: film.get("name", "").lower(), reverse=True)
    elif sort_value == "Year â†“":
        ordered.sort(key=lambda film: (genre_sort_key_year(film), film.get("name", "").lower()), reverse=True)
    elif sort_value == "Year â†‘":
        ordered.sort(key=lambda film: (genre_sort_key_year(film), film.get("name", "").lower()))
    return ordered


def get_genre_detail_context(genre_page_id, filters=None):
    catalog = build_genre_catalog()
    genre_record = catalog.get("records_by_page_id", {}).get(genre_page_id)
    if not genre_record:
        return None
    films = [build_film_entry(film) for film in fetch_all_films()]
    genre_counts = {}
    for film in films:
        for relation_id in list(film.get("genre_relation_ids", []) or []):
            genre_counts[relation_id] = genre_counts.get(relation_id, 0) + 1
    genre_options = []
    for record in sorted(catalog.get("records", []), key=lambda item: (item.get("display_name", "").lower(), item.get("page_id", ""))):
        page_id = record.get("page_id", "")
        if not page_id:
            continue
        genre_options.append({
            "page_id": page_id,
            "name": record.get("display_name", ""),
            "count": genre_counts.get(page_id, 0),
        })
    related_films = [
        film for film in films
        if genre_page_id in list(film.get("genre_relation_ids", []) or [])
    ]
    watched_films = [film for film in related_films if is_watched_film_status(film)]
    want_films = [
        film for film in related_films
        if normalize_movie_status(film.get("status", "")) in PRIMARY_WATCH_NEXT_STATUSES
    ]
    filters = filters or {}
    status_filter = filters.get("status") if filters.get("status") in GENRE_STATUS_FILTER_OPTIONS else "All"
    category_filter = filters.get("category") if filters.get("category") in GENRE_CATEGORY_FILTER_OPTIONS else "All"
    sort_filter = filters.get("sort") if filters.get("sort") in GENRE_SORT_OPTIONS else "Score â†“"
    filtered_films = [
        film for film in related_films
        if genre_page_matches_status(film, status_filter) and genre_page_matches_category(film, category_filter)
    ]
    filtered_films = sort_genre_films(filtered_films, sort_filter)
    filtered_watched_films = [film for film in filtered_films if is_watched_film_status(film)]
    filtered_want_films = [
        film for film in filtered_films
        if normalize_movie_status(film.get("status", "")) in PRIMARY_WATCH_NEXT_STATUSES
    ]
    other_films = [
        film for film in filtered_films
        if film not in filtered_watched_films and film not in filtered_want_films
    ]
    return {
        "genre": genre_record,
        "films": filtered_films,
        "all_linked_films": related_films,
        "watched_films": watched_films,
        "want_films": want_films,
        "filtered_watched_films": filtered_watched_films,
        "filtered_want_films": filtered_want_films,
        "other_films": other_films,
        "current_filters": {
            "genre_page_id": genre_page_id,
            "status": status_filter,
            "category": category_filter,
            "sort": sort_filter,
        },
        "genre_options": genre_options,
        "status_filters": GENRE_STATUS_FILTER_OPTIONS,
        "category_filters": GENRE_CATEGORY_FILTER_OPTIONS,
        "sort_filters": GENRE_SORT_OPTIONS,
    }


def build_film_library_snapshot(limit=200):
    films = fetch_all_films()
    library_lines = []
    category_counts = {}
    status_counts = {}
    for film in films[:limit]:
        category = film.get("category") or "Uncategorized"
        status = film.get("status") or "Unknown"
        score = film.get("score") or "Unrated"
        category_counts[category] = category_counts.get(category, 0) + 1
        status_counts[status] = status_counts.get(status, 0) + 1
        parts = [film.get("name") or "Untitled", f"category: {category}", f"status: {status}", f"score: {score}"]
        if film.get("watch_date"):
            parts.append(f"watching history: {film['watch_date']}")
        if film.get("finish_date"):
            parts.append(f"finishing history: {film['finish_date']}")
        if film.get("rewatch"):
            parts.append(f"rewatch: {film['rewatch']}")
        library_lines.append(" - " + " | ".join(parts))
    category_summary = ", ".join(f"{name} ({count})" for name, count in sorted(category_counts.items()))
    status_summary = ", ".join(f"{name} ({count})" for name, count in sorted(status_counts.items()))
    return {
        "count": len(films),
        "category_summary": category_summary or "No category data",
        "status_summary": status_summary or "No status data",
        "entries": "\n".join(library_lines) if library_lines else "No films were found in the library."
    }


def is_watched_film_status(film):
    normalized = normalize_movie_status(film.get("status", ""))
    if normalized in WATCHED_STATUS_ALIASES:
        return True
    return bool(film.get("finish_date"))


def build_unseen_movie_snapshot(limit=200):
    curated = build_movie_curation_candidates(use_case="watch_next", limit=None)
    eligible = curated["items"]
    category_counts = {}
    status_counts = {}
    for film in eligible:
        category = film.get("category") or "Uncategorized"
        status = film.get("status") or "Unknown"
        category_counts[category] = category_counts.get(category, 0) + 1
        status_counts[status] = status_counts.get(status, 0) + 1
    library_lines = []
    for film in eligible[:limit]:
        parts = [
            film.get("name") or "Untitled",
            f"category: {film.get('category') or 'Uncategorized'}",
            f"status: {film.get('status') or 'Unknown'}",
            f"score: {film.get('score') or 'Unrated'}",
            f"pool: {film.get('pool') or 'secondary'}",
            f"curation score: {film.get('curation_score', 0)}",
        ]
        if film.get("watch_date"):
            parts.append(f"watching history: {film['watch_date']}")
        if film.get("rewatch"):
            parts.append(f"rewatch: {film['rewatch']}")
        if film.get("curation_reasons"):
            parts.append("why: " + "; ".join(film["curation_reasons"][:2]))
        library_lines.append(" - " + " | ".join(parts))
    category_summary = ", ".join(f"{name} ({count})" for name, count in sorted(category_counts.items()))
    status_summary = ", ".join(f"{name} ({count})" for name, count in sorted(status_counts.items()))
    return {
        "count": curated["summary"]["total_candidates"],
        "category_summary": category_summary or "No eligible category data",
        "status_summary": status_summary or "No eligible status data",
        "entries": "\n".join(library_lines) if library_lines else "No eligible unseen titles were found in the library."
    }


def build_watched_movie_snapshot(limit=250):
    films = fetch_all_films()
    watched = []
    for film in films:
        if is_watched_film_status(film):
            watched.append(film)
    watched_lines = []
    for film in watched[:limit]:
        watched_lines.append(
            " - "
            + " | ".join([
                film.get("name") or "Untitled",
                f"rating: {film.get('score') or 'Unrated'}",
                f"category: {film.get('category') or 'Uncategorized'}",
                f"status: {film.get('status') or 'Unknown'}",
            ])
        )
    return {
        "count": len(watched),
        "entries": "\n".join(watched_lines) if watched_lines else "No watched movies were found."
    }


def chat_history_category(chat_mode):
    mapping = {
        "cinematic": "analysis",
        "curation": "curation",
        "german": "german",
    }
    return mapping.get((chat_mode or "").strip().lower(), "")


def build_recent_chat_context(category, limit=12):
    messages = get_recent_messages(category, limit=limit)
    if not messages:
        return "No prior saved messages."
    lines = []
    for item in messages:
        lines.append(f"{item['role']}: {item['message']}")
    return "\n".join(lines)


def normalize_movie_name(value):
    return re.sub(r"\s+", " ", str(value or "").strip()).lower()


def find_film_by_name(movie_name, films=None):
    pool = films if films is not None else fetch_all_films()
    wanted = normalize_movie_name(movie_name)
    if not wanted:
        return None
    exact = next((film for film in pool if normalize_movie_name(film.get("name")) == wanted), None)
    if exact:
        return exact
    return next((film for film in pool if wanted in normalize_movie_name(film.get("name"))), None)


def parse_film_discussion_command(user_message):
    raw = str(user_message or "").strip()
    upper = raw.upper()
    commands = ("DISCUSS:", "AGREE", "DISAGREE", "GO DEEPER", "NEXT TOPIC", "COMPARE:", "RATE IT")
    matched = next((command for command in commands if upper.startswith(command)), None)
    if not matched:
        return {"command": "FREEFORM", "movie_name": "", "reaction": "", "raw": raw}
    if matched == "DISCUSS:":
        body = raw[len("DISCUSS:"):].strip()
        movie_name, reaction = body, ""
        if "|" in body:
            movie_name, reaction = body.split("|", 1)
            movie_name = movie_name.strip()
            reaction = reaction.strip()
        return {"command": "DISCUSS", "movie_name": movie_name, "reaction": reaction, "raw": raw}
    if matched == "COMPARE:":
        return {"command": "COMPARE", "movie_name": raw[len("COMPARE:"):].strip(), "reaction": "", "raw": raw}
    return {"command": matched, "movie_name": "", "reaction": "", "raw": raw}


def select_comparison_films(main_film=None, compare_film=None, limit=12):
    watched = [film for film in fetch_all_films() if is_watched_film_status(film)]
    selected = []
    seen = set()

    def push(film):
        if not film:
            return
        name = film.get("name", "")
        if not name or name in seen:
            return
        seen.add(name)
        selected.append(film)

    push(main_film)
    push(compare_film)
    if main_film and main_film.get("category"):
        for film in watched:
            if film.get("category") == main_film.get("category"):
                push(film)
            if len(selected) >= limit:
                break
    if len(selected) < limit:
        high_priority = ["god mode", "close to god mode", "masterpiece", "Sweet", "good"]
        ordered = sorted(
            watched,
            key=lambda film: (high_priority.index(film.get("score")) if film.get("score") in high_priority else len(high_priority), film.get("name") or "")
        )
        for film in ordered:
            push(film)
            if len(selected) >= limit:
                break
    return selected[:limit]


def build_film_discussion_system_prompt():
    watched_snapshot = build_watched_movie_snapshot(limit=250)
    return FILM_DISCUSSION_PROMPT + "\n\n" + watched_snapshot["entries"]


def build_film_discussion_user_prompt(user_message, state):
    parsed = parse_film_discussion_command(user_message)
    films = fetch_all_films()
    discussion_state = dict(state or {})
    discussion_state.setdefault("current_movie", "")
    discussion_state.setdefault("current_compare", "")
    discussion_state.setdefault("last_command", "")

    if parsed["command"] == "DISCUSS":
        target = find_film_by_name(parsed["movie_name"], films)
        if not target:
            raise RuntimeError(f'Could not find "{parsed["movie_name"]}" in your movie library.')
        discussion_state["current_movie"] = target.get("name", "")
        discussion_state["current_compare"] = ""
        discussion_state["last_command"] = "DISCUSS"
        main_film = target
        compare_film = None
    else:
        main_film = find_film_by_name(discussion_state.get("current_movie", ""), films) if discussion_state.get("current_movie") else None
        compare_film = find_film_by_name(discussion_state.get("current_compare", ""), films) if discussion_state.get("current_compare") else None
        if parsed["command"] in ("AGREE", "DISAGREE", "GO DEEPER", "NEXT TOPIC", "RATE IT", "COMPARE", "FREEFORM") and not main_film:
            raise RuntimeError('Start with "DISCUSS: movie name" before using Film Analysis commands.')
        if parsed["command"] == "COMPARE":
            compare_film = find_film_by_name(parsed["movie_name"], films)
            if not compare_film:
                raise RuntimeError(f'Could not find "{parsed["movie_name"]}" for comparison.')
            discussion_state["current_compare"] = compare_film.get("name", "")
            discussion_state["last_command"] = "COMPARE"
        elif parsed["command"] != "FREEFORM":
            discussion_state["last_command"] = parsed["command"]

    comparison_films = select_comparison_films(main_film=main_film, compare_film=compare_film, limit=12)
    comparison_lines = []
    for film in comparison_films:
        comparison_lines.append(
            " - "
            + " | ".join([
                film.get("name") or "Untitled",
                f"rating: {film.get('score') or 'Unrated'}",
                f"category: {film.get('category') or 'Uncategorized'}",
                f"status: {film.get('status') or 'Unknown'}",
            ])
        )

    main_context = "None selected yet."
    if main_film:
        main_context = " | ".join([
            main_film.get("name") or "Untitled",
            f"rating: {main_film.get('score') or 'Unrated'}",
            f"category: {main_film.get('category') or 'Uncategorized'}",
            f"status: {main_film.get('status') or 'Unknown'}",
        ])
    compare_context = "None."
    if compare_film:
        compare_context = " | ".join([
            compare_film.get("name") or "Untitled",
            f"rating: {compare_film.get('score') or 'Unrated'}",
            f"category: {compare_film.get('category') or 'Uncategorized'}",
            f"status: {compare_film.get('status') or 'Unknown'}",
        ])

    prompt = (
        "FILM ANALYSIS DISCUSSION STATE\n"
        f"Current command: {parsed['command']}\n"
        f"Current movie: {discussion_state.get('current_movie') or 'None'}\n"
        f"Comparison movie: {discussion_state.get('current_compare') or 'None'}\n"
        f"Previous command: {discussion_state.get('last_command') or 'None'}\n\n"
        "MAIN DISCUSSION CONTEXT\n"
        f"{main_context}\n\n"
        "COMPARISON CONTEXT\n"
        f"{compare_context}\n\n"
        "WATCHED FILMS TO DRAW FROM FOR PERSONAL COMPARISON\n"
        f"{chr(10).join(comparison_lines) if comparison_lines else 'No comparison films available.'}\n\n"
        "USER MESSAGE\n"
        f"{user_message}\n"
    )
    if parsed.get("reaction"):
        prompt += f"\nINITIAL REACTION\n{parsed['reaction']}\n"
    prompt += (
        "\nRemember: do not summarize plot, always connect to watched films, keep Arabic + English format, and ask only one question."
    )
    return prompt, discussion_state


def get_gemini_system_instruction(chat_mode="cinematic"):
    mode = (chat_mode or "cinematic").strip().lower()
    if mode == "german":
        return (
            "You are the dedicated German Tutor inside Cinema Prive. "
            "Your role is only beginner German learning support for A1 and A2 level content, especially YouTube lessons, basic grammar, vocabulary, sentence structure, and correction of simple German sentences. "
            "Use simple German first. Keep sentences short, clear, and educational. "
            "When needed, add brief support in English, French, or Moroccan Darija only to make the lesson easier. "
            "Explain grammar practically: articles, cases, verb position, word order, common connectors, pronunciation hints, and everyday vocabulary. "
            "If the user gives a German sentence, correct it gently, show the better version, and explain the rule in a simple way. "
            "Do not do film analysis, symbolic interpretation, psychology, or advanced cinematic discussion. "
            "Do not become abstract or academically heavy unless the user asks for formal German phrasing, and even then stay teacherly and accessible. "
            "If the topic looks like a movie discussion, redirect the user to the Film Analysis tab instead of mixing the two roles."
        )
    if mode == "study":
        return (
            "You are the dedicated YouTube Study assistant inside Cinema Prive. "
            "Your role is to help the user study educational or learning-oriented videos, especially YouTube lessons, Watch Later study items, and practical learning content. "
            "Focus on extracting study goals, lesson structure, key ideas, action steps, terminology, and what to pay attention to next. "
            "Be clear, practical, and concise. "
            "Do not switch into movie recommendation, film symbolism, or beginner German tutoring unless the content is explicitly German-learning material, in which case keep the explanation study-focused."
        )
    if mode == "curation":
        return (
            "You are the dedicated Movie Curation assistant inside Cinema Prive. "
            "Your role is to recommend movies only from the user's own library, using only the eligible unseen titles provided by backend filtering. "
            "Never recommend titles marked as finished, watched, completed, seen, or otherwise already consumed. "
            "Base recommendations on the eligible pool only, and be honest if the pool is small or narrow. "
            "Use the user's categories, scores, statuses, and patterns to explain why certain unseen titles are promising next choices. "
            "Do not drift into German tutoring, YouTube lesson study, or abstract film analysis that ignores the recommendation task."
        )
    if mode == "cinematic":
        return build_film_discussion_system_prompt()
    return (
        "You are the dedicated Film Analysis assistant for Cinema Prive. "
        "Your focus is auteur cinema, especially Ingmar Bergman, and chess. "
        "Write with the seriousness of a great film critic: psychologically precise, elegant, and deeply attentive to inner conflict, memory, guilt, silence, spiritual crisis, and moral ambiguity. "
        "Use the user's library of films as the main canon for recommendations, comparisons, and thematic analysis. "
        "When helpful, connect cinematic ideas to chess concepts such as tension, sacrifice, tempo, positional pressure, and endgame psychology. "
        "Ground recommendations in titles, categories, scores, statuses, and viewing patterns present in the provided library snapshot. "
        "Never invent films that are not in the given library context unless the user explicitly asks for outside recommendations. "
        "Do not switch into beginner German tutoring, A1/A2 grammar coaching, or vocabulary-teacher mode. "
        "If the user asks for beginner German learning help, tell them to use the German Tutor tab."
    )


def get_gemini_model(chat_mode="cinematic"):
    global GEMINI_MODEL_NAME_CACHE
    if genai is None:
        raise RuntimeError("google-generativeai is not installed. Install it to enable Gemini chat.")
    api_key = (GEMINI_API_KEY or "").strip()
    if not api_key:
        raise RuntimeError("Missing GEMINI_API_KEY. Add it to your .env or environment.")
    genai.configure(api_key=api_key)
    if not GEMINI_MODEL_NAME_CACHE:
        preferred_models = [
            "models/gemini-1.5-flash",
            "models/gemini-flash-latest",
            "models/gemini-2.0-flash",
            "models/gemini-2.5-flash",
        ]
        available_models = {
            model.name
            for model in genai.list_models()
            if "generateContent" in (getattr(model, "supported_generation_methods", []) or [])
        }
        GEMINI_MODEL_NAME_CACHE = next((name for name in preferred_models if name in available_models), None)
    if not GEMINI_MODEL_NAME_CACHE:
        raise RuntimeError("No supported Gemini Flash model is available for this API key.")
    system_instruction = get_gemini_system_instruction(chat_mode)
    return genai.GenerativeModel(GEMINI_MODEL_NAME_CACHE, system_instruction=system_instruction)


def extract_gemini_text(response):
    text = getattr(response, "text", "")
    if text:
        return text.strip()
    candidates = getattr(response, "candidates", []) or []
    for candidate in candidates:
        content = getattr(candidate, "content", None)
        parts = getattr(content, "parts", []) or []
        chunks = []
        for part in parts:
            chunk = getattr(part, "text", "")
            if chunk:
                chunks.append(chunk)
        if chunks:
            return "\n".join(chunks).strip()
    return ""


def generate_gemini_reply(user_message, chat_mode="cinematic", state=None):
    mode = (chat_mode or "cinematic").strip().lower()
    model = get_gemini_model(mode)
    recent_context = build_recent_chat_context(chat_history_category(mode), limit=12)
    if mode == "german":
        prompt = (
            "German Tutor mode:\n"
            "This mode is only for German learning, especially A1/A2 level, YouTube lessons, grammar explanation, vocabulary, and simple sentence correction.\n"
            "Recent saved conversation context:\n"
            f"{recent_context}\n\n"
            "Keep the explanation simple, practical, and teacher-like.\n\n"
            "User request:\n"
            f"{user_message}\n\n"
            "Respond like a dedicated German tutor and do not switch into film analysis."
        )
        next_state = {}
    elif mode == "study":
        prompt = (
            "YouTube Study mode:\n"
            "This mode is for educational or learning-oriented videos, lessons, study queues, and practical video-based learning.\n"
            "Recent saved conversation context:\n"
            f"{recent_context}\n\n"
            "User request:\n"
            f"{user_message}\n\n"
            "Respond like a study partner who helps the user understand what to learn from the selected video."
        )
        next_state = {}
    elif mode == "curation":
        snapshot = build_unseen_movie_snapshot(limit=200)
        prompt = (
            "Movie Curation mode:\n"
            "The backend already filtered the movie library to only eligible unseen titles. Titles marked as watched or finished are excluded.\n"
            "Recent saved conversation context:\n"
            f"{recent_context}\n\n"
            f"Eligible unseen titles: {snapshot['count']}\n"
            f"Category spread: {snapshot['category_summary']}\n"
            f"Status spread: {snapshot['status_summary']}\n"
            "Eligible entries:\n"
            f"{snapshot['entries']}\n\n"
            "User request:\n"
            f"{user_message}\n\n"
            "Recommend only from this eligible unseen pool. If the pool is small, say so clearly."
        )
        next_state = {}
    else:
        prompt, next_state = build_film_discussion_user_prompt(user_message, state or {})
        prompt += "\n\nRECENT SAVED DISCUSSION CONTEXT\n" + recent_context
    response = model.generate_content(prompt)
    reply = extract_gemini_text(response)
    if not reply:
        raise RuntimeError("Gemini returned an empty response.")
    return reply, next_state

def build_notebook_proxy_preview(reason="NotebookLM could not be rendered directly inside this panel."):
    launch_url = NOTEBOOKLM_URL
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>NotebookLM Preview</title>
    <style>
        :root {{
            --bg: #0d0d0d;
            --panel: rgba(18, 18, 18, 0.96);
            --border: rgba(106, 192, 69, 0.28);
            --accent: #6ac045;
            --text: #ede8df;
            --muted: #8a847a;
        }}
        * {{ box-sizing: border-box; }}
        body {{
            margin: 0;
            min-height: 100vh;
            font-family: Inter, Arial, sans-serif;
            color: var(--text);
            background:
                radial-gradient(circle at top, rgba(106, 192, 69, 0.1), transparent 28%),
                linear-gradient(180deg, #0d0d0d, #090909);
            display: flex;
            align-items: center;
            justify-content: center;
            padding: 24px;
        }}
        .preview {{
            width: min(100%, 680px);
            border: 1px solid var(--border);
            border-radius: 24px;
            background: var(--panel);
            box-shadow: 0 24px 60px rgba(0, 0, 0, 0.38);
            overflow: hidden;
        }}
        .hero {{
            padding: 28px;
            border-bottom: 1px solid rgba(106, 192, 69, 0.16);
        }}
        .badge {{
            display: inline-block;
            padding: 6px 10px;
            border: 1px solid rgba(106, 192, 69, 0.35);
            border-radius: 999px;
            color: var(--accent);
            font-size: 12px;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            margin-bottom: 16px;
        }}
        h1 {{
            margin: 0 0 10px;
            font-size: 30px;
            font-weight: 600;
        }}
        p {{
            margin: 0;
            color: var(--muted);
            line-height: 1.7;
            font-size: 14px;
        }}
        .browser {{
            padding: 20px 28px 28px;
        }}
        .browser-bar {{
            display: flex;
            align-items: center;
            gap: 8px;
            margin-bottom: 18px;
        }}
        .dot {{
            width: 10px;
            height: 10px;
            border-radius: 50%;
            background: rgba(255, 255, 255, 0.16);
        }}
        .url {{
            flex: 1;
            padding: 10px 14px;
            border-radius: 12px;
            border: 1px solid rgba(106, 192, 69, 0.18);
            background: rgba(255, 255, 255, 0.03);
            color: var(--muted);
            font-size: 13px;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }}
        .viewport {{
            border: 1px solid rgba(106, 192, 69, 0.16);
            border-radius: 18px;
            min-height: 280px;
            background:
                linear-gradient(180deg, rgba(255, 255, 255, 0.03), rgba(255, 255, 255, 0)),
                rgba(255, 255, 255, 0.01);
            padding: 24px;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
            gap: 18px;
        }}
        .lines {{
            display: grid;
            gap: 12px;
        }}
        .line {{
            height: 12px;
            border-radius: 999px;
            background: linear-gradient(90deg, rgba(106, 192, 69, 0.18), rgba(255, 255, 255, 0.03));
        }}
        .line:nth-child(1) {{ width: 76%; }}
        .line:nth-child(2) {{ width: 92%; }}
        .line:nth-child(3) {{ width: 64%; }}
        .line:nth-child(4) {{ width: 85%; }}
        .cta {{
            display: inline-block;
            align-self: flex-start;
            text-decoration: none;
            border-radius: 14px;
            border: 1px solid rgba(106, 192, 69, 0.9);
            background: linear-gradient(180deg, rgba(106, 192, 69, 0.22), rgba(106, 192, 69, 0.08));
            color: #eaffdf;
            padding: 14px 18px;
            font-size: 13px;
            font-weight: 600;
            letter-spacing: 0.08em;
            text-transform: uppercase;
        }}
        .footer {{
            margin-top: 14px;
            color: #6a6460;
            font-size: 12px;
            line-height: 1.6;
        }}
    </style>
</head>
<body>
    <div class="preview">
        <div class="hero">
            <div class="badge">Pinned Browser Tab</div>
            <h1>NotebookLM Preview</h1>
            <p>{reason}</p>
        </div>
        <div class="browser">
            <div class="browser-bar">
                <span class="dot"></span>
                <span class="dot"></span>
                <span class="dot"></span>
                <div class="url">{launch_url}</div>
            </div>
            <div class="viewport">
                <div class="lines">
                    <div class="line"></div>
                    <div class="line"></div>
                    <div class="line"></div>
                    <div class="line"></div>
                </div>
                <a class="cta" href="{launch_url}" target="_blank" rel="noopener noreferrer">Open Research Workspace</a>
            </div>
            <div class="footer">
                This preview stays inside the panel for context, while the full authenticated NotebookLM experience opens in its own browser tab when Google blocks proxy rendering.
            </div>
        </div>
    </div>
</body>
</html>"""

def prepare_notebook_proxy_html(raw_html):
    base_tag = f'<base href="{NOTEBOOKLM_URL}">'
    if "<head>" in raw_html:
        return raw_html.replace("<head>", f"<head>{base_tag}", 1)
    return base_tag + raw_html


def dragon_auth_enabled():
    return bool(DRAGON_ADMIN_USERNAME and DRAGON_ADMIN_PASSWORD)


def dragon_site_protection_enabled():
    return dragon_auth_enabled() and DRAGON_PROTECT_WHOLE_SITE


def dragon_is_authenticated():
    return bool(session.get("dragon_authenticated"))


def dragon_is_safe_next_url(value):
    candidate = str(value or "").strip()
    if not candidate.startswith("/") or candidate.startswith("//"):
        return False
    parsed = urllib.parse.urlparse(candidate)
    return not parsed.scheme and not parsed.netloc


def dragon_next_url(default_endpoint="home"):
    candidate = request.values.get("next") or request.args.get("next") or ""
    return candidate if dragon_is_safe_next_url(candidate) else url_for(default_endpoint)


def dragon_login_required_for_request():
    if not dragon_auth_enabled():
        return False
    endpoint = str(request.endpoint or "")
    if endpoint in {"static", "login", "logout", "healthz"}:
        return False
    path = str(request.path or "/")
    if path.startswith("/static/"):
        return False
    if dragon_site_protection_enabled():
        return True
    return path.startswith("/admin")


@app.before_request
def enforce_dragon_private_access():
    if not dragon_login_required_for_request() or dragon_is_authenticated():
        return None
    next_url = request.full_path if request.query_string else request.path
    if request.path.startswith("/api/") or request.method != "GET":
        return jsonify({"ok": False, "error": "Authentication required.", "login_url": url_for("login", next=next_url)}), 401
    return redirect(url_for("login", next=next_url))

# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  ROUTES
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
@app.route("/healthz")
def healthz():
    return jsonify({"ok": True})


@app.route("/login", methods=["GET", "POST"])
def login():
    if not dragon_auth_enabled():
        return redirect(url_for("home"))
    if dragon_is_authenticated():
        return redirect(dragon_next_url())
    error_message = ""
    next_url = dragon_next_url()
    if request.method == "POST":
        username = str(request.form.get("username", "") or "").strip()
        password = str(request.form.get("password", "") or "")
        if username == DRAGON_ADMIN_USERNAME and password == DRAGON_ADMIN_PASSWORD:
            session["dragon_authenticated"] = True
            session.permanent = True
            return redirect(next_url)
        error_message = "Wrong username or password."
    return render_template("login.html", next_url=next_url, error_message=error_message)


@app.route("/logout")
def logout():
    session.pop("dragon_authenticated", None)
    next_url = dragon_next_url(default_endpoint="login")
    return redirect(next_url)


@app.route("/refresh")
def refresh():
    scope = (request.args.get("scope") or "all").strip().lower()
    refresh_films = scope in {"all", "films", "movies", "movie"}
    refresh_youtube = scope in {"all", "youtube", "playlists"}
    if refresh_films:
        clear_film_cache_entry()
    if refresh_films and (TMDB_API_KEY or "").strip() and (NOTION_TOKEN or "").strip() and (NOTION_DATABASE_ID or "").strip():
        try:
            run_tmdb_sync_enrichment()
        except Exception:
            pass
    reset_playlists_metadata()
    refresh_all_cached_data(refresh_films=refresh_films, refresh_youtube=refresh_youtube)
    next_url = request.args.get("next") or url_for("home")
    return redirect(next_url)


def render_section_page(section_name, title=None, quick_delete_enabled=False):
    admin_data = load_admin_data()
    playlists_with_videos, default_limit, section_channel_groups = build_youtube_section_playlists(section_name, admin_data=admin_data)
    section_title = title or section_name
    section_profile = youtube_section_blueprint(section_name)
    section_curation_context = build_youtube_channel_curation_context(section_name, admin_data=admin_data)
    section_feed_context = build_youtube_section_feed_context(section_name, admin_data=admin_data)
    section_profile.update(section_curation_context)
    section_profile["channel_groups"] = section_channel_groups
    ai_context = ai_context_for_section(section_name)
    return render_template(
        "youtube_section.html",
        title=section_title,
        playlists=playlists_with_videos,
        default_limit=default_limit,
        quick_delete_enabled=quick_delete_enabled,
        section_profile=section_profile,
        section_channel_groups=section_channel_groups,
        section_channel_curation=section_curation_context,
        section_feed_context=section_feed_context,
        build_query_url=build_query_url,
        ai_default_mode=ai_context["mode"],
        ai_page_context=ai_context["page_context"]
    )


@app.route("/api/youtube_durations", methods=["POST"])
def youtube_durations():
    payload = request.get_json(silent=True) or {}
    video_ids = payload.get("video_ids", [])
    if not isinstance(video_ids, list):
        return jsonify({"ok": False, "error": "video_ids must be a list"}), 400
    unique_video_ids = [video_id for video_id in dict.fromkeys(video_ids) if video_id]
    fetch_youtube_video_metadata(unique_video_ids)
    return jsonify({
        "ok": True,
        "durations": {
            video_id: get_youtube_duration(video_id)
            for video_id in unique_video_ids
        }
    })


@app.route("/api/movie-curation")
def movie_curation_api():
    category = request.args.get("category", "")
    source = request.args.get("source", "")
    use_case = request.args.get("use_case", "watch_next")
    limit_arg = request.args.get("limit", "12")
    limit = None
    if limit_arg and str(limit_arg).strip().lower() != "all":
        try:
            limit = max(int(limit_arg), 1)
        except (TypeError, ValueError):
            return jsonify({"ok": False, "error": "limit must be a positive integer or All"}), 400
    curated = build_movie_curation_candidates(use_case=use_case, category_filter=category, source_filter=source, limit=limit)
    return jsonify({"ok": True, **curated})


@app.route("/debug/movie-curation")
def movie_curation_debug():
    curated = build_movie_curation_candidates(use_case="watch_next", limit=20)
    source_films = fetch_all_films()
    source_titles = {str(film.get("name") or "").strip() for film in source_films if str(film.get("name") or "").strip()}
    candidates = curated["items"]
    all_from_library = all(str(item.get("name") or "").strip() in source_titles for item in candidates)
    watched_in_results = [item.get("name", "Untitled") for item in candidates if is_watched_film_status(item)]

    debug_rows = []
    for item in candidates:
        debug_rows.append({
            "title": item.get("name", "Untitled"),
            "category": item.get("category", ""),
            "status": item.get("status", ""),
            "score": item.get("score", ""),
            "reason": "; ".join(item.get("curation_reasons", [])[:2]) or "Eligible unseen candidate",
        })

    return jsonify({
        "ok": True,
        "use_case": "watch_next",
        "source_of_truth": "live_notion_library_via_fetch_all_films",
        "top_candidate_count": len(debug_rows),
        "all_candidates_from_own_library": all_from_library,
        "watched_finished_titles_excluded": not bool(watched_in_results),
        "watched_finished_titles_found_in_results": watched_in_results,
        "summary": curated.get("summary", {}),
        "candidates": debug_rows,
    })


@app.route("/chat", methods=["POST"])
def chat():
    payload = request.get_json(silent=True) or {}
    user_message = str(payload.get("message", "")).strip()
    display_message = str(payload.get("display_message", "")).strip()
    chat_mode = str(payload.get("mode", "cinematic")).strip().lower()
    chat_state = payload.get("state", {})
    if not isinstance(chat_state, dict):
        chat_state = {}
    if not user_message:
        return jsonify({"ok": False, "error": "Message is required."}), 400
    if chat_mode not in ("cinematic", "german", "curation", "study"):
        return jsonify({"ok": False, "error": "Unsupported chat mode."}), 400
    try:
        reply, next_state = generate_gemini_reply(user_message, chat_mode=chat_mode, state=chat_state)
    except RuntimeError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 503
    except Exception as exc:
        return jsonify({"ok": False, "error": f"Gemini request failed: {exc}"}), 500
    history_bucket = chat_history_category(chat_mode)
    if history_bucket:
        save_message(history_bucket, "user", display_message or user_message)
        save_message(history_bucket, "assistant", reply)
    return jsonify({"ok": True, "reply": reply, "state": next_state})


@app.route("/chat_history")
def chat_history():
    category = chat_history_category(request.args.get("category", ""))
    if not category:
        return jsonify({"ok": False, "error": "Unsupported history category."}), 400
    try:
        limit = max(int(request.args.get("limit", 30)), 1)
    except (TypeError, ValueError):
        limit = 30
    return jsonify({"ok": True, "messages": get_recent_messages(category, limit=limit)})


@app.route("/test-notion")
def test_notion():
    notion_token = config_value("NOTION_TOKEN", "")
    notion_database_id = config_value("NOTION_DATABASE_ID", "")
    if not notion_token:
        return jsonify({"status": "error", "message": "Missing NOTION_TOKEN. Add it to the root .env file or process environment."}), 500
    if not notion_database_id:
        return jsonify({"status": "error", "message": "Missing NOTION_DATABASE_ID. Add it to the root .env file or process environment."}), 500

    url = f"https://api.notion.com/v1/databases/{notion_database_id}/query"
    headers = {
        "Authorization": f"Bearer {notion_token}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json"
    }
    payload = {"page_size": 5}

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=15)
    except requests.RequestException as exc:
        return jsonify({"status": "error", "message": f"Failed to reach Notion: {exc}"}), 502

    if response.status_code != 200:
        try:
            error_payload = response.json()
            error_message = error_payload.get("message") or error_payload.get("code") or response.text
        except ValueError:
            error_message = response.text or f"Notion returned status {response.status_code}."
        return jsonify({"status": "error", "message": error_message}), response.status_code

    try:
        data = response.json()
    except ValueError:
        return jsonify({"status": "error", "message": "Notion returned invalid JSON."}), 502

    movies = []
    for page in data.get("results", []):
        movies.append(extract_notion_page_title(page.get("properties", {})))

    return jsonify({"status": "success", "movies": movies})


@app.route("/sync-tmdb")
def sync_tmdb():
    payload, status_code = run_tmdb_sync_enrichment()
    return jsonify(payload), status_code


@app.route("/directors-migration-preview")
def directors_migration_preview():
    try:
        preview = build_director_migration_preview()
    except requests.RequestException as exc:
        return jsonify({"ok": False, "error": f"Notion request failed: {exc}"}), 502
    return jsonify({"ok": True, **preview})


@app.route("/directors-migration-apply", methods=["POST"])
def directors_migration_apply():
    try:
        result = apply_director_migration()
    except requests.RequestException as exc:
        return jsonify({"ok": False, "error": f"Director migration failed while calling Notion: {exc}"}), 502
    except RuntimeError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500
    return jsonify({"ok": True, **result})


@app.route("/directors-sync-complete", methods=["POST"])
def directors_sync_complete():
    try:
        result = complete_directors_integration()
    except requests.RequestException as exc:
        return jsonify({"ok": False, "error": f"Directors sync failed while calling Notion/TMDb: {exc}"}), 502
    except RuntimeError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500
    return jsonify({"ok": True, **result})


def build_admin_shared_context():
    admin_data = load_admin_data()
    reading_admin = build_reading_admin_context()
    combined_sections = build_combined_sections()
    playlist_rows = build_admin_table_rows(admin_data)
    section_names = [section.get("name", "") for section in combined_sections]
    pockettube_imports = admin_data.get("youtube_pockettube_imports", {"latest": {}, "imports": []})
    pockettube_latest = pockettube_imports.get("latest", {}) if isinstance(pockettube_imports, dict) else {}
    pockettube_import_list = pockettube_imports.get("imports", []) if isinstance(pockettube_imports, dict) else []
    return {
        "admin_data": admin_data,
        "reading_admin": reading_admin,
        "combined_sections": combined_sections,
        "playlist_rows": playlist_rows,
        "section_names": section_names,
        "pockettube_imports": pockettube_imports if isinstance(pockettube_imports, dict) else {"latest": {}, "imports": []},
        "pockettube_latest": pockettube_latest if isinstance(pockettube_latest, dict) else {},
        "pockettube_import_count": len(pockettube_import_list) if isinstance(pockettube_import_list, list) else 0,
    }


def build_admin_hub_context():
    shared = build_admin_shared_context()
    admin_data = shared["admin_data"]
    reading_admin = shared["reading_admin"]
    combined_sections = shared["combined_sections"]
    playlist_rows = shared["playlist_rows"]
    pockettube_import_count = int(shared.get("pockettube_import_count", 0) or 0)
    return {
        **shared,
        "admin_cards": [
            {
                "key": "sections",
                "label": "Sections",
                "title": "Category / Section Management",
                "description": "Add, rename, and prune the dashboard categories that hold playlists.",
                "href": url_for("admin_sections"),
                "meta": [f"{len(combined_sections)} categories"],
                "icon": "folder-tree",
            },
            {
                "key": "playlists",
                "label": "Playlists",
                "title": "Playlist Management",
                "description": "Edit playlist names, URLs, and category placement from one focused page.",
                "href": url_for("admin_playlists"),
                "meta": [f"{len(playlist_rows)} playlists"],
                "icon": "list-video",
            },
            {
                "key": "reading",
                "label": "Reading",
                "title": "Reading RSS Management",
                "description": "Keep RSS sources, sync controls, and source diagnostics in one place.",
                "href": url_for("admin_reading"),
                "meta": [
                    f"{reading_admin['reading_rss_source_count']} RSS sources",
                    f"{reading_admin['reading_rss_active_source_count']} active",
                ],
                "icon": "rss",
            },
            {
                "key": "io",
                "label": "Imports / Exports / Backups",
                "title": "Imports / Exports / Backups",
                "description": "Handle Reading JSON backups, PocketTube imports, and export flows.",
                "href": url_for("admin_io"),
                "meta": [
                    f"{reading_admin['reading_backup_count']} backups",
                    f"{pockettube_import_count} PocketTube imports",
                ],
                "icon": "download",
            },
            {
                "key": "diagnostics",
                "label": "Diagnostics",
                "title": "Diagnostics",
                "description": "Inspect source health, sync notes, and recovery hints without the noise.",
                "href": url_for("admin_diagnostics"),
                "meta": [
                    f"{reading_admin['reading_recovery_hit_count']} recovery hints",
                    f"Last sync {reading_admin['reading_last_sync_at_display']}",
                ],
                "icon": "activity",
            },
        ],
    }


def build_admin_panel_context(panel_key):
    shared = build_admin_shared_context()
    reading_admin = shared["reading_admin"]
    combined_sections = shared["combined_sections"]
    playlist_rows = shared["playlist_rows"]
    if panel_key == "sections":
        title = "Category / Section Management"
        description = "Add, rename, and remove dashboard categories without the rest of Admin in the way."
    elif panel_key == "playlists":
        title = "Playlist Management"
        description = "Edit playlist names, URLs, and category placement from a focused page."
    elif panel_key == "reading":
        title = "Reading RSS Management"
        description = "Manage RSS sources, sync them, and inspect source-level diagnostics."
    elif panel_key == "io":
        title = "Imports / Exports / Backups"
        description = "Handle JSON backups, export flows, and PocketTube import data."
    elif panel_key == "diagnostics":
        title = "Diagnostics"
        description = "Read the source health, sync history, and recovery hints at a glance."
    else:
        title = "Admin"
        description = "Focused admin workspace."
    return {
        **shared,
        "panel_key": panel_key,
        "panel_title": title,
        "panel_description": description,
        "panel_back_url": url_for("admin"),
        "panel_source_count": reading_admin["reading_rss_source_count"],
        "panel_active_source_count": reading_admin["reading_rss_active_source_count"],
        "panel_section_count": len(combined_sections),
        "panel_playlist_count": len(playlist_rows),
    }


def handle_admin_panel_post(next_url):
    admin_data = load_admin_data()
    try:
        success_message = handle_admin_action(admin_data, request.form)
        clear_youtube_runtime_cache()
        return redirect(append_query_param(next_url, success=success_message))
    except ValueError as exc:
        return redirect(append_query_param(next_url, error=str(exc)))


@app.route("/admin", methods=["GET", "POST"])
def admin():
    error_message = ""
    success_message = request.args.get("success", "")
    error_message = request.args.get("error", "") or ""
    if request.method == "POST":
        return handle_admin_panel_post(url_for("admin"))
    hub_context = build_admin_hub_context()
    return render_template(
        "admin.html",
        **hub_context,
        success_message=success_message,
        error_message=error_message,
        ai_default_mode="cinematic",
        ai_page_context="general"
    )


@app.route("/admin/sections", methods=["GET", "POST"])
def admin_sections():
    if request.method == "POST":
        return handle_admin_panel_post(url_for("admin_sections"))
    context = build_admin_panel_context("sections")
    return render_template(
        "admin_section.html",
        **context,
        success_message=request.args.get("success", ""),
        error_message=request.args.get("error", "") or "",
        ai_default_mode="cinematic",
        ai_page_context="general",
    )


@app.route("/admin/playlists", methods=["GET", "POST"])
def admin_playlists():
    if request.method == "POST":
        return handle_admin_panel_post(url_for("admin_playlists"))
    context = build_admin_panel_context("playlists")
    return render_template(
        "admin_section.html",
        **context,
        success_message=request.args.get("success", ""),
        error_message=request.args.get("error", "") or "",
        ai_default_mode="cinematic",
        ai_page_context="general",
    )


@app.route("/admin/reading", methods=["GET", "POST"])
def admin_reading():
    if request.method == "POST":
        return handle_admin_panel_post(url_for("admin_reading"))
    context = build_admin_panel_context("reading")
    return render_template(
        "admin_section.html",
        **context,
        success_message=request.args.get("success", ""),
        error_message=request.args.get("error", "") or "",
        ai_default_mode="cinematic",
        ai_page_context="general",
    )


@app.route("/admin/io", methods=["GET", "POST"])
def admin_io():
    if request.method == "POST":
        return handle_admin_panel_post(url_for("admin_io"))
    context = build_admin_panel_context("io")
    return render_template(
        "admin_section.html",
        **context,
        success_message=request.args.get("success", ""),
        error_message=request.args.get("error", "") or "",
        ai_default_mode="cinematic",
        ai_page_context="general",
    )


@app.route("/admin/diagnostics")
def admin_diagnostics():
    context = build_admin_panel_context("diagnostics")
    return render_template(
        "admin_section.html",
        **context,
        success_message=request.args.get("success", ""),
        error_message=request.args.get("error", "") or "",
        ai_default_mode="cinematic",
        ai_page_context="general",
    )


def _load_pockettube_import_payload_from_request(req):
    if req.is_json:
        payload = req.get_json(silent=True)
        if payload is None:
            raise ValueError("PocketTube JSON payload is required.")
        return payload, "json"
    upload = req.files.get("pockettube_file")
    if upload and getattr(upload, "filename", ""):
        raw_bytes = upload.read()
        if not raw_bytes:
            raise ValueError("Uploaded PocketTube file is empty.")
        try:
            text = raw_bytes.decode("utf-8-sig")
        except UnicodeDecodeError:
            text = raw_bytes.decode("utf-8", errors="replace")
        if not text.strip():
            raise ValueError("Uploaded PocketTube file is empty.")
        return json.loads(text), upload.filename
    text = (req.form.get("pockettube_json", "") or req.form.get("pockettube_payload", "") or "").strip()
    if not text:
        raise ValueError("PocketTube JSON payload or file is required.")
    return json.loads(text), "form"


@app.route("/api/youtube-pockettube-import", methods=["POST"])
def youtube_pockettube_import():
    wants_json = request.is_json or str(request.args.get("format", "") or "").strip().lower() == "json" or "application/json" in (request.headers.get("Accept", "") or "").lower()
    try:
        raw_payload, source_label = _load_pockettube_import_payload_from_request(request)
        import_summary = normalize_pockettube_import_payload(raw_payload)
        admin_data = load_admin_data()
        updated_admin_data = merge_pockettube_import_into_admin_data(admin_data, import_summary)
        response_payload = {
            "ok": True,
            "source": source_label,
            "source_name": import_summary.get("source_name", "PocketTube"),
            "fingerprint": import_summary.get("fingerprint", ""),
            "section_count": import_summary.get("section_count", 0),
            "group_count": import_summary.get("group_count", 0),
            "channel_count": import_summary.get("channel_count", 0),
            "curated_channel_count": len((updated_admin_data.get("youtube_channel_curation", {}) or {}).get("channels", []) or []),
            "pockettube_import_count": len(((updated_admin_data.get("youtube_pockettube_imports", {}) or {}).get("imports", []) or [])),
        }
        if wants_json:
            return jsonify(response_payload)
        success_message = (
            f'PocketTube import applied: {response_payload["channel_count"]} channels across '
            f'{response_payload["group_count"]} groups in {response_payload["section_count"]} sections.'
        )
        return redirect(url_for("admin", success=success_message))
    except Exception as exc:
        if wants_json:
            return jsonify({"ok": False, "error": str(exc)}), 400
        return redirect(url_for("admin", error=str(exc)))


@app.route("/admin/movie-want-to-union-compare")
def admin_movie_want_to_union_compare():
    runs = request.args.get("runs", 2)
    report = build_movie_want_to_union_compare(runs=runs, save_report=True)
    return render_template(
        "admin_movie_want_to_union_compare.html",
        report=report,
        ai_default_mode="cinematic",
        ai_page_context="general"
    )


@app.route("/export-movies-data")
def export_movies_data_route():
    try:
        summary = export_movies_data()
    except Exception as exc:
        next_url = request.args.get("next") or url_for("admin")
        return redirect(f"{next_url}{'&' if '?' in next_url else '?'}error={urllib.parse.quote(str(exc))}")
    next_url = request.args.get("next") or url_for("admin")
    success_message = (
        f"Exported {summary['total_exported']} movies. "
        f"JSON: {summary['json_path']} | CSV: {summary['csv_path']}"
    )
    return redirect(f"{next_url}{'&' if '?' in next_url else '?'}success={urllib.parse.quote(success_message)}")


@app.route("/")
def home():
    films = [build_film_entry(film) for film in fetch_library_films_for_flagged_paths()]
    total = len(films)
    finished = len([f for f in films if f["status"] == "Finished"])
    watching = len([f for f in films if f["status"] == "Watching"])
    want = len(fetch_want_to_films_for_flagged_paths()) if movie_want_to_union_fetch_enabled() else len([f for f in films if f["status"] == "i want to"])
    top = sorted(films, key=lambda x: -x["score_num"])[:5]
    return render_template("index.html", total=total, finished=finished,
                           watching=watching, want=want, top=top,
                           score_display=SCORE_DISPLAY, score_color=SCORE_COLOR,
                           ai_default_mode="cinematic", ai_page_context="general")

@app.route("/library")
def library():
    films = [build_film_entry(film) for film in fetch_library_films_for_flagged_paths()]
    raw_search = request.args.get("search", "")
    search = raw_search.lower()
    raw_genre = str(request.args.get("genre", "") or "").strip()
    genre_page_id = str(request.args.get("genre_id", "") or "").strip()
    raw_director = str(request.args.get("director", "") or "").strip()
    director_page_id = str(request.args.get("director_id", "") or "").strip()
    cat = request.args.get("category", "All")
    if raw_director or director_page_id:
        status_default = "All Status"
    elif raw_genre or genre_page_id:
        status_default = "i want to"
    else:
        status_default = "All Status"
    status = request.args.get("status", status_default)
    source = normalize_source_filter(request.args.get("source", "All sources"))
    score = request.args.get("score", "All Scores")
    sort = request.args.get("sort", "Score â†“")
    filtered = films[:]
    if raw_genre or genre_page_id:
        filtered = [f for f in filtered if film_matches_genre_filter(f, raw_genre, genre_page_id)]
    if raw_director or director_page_id:
        filtered = [f for f in filtered if film_matches_director_filter(f, raw_director, director_page_id)]
    if search: filtered = [f for f in filtered if search in f["name"].lower()]
    if cat != "All": filtered = [f for f in filtered if f["category"] == cat]
    if status != "All Status": filtered = [f for f in filtered if f["status"] == status]
    if source != "All sources": filtered = [f for f in filtered if movie_matches_source_filter(f, source)]
    if score != "All Scores": filtered = [f for f in filtered if f["score"] == score]
    if sort == "Score â†“": filtered.sort(key=lambda x: -x["score_num"])
    elif sort == "Score â†‘": filtered.sort(key=lambda x: x["score_num"])
    elif sort == "Title Aâ€“Z": filtered.sort(key=lambda x: x["name"].lower())
    elif sort == "Title Zâ€“A": filtered.sort(key=lambda x: x["name"].lower(), reverse=True)
    elif sort == "Date Watched â†“": filtered.sort(key=lambda x: x["watch_date"] or "", reverse=True)
    elif sort == "Date Watched â†‘": filtered.sort(key=lambda x: x["watch_date"] or "")
    limit_override = request.args.get("limit")
    per_page = 50
    if limit_override:
        if limit_override == "All":
            per_page = max(len(filtered), 1)
        else:
            try:
                per_page = max(int(limit_override), 1)
            except (TypeError, ValueError):
                per_page = int(request.args.get("per_page", 50))
    else:
        per_page = int(request.args.get("per_page", 50))
    page = int(request.args.get("page", 1))
    pagination = paginate_items(filtered, page, per_page)
    for film in pagination["items"]:
        film["country_display"] = ""
        try:
            tmdb_data = fetch_tmdb_enrichment(
                film.get("name", ""),
                category=film.get("category", ""),
                year=film.get("year", "")
            )
        except Exception:
            tmdb_data = None
        countries = list((tmdb_data or {}).get("origin_countries", []) or [])
        if countries:
            film["country_display"] = format_origin_country_display(countries, limit=2)
    categories = ["All"] + sorted({f["category"] for f in films if f["category"]})
    statuses = ["All Status"] + sorted({f["status"] for f in films if f["status"]})
    sources = SOURCE_FILTER_OPTIONS
    scores = ["All Scores"] + [k for k in SCORE_ORDER if k]
    active_genre_label = resolve_genre_filter_label(films, raw_genre, genre_page_id)
    active_director_label = resolve_director_filter_label(films, raw_director, director_page_id)
    suggestion_titles = []
    seen_suggestion_titles = set()
    for film in films:
        title = str(film.get("name") or "").strip()
        normalized_title = title.lower()
        if not title or normalized_title in seen_suggestion_titles:
            continue
        seen_suggestion_titles.add(normalized_title)
        suggestion_titles.append(title)
    return render_template("library.html", films=pagination["items"], total=pagination["total"], page=pagination["page"],
                           total_pages=pagination["total_pages"], per_page=per_page,
                           pagination_numbers=pagination["pagination"],
                           categories=categories, statuses=statuses, sources=sources, scores=scores,
                           suggestion_titles=suggestion_titles,
                           current_filters={"search": raw_search, "category": cat,
                                            "status": status, "source": source, "score": score, "sort": sort,
                                            "genre": raw_genre, "genre_id": genre_page_id, "genre_label": active_genre_label,
                                            "director": raw_director, "director_id": director_page_id, "director_label": active_director_label},
                           score_display=SCORE_DISPLAY, score_color=SCORE_COLOR, yts_url=yts_url,
                           build_query_url=build_query_url,
                           ai_default_mode="cinematic", ai_page_context="movie")


@app.route("/movies")
def movies():
    return library()


@app.route("/writing")
def legacy_books_archive_redirect():
    return redirect(url_for("books_archive"), code=301)


@app.route("/writing/<entry_id>")
def legacy_books_entry_redirect(entry_id):
    return redirect(url_for("books_entry_detail", entry_id=entry_id), code=301)


@app.route("/books")
def books_archive():
    books_view = build_books_view()
    return render_template(
        "books.html",
        title="Books",
        build_query_url=build_query_url,
        books_text_direction=books_text_direction,
        ai_default_mode="cinematic",
        ai_page_context="general",
        **books_view,
    )


@app.route("/books/<entry_id>")
def books_entry_detail(entry_id):
    fetched = fetch_books_entries()
    entries = list(fetched.get("entries", []) or [])
    normalized_entry_id = compact_notion_id(entry_id)
    entry = next((item for item in entries if item.get("id") == normalized_entry_id), None)
    if not entry:
        return Response("Book entry not found.", status=404)
    quotes_fetched = fetch_book_quotes_for_entry(entry.get("notion_page_id", ""))
    quotes = list(quotes_fetched.get("entries", []) or [])
    return render_template(
        "books_detail.html",
        title=entry.get("title") or "Book",
        entry=entry,
        books_error_message=str(fetched.get("error") or "").strip(),
        book_quotes=quotes,
        book_quotes_count=len(quotes),
        book_quotes_error_message=str(quotes_fetched.get("error") or "").strip(),
        books_return_url=url_for("books_archive"),
        books_text_direction=books_text_direction,
        ai_default_mode="cinematic",
        ai_page_context="general",
    )


def build_books_cover_placeholder_svg(title="", authors=""):
    title_text = re.sub(r"\s+", " ", str(title or "").strip())[:36]
    authors_text = re.sub(r"\s+", " ", str(authors or "").strip())[:48]
    if not title_text:
        title_text = "No cover"
    title_text = escape(title_text)
    authors_text = escape(authors_text)
    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 420 640" role="img" aria-label="{title_text}">
  <defs>
    <linearGradient id="g" x1="0" y1="0" x2="0" y2="1">
      <stop offset="0%" stop-color="#141414"/>
      <stop offset="100%" stop-color="#060606"/>
    </linearGradient>
  </defs>
  <rect width="420" height="640" rx="24" fill="url(#g)"/>
  <rect x="28" y="28" width="364" height="584" rx="20" fill="none" stroke="rgba(255,255,255,0.08)"/>
  <circle cx="210" cy="222" r="52" fill="rgba(255,255,255,0.05)"/>
  <path d="M188 204h44v72h-44z" fill="rgba(255,255,255,0.35)"/>
  <path d="M176 216h68v10h-68zM176 234h68v10h-68zM176 252h48v10h-48z" fill="rgba(255,255,255,0.35)"/>
  <text x="210" y="342" fill="#d7d7d7" text-anchor="middle" font-family="Arial, sans-serif" font-size="24" letter-spacing="4">BOOK</text>
  <text x="210" y="382" fill="#9f9f9f" text-anchor="middle" font-family="Arial, sans-serif" font-size="16" letter-spacing="2">NO COVER</text>
  <text x="210" y="452" fill="#ffffff" text-anchor="middle" font-family="Arial, sans-serif" font-size="20">{title_text}</text>
  <text x="210" y="486" fill="#a8a8a8" text-anchor="middle" font-family="Arial, sans-serif" font-size="14">{authors_text}</text>
</svg>"""
    return Response(svg, mimetype="image/svg+xml")


@app.route("/books/<entry_id>/cover")
def books_entry_cover(entry_id):
    fetched = fetch_books_entries()
    entries = list(fetched.get("entries", []) or [])
    normalized_entry_id = compact_notion_id(entry_id)
    entry = next((item for item in entries if item.get("id") == normalized_entry_id), None)
    if not entry:
        return build_books_cover_placeholder_svg("No cover")

    cover_url = str(entry.get("cover_url") or "").strip()
    if cover_url:
        return redirect(cover_url, code=302)

    title = entry.get("title") or ""
    authors = entry.get("authors_display") or ""
    cache_key = normalize_book_cover_cache_key(title, authors)
    cached_value = BOOK_COVER_CACHE.get(cache_key)
    if cached_value is None:
        cached_value = ""
        for source_fetcher in (fetch_openlibrary_cover_url, fetch_google_books_cover_url):
            try:
                cached_value = source_fetcher(title, authors)
            except Exception:
                cached_value = ""
            if cached_value:
                break
        BOOK_COVER_CACHE[cache_key] = cached_value

    if cached_value:
        return redirect(cached_value, code=302)
    return build_books_cover_placeholder_svg(title, authors)


@app.route("/api/books/quotes/import", methods=["POST"])
def books_quotes_import():
    payload = request.get_json(silent=True) if request.is_json else {}
    payload = payload if isinstance(payload, dict) else {}
    dry_run_value = payload.get("dry_run", request.form.get("dry_run", "true"))
    dry_run = str(dry_run_value or "").strip().lower() in {"1", "true", "yes", "on"}
    source_page_id = str(payload.get("source_page_id", request.form.get("source_page_id", "")) or "").strip()
    source_page_title = str(payload.get("source_page_title", request.form.get("source_page_title", "")) or "").strip()
    try:
        report = import_book_quotes_from_notion(
            source_page_id=source_page_id,
            source_page_title=source_page_title,
            dry_run=dry_run,
        )
    except requests.RequestException as exc:
        return jsonify({"ok": False, "error": f"Notion request failed: {exc}"}), 502
    except RuntimeError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    return jsonify(report)


@app.route("/api/books/quotes/readiness", methods=["GET", "POST"])
def books_quotes_readiness():
    payload = request.get_json(silent=True) if request.is_json else {}
    payload = payload if isinstance(payload, dict) else {}
    database_id = str(payload.get("database_id", request.args.get("database_id", request.form.get("database_id", ""))) or "").strip()
    try:
        report = inspect_book_quotes_migration_readiness(database_id=database_id)
    except requests.RequestException as exc:
        return jsonify({"ok": False, "error": f"Notion request failed: {exc}"}), 502
    return jsonify(report)


@app.route("/api/books/quotes/migrate", methods=["POST"])
def books_quotes_migrate():
    payload = request.get_json(silent=True) if request.is_json else {}
    payload = payload if isinstance(payload, dict) else {}
    dry_run_value = payload.get("dry_run", request.form.get("dry_run", "true"))
    dry_run = str(dry_run_value or "").strip().lower() in {"1", "true", "yes", "on"}
    database_id = str(payload.get("database_id", request.form.get("database_id", "")) or "").strip()
    try:
        report = migrate_book_quotes_rich_text_to_database(
            dry_run=dry_run,
            database_id=database_id,
        )
    except requests.RequestException as exc:
        return jsonify({"ok": False, "error": f"Notion request failed: {exc}"}), 502
    except RuntimeError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    return jsonify(report)


@app.route("/reading")
def reading():
    reading_view = build_reading_view()
    reading_view["success_message"] = str(request.args.get("success", "") or "").strip()
    reading_view["error_message"] = str(request.args.get("error", "") or "").strip()
    return render_template(
        "reading.html",
        title="Reading",
        build_query_url=build_query_url,
        ai_default_mode="cinematic",
        ai_page_context="general",
        **reading_view,
    )


@app.route("/reading/article/<entry_id>")
def reading_article(entry_id):
    reading_view = build_reading_view()
    entries = list(reading_view.get("entries", []) or [])
    preferred_card_entry = get_reading_entry(entry_id)
    preferred_card_image_url = normalize_reading_url(preferred_card_entry.get("image_url", "")) if preferred_card_entry else ""
    force_refresh = str(request.args.get("refresh", "") or "").strip().lower() in {"1", "true", "yes", "on", "force"}
    entry = ensure_reading_entry_content(entry_id, force_refresh=force_refresh)
    if not entry:
        return Response("Reading entry not found.", status=404)
    current_index = next((index for index, item in enumerate(entries) if item.get("id") == str(entry_id or "").strip()), -1)
    if entry.get("status") == "unread":
        updated_entry = update_reading_entry(entry_id, {"status": "reading"})
        if updated_entry:
            entry = updated_entry
            if current_index >= 0:
                entries[current_index] = updated_entry
    source_url = entry.get("original_url") or entry.get("url")
    lead_image_url = normalize_reading_url(entry.get("lead_image_url", "")) if entry.get("lead_image_kind") in {"explicit", "feed_cover"} else ""
    article_hero_image = reading_choose_article_hero_image(
        preferred_image=preferred_card_image_url,
        lead_image=lead_image_url,
        content_html=entry.get("content_html", ""),
        article_url=source_url,
        source_url=entry.get("feed_url", ""),
        source_name=entry.get("source", ""),
    )
    if article_hero_image and normalize_reading_image_identity(entry.get("author_image_url", "")) == normalize_reading_image_identity(article_hero_image):
        article_hero_image = ""
    article_author_image_url = entry.get("author_image_url", "")
    if article_author_image_url and not reading_is_valid_author_avatar(
        article_author_image_url,
        article_url=source_url,
        author_name=entry.get("author", ""),
        hero_image=article_hero_image,
        entry_image=entry.get("image_url", ""),
    ):
        article_author_image_url = ""
    article_html = sanitize_reading_article_html(
        entry.get("content_html", ""),
        base_url=source_url,
        hero_image=article_hero_image,
        author_image=article_author_image_url,
    )
    article_text = reading_entry_body_text(entry)
    article_paragraphs = [] if article_html else [paragraph.strip() for paragraph in article_text.split("\n\n") if paragraph.strip()]
    if not article_paragraphs and article_text:
        article_paragraphs = [article_text]
    article_direction = detect_reading_direction(
        entry.get("title", ""),
        article_text,
        strip_reading_html(entry.get("content_html", "")),
    )
    tts_payload = build_reading_tts_payload(entry)
    prev_entry = entries[current_index - 1] if current_index > 0 else None
    next_entry = entries[current_index + 1] if current_index >= 0 and current_index < len(entries) - 1 else None
    article_query = reading_view.get("filter_query", {})
    reading_return_url = url_for("reading", **article_query)
    return render_template(
        "reading_article.html",
        title=entry.get("title") or "Reading Article",
        entry=entry,
        article_html=Markup(article_html) if article_html else "",
        article_paragraphs=article_paragraphs,
        article_dir=article_direction["dir"],
        article_lang=article_direction["lang"],
        article_hero_image=article_hero_image,
        article_author_name=entry.get("author", ""),
        article_author_image_url=article_author_image_url,
        reading_tts_available=tts_payload["available"],
        reading_tts_status_message=tts_payload["status_message"],
        reading_tts_audio_url=reading_tts_audio_url(entry.get("id", ""), tts_payload["text_hash"]) if tts_payload["available"] else "",
        reading_tts_timings_url=reading_tts_timings_url(entry.get("id", ""), tts_payload["text_hash"]) if tts_payload["available"] else "",
        reading_tts_lang=tts_payload["lang"],
        reading_tts_voice=tts_payload["voice"],
        reading_tts_version=tts_payload["text_hash"],
        article_query=article_query,
        reading_return_url=reading_return_url,
        reading_prev_url=url_for("reading_article", entry_id=prev_entry.get("id"), **article_query) if prev_entry else "",
        reading_next_url=url_for("reading_article", entry_id=next_entry.get("id"), **article_query) if next_entry else "",
        reading_context_label=" / ".join([value for value in [
            entry.get("source", ""),
            reading_category_label(entry.get("category", "")) if entry.get("category") else "",
            entry.get("topic_display", "") or "",
            entry.get("status", ""),
        ] if value]),
        ai_default_mode="cinematic",
        ai_page_context="general",
    )


@app.route("/reading/article/<entry_id>/audio", methods=["GET"])
def reading_article_audio(entry_id):
    force_refresh = str(request.args.get("refresh", "") or "").strip().lower() in {"1", "true", "yes", "on", "force"}
    entry = ensure_reading_entry_content(entry_id, force_refresh=force_refresh)
    if not entry:
        return Response("Reading entry not found.", status=404, mimetype="text/plain")

    tts_payload = build_reading_tts_payload(entry)
    if not tts_payload["available"]:
        return Response(tts_payload["status_message"], status=422, mimetype="text/plain")
    if edge_tts is None:
        return Response("Audio generation is unavailable right now.", status=503, mimetype="text/plain")

    cache_path = Path(tts_payload["cache_path"])
    if not cache_path.exists() or not Path(tts_payload["timings_path"]).exists():
        with READING_TTS_GENERATION_LOCK:
            if not cache_path.exists() or not Path(tts_payload["timings_path"]).exists():
                try:
                    ensure_reading_tts_cache(tts_payload)
                except Exception as exc:
                    try:
                        if cache_path.exists() and cache_path.stat().st_size == 0:
                            cache_path.unlink()
                    except Exception:
                        pass
                    return Response(f"Could not generate reading audio: {exc}", status=503, mimetype="text/plain")

    if not cache_path.exists():
        return Response("Audio cache could not be prepared.", status=503, mimetype="text/plain")

    response = send_file(
        str(cache_path),
        mimetype="audio/mpeg",
        as_attachment=False,
        download_name=cache_path.name,
        conditional=True,
        max_age=60 * 60 * 24 * 30,
    )
    response.headers["X-Reading-TTS-Lang"] = tts_payload["lang"]
    response.headers["X-Reading-TTS-Voice"] = tts_payload["voice"]
    return response


@app.route("/reading/article/<entry_id>/audio/timings", methods=["GET"])
def reading_article_audio_timings(entry_id):
    entry = ensure_reading_entry_content(entry_id, force_refresh=False)
    if not entry:
        return jsonify({"ok": False, "error": "Reading entry not found."}), 404

    tts_payload = build_reading_tts_payload(entry)
    if not tts_payload["available"]:
        return jsonify({"ok": False, "error": tts_payload["status_message"], "timings": []}), 422

    timings_path = Path(tts_payload["timings_path"])
    if not timings_path.exists():
        with READING_TTS_GENERATION_LOCK:
            if not timings_path.exists():
                try:
                    ensure_reading_tts_cache(tts_payload)
                except Exception as exc:
                    return jsonify({"ok": False, "error": f"Could not prepare sentence timings: {exc}", "timings": []}), 503

    try:
        payload = json.loads(timings_path.read_text(encoding="utf-8"))
    except Exception:
        payload = save_reading_tts_timings(timings_path, tts_payload["metadata_path"], tts_payload["text"], tts_payload)
    if not isinstance(payload, dict):
        payload = save_reading_tts_timings(timings_path, tts_payload["metadata_path"], tts_payload["text"], tts_payload)
    if int(payload.get("timings_version", 0) or 0) < READING_TTS_TIMINGS_VERSION:
        payload = save_reading_tts_timings(timings_path, tts_payload["metadata_path"], tts_payload["text"], tts_payload)
    payload.setdefault("source", "edge_sentence_boundary" if Path(tts_payload["metadata_path"]).exists() else "estimated")
    payload.setdefault("sync_lead_seconds", READING_TTS_SYNC_LEAD_SECONDS)
    payload.setdefault("audio_start_offset_seconds", 0.0)
    payload.setdefault("blocks", tts_payload.get("blocks", []))
    if "sentences" not in payload:
        payload["sentences"] = [
            {
                "index": item.get("index", index),
                "text": item.get("text", ""),
                "block_type": item.get("block_type", "paragraph"),
                "block_index": item.get("block_index", index),
                "block_order": item.get("block_order", index),
            }
            for index, item in enumerate(payload.get("timings", []) or [])
            if isinstance(item, dict)
        ]
    payload["ok"] = True
    return jsonify(payload)


@app.route("/reading/source/add", methods=["POST"])
def reading_source_add():
    name = str(request.form.get("name", "") or "").strip()
    url = normalize_reading_url(request.form.get("url", ""))
    topic = str(request.form.get("topic", "") or "").strip()
    category = normalize_reading_category(request.form.get("category", ""))
    active = str(request.form.get("active", "") or "").strip().lower() in {"1", "true", "yes", "on"}
    next_url = str(request.form.get("next", "") or url_for("reading")).strip() or url_for("reading")
    if not name:
        return redirect(append_query_param(next_url, error="Source name is required."))
    _, message = upsert_reading_source_record(name=name, url=url, topic=topic, category=category, active=active, source_id=str(request.form.get("source_id", "") or "").strip())
    return redirect(append_query_param(next_url, success=message))


@app.route("/reading/source/<source_id>/toggle", methods=["POST"])
def reading_source_toggle(source_id):
    next_url = str(request.form.get("next", "") or url_for("reading")).strip() or url_for("reading")
    toggled = toggle_reading_source_active(source_id)
    if toggled:
        return redirect(append_query_param(next_url, success=f"Updated source {toggled.get('name', '')}."))
    return redirect(append_query_param(next_url, error="Source not found."))


@app.route("/reading/source/<source_id>/remove", methods=["POST"])
def reading_source_remove(source_id):
    next_url = str(request.form.get("next", "") or url_for("reading")).strip() or url_for("reading")
    data = load_reading_data()
    source_id = str(source_id or "").strip()
    removed_name = next((str(source.get("name", "") or "").strip() for source in data.get("sources", []) if isinstance(source, dict) and source.get("id") == source_id), "source")
    removed = remove_reading_source(source_id)
    if not removed:
        return redirect(append_query_param(next_url, error="Source not found."))
    return redirect(append_query_param(next_url, success=f"Removed source {removed_name}."))


@app.route("/reading/sync", methods=["POST"])
def reading_sync():
    next_url = str(request.form.get("next", "") or url_for("reading")).strip() or url_for("reading")
    source_id = str(request.form.get("source_id", "") or "").strip()
    result = sync_reading_sources(source_id=source_id)
    imported_total = int(result.get("imported_total", 0) or 0)
    if imported_total > 0:
        message = f"{imported_total} new article(s) imported."
    else:
        message = str(result.get("last_sync_message", "") or "").strip() or "Already up to date."
    redirect_kwargs = {"success": message}
    if imported_total > 0:
        redirect_kwargs["fresh"] = "1"
    return redirect(append_query_param(next_url, **redirect_kwargs))


@app.route("/reading/export", methods=["GET"])
def reading_export():
    data = load_reading_data()
    export_payload = json.dumps(data, indent=2, ensure_ascii=False)
    filename = f"reading-data-export-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}.json"
    return Response(
        export_payload,
        mimetype="application/json",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Cache-Control": "no-store",
        },
    )


@app.route("/reading/import", methods=["POST"])
def reading_import():
    next_url = str(request.form.get("next", "") or url_for("admin")).strip() or url_for("admin")
    uploaded = request.files.get("reading_data_file")
    if not uploaded or not str(getattr(uploaded, "filename", "") or "").strip():
        return redirect(append_query_param(next_url, error="Choose a Reading JSON file to import."))
    try:
        raw_text = uploaded.read().decode("utf-8-sig")
        payload = json.loads(raw_text)
        if not isinstance(payload, dict):
            raise ValueError("Reading import file must contain a JSON object.")
        normalized = save_reading_data(payload)
        source_count = len(normalized.get("sources", []))
        entry_count = len(normalized.get("entries", []))
        message = f"Imported Reading data: {source_count} source(s), {entry_count} entries."
        return redirect(append_query_param(next_url, success=message))
    except Exception as exc:
        return redirect(append_query_param(next_url, error=f"Reading import failed: {exc}"))


@app.route("/reading/backup/<path:filename>", methods=["GET"])
def reading_backup_download(filename):
    safe_name = Path(str(filename or "")).name
    if not safe_name:
        return Response("Backup not found.", status=404)
    target = READING_BACKUPS_DIR / safe_name
    if not target.exists() or not target.is_file():
        return Response("Backup not found.", status=404)
    try:
        return Response(
            target.read_text(encoding="utf-8"),
            mimetype="application/json",
            headers={
                "Content-Disposition": f'attachment; filename="{safe_name}"',
                "Cache-Control": "no-store",
            },
        )
    except Exception:
        return Response("Backup not found.", status=404)


@app.route("/reading/entry/<entry_id>/action", methods=["POST"])
def reading_entry_action(entry_id):
    next_url = str(request.form.get("next", "") or url_for("reading")).strip() or url_for("reading")
    action = str(request.form.get("action", "") or "").strip().lower()
    if action in READING_STATUSES:
        entry = update_reading_entry(entry_id, {"status": action})
    elif action == "star":
        entry = update_reading_entry(entry_id, {"starred": True})
    elif action == "unstar":
        entry = update_reading_entry(entry_id, {"starred": False})
    else:
        return redirect(append_query_param(next_url, error="Unknown reading action."))
    if not entry:
        return redirect(append_query_param(next_url, error="Reading entry not found."))
    if action in READING_STATUSES:
        message = f"Marked {entry.get('title', 'article')} as {action}."
    elif action == "star":
        message = f"Starred {entry.get('title', 'article')}."
    else:
        message = f"Unstarred {entry.get('title', 'article')}."
    return redirect(append_query_param(next_url, success=message))


@app.route("/movies-review")
def movies_review():
    review_queue = build_movies_review_queue()
    return render_template(
        "movies_review.html",
        review_items=review_queue["items"],
        summary=review_queue["summary"],
        score_display=SCORE_DISPLAY,
        score_color=SCORE_COLOR,
        ai_default_mode="cinematic",
        ai_page_context="movie"
    )


@app.route("/admin/review-movies", methods=["GET", "POST"])
def admin_review_movies():
    if request.method == "POST":
        action = str(request.form.get("action", "next") or "next").strip().lower()
        try:
            index = max(int(request.form.get("index", "0")), 0)
        except (TypeError, ValueError):
            index = 0
        review_queue = build_admin_movie_review_queue()
        items = review_queue["items"]
        current_item = items[index] if index < len(items) else None

        if action == "apply":
            if not current_item or not current_item.get("proposed_correction") or not current_item.get("notion_page_id"):
                return redirect(url_for("admin_review_movies", index=index, notice="No safe proposal available to apply."))
            pages = {page.get("id", ""): page for page in fetch_all_notion_database_pages()}
            page = pages.get(current_item["notion_page_id"])
            if not page:
                return redirect(url_for("admin_review_movies", index=index, notice="Notion page could not be found."))
            payload = build_review_correction_payload(page.get("properties", {}) or {}, current_item)
            if not payload:
                return redirect(url_for("admin_review_movies", index=index, notice="No safe field changes were found to apply."))
            try:
                update_notion_page_properties(current_item["notion_page_id"], payload)
                clear_runtime_cache()
                refresh_film_cache_from_source()
            except requests.RequestException as exc:
                return redirect(url_for("admin_review_movies", index=index, notice=f"Apply failed while updating Notion: {exc}"))
            next_index = index + 1
            return redirect(url_for("admin_review_movies", index=next_index, notice="Updated in Notion"))

        next_index = index + 1
        notice_map = {
            "ok": "Marked as OK for this review pass.",
            "skip": "Skipped for now.",
            "next": "Moved to the next movie.",
        }
        return redirect(url_for("admin_review_movies", index=next_index, notice=notice_map.get(action, notice_map["next"])))

    review_queue = build_admin_movie_review_queue()
    items = review_queue["items"]
    summary = review_queue["summary"]
    notice = str(request.args.get("notice", "") or "").strip()
    try:
        index = max(int(request.args.get("index", "0")), 0)
    except (TypeError, ValueError):
        index = 0

    current_item = items[index] if index < len(items) else None
    return render_template(
        "admin_review_movies.html",
        review_item=current_item,
        queue_index=index,
        queue_total=len(items),
        summary=summary,
        notice=notice,
        score_display=SCORE_DISPLAY,
        score_color=SCORE_COLOR,
        ai_default_mode="cinematic",
        ai_page_context="movie"
    )


@app.route("/movies-final-review")
def movies_final_review():
    review_payload = build_targeted_movie_review()
    confidence_filter = normalize_movie_review_filter(request.args.get("confidence", "all"), {"high", "medium", "all"})
    row_type_filter = normalize_movie_review_filter(request.args.get("row_type", "all"), {"real", "non_movie", "all"})
    filtered_review_items = filter_movie_review_items(
        review_payload["items"],
        confidence_filter=confidence_filter,
        row_type_filter=row_type_filter,
    )
    review_counts = {
        "high": sum(1 for item in review_payload["items"] if normalize_movie_category(item.get("confidence", "")) == "high"),
        "medium": sum(1 for item in review_payload["items"] if normalize_movie_category(item.get("confidence", "")) == "medium"),
        "all": len(review_payload["items"]),
        "real": sum(1 for item in review_payload["items"] if normalize_movie_category(item.get("row_type", "")) == "real"),
        "non_movie": sum(1 for item in review_payload["items"] if normalize_movie_category(item.get("row_type", "")) == "non_movie"),
    }
    return render_template(
        "movies_final_review.html",
        review_items=filtered_review_items,
        summary=review_payload["summary"],
        filter_confidence=confidence_filter,
        filter_row_type=row_type_filter,
        review_counts=review_counts,
        visible_review_count=len(filtered_review_items),
        ai_default_mode="cinematic",
        ai_page_context="movie"
    )


@app.route("/movies-corrections-preview")
def movies_corrections_preview():
    report = build_correction_preview_report()
    apply_plan = build_strong_correction_apply_plan()
    confidence_filter = normalize_movie_review_filter(request.args.get("confidence", "all"), {"high", "medium", "all"})
    row_type_filter = normalize_movie_review_filter(request.args.get("row_type", "all"), {"real", "non_movie", "all"})
    status_filter = normalize_movie_review_filter(request.args.get("status", "all"), {"applied", "skipped", "failed", "all"})
    filtered_planned_updates = filter_movie_review_items(
        apply_plan["items"],
        confidence_filter=confidence_filter,
        row_type_filter=row_type_filter,
    )
    filtered_strong_matches = filter_movie_review_items(
        report["groups"]["strong_matches"],
        confidence_filter=confidence_filter,
        row_type_filter=row_type_filter,
    )
    filtered_possible_matches = filter_movie_review_items(
        report["groups"]["possible_matches"],
        confidence_filter=confidence_filter,
        row_type_filter=row_type_filter,
    )
    filtered_conflicts = filter_movie_review_items(
        report["groups"]["conflicts"],
        confidence_filter=confidence_filter,
        row_type_filter=row_type_filter,
    )
    filtered_wrong_section_rows = filter_movie_review_items(
        report["groups"]["wrong_section_rows"],
        confidence_filter=confidence_filter,
        row_type_filter=row_type_filter,
    )
    filtered_no_match_rows = filter_movie_review_items(
        report["groups"]["no_match_rows"],
        confidence_filter=confidence_filter,
        row_type_filter=row_type_filter,
    )
    selected_default_count = sum(1 for item in apply_plan["items"] if item.get("default_selected"))
    applied_path = request.args.get("applied_path", "")
    apply_report = load_correction_report(applied_path)
    apply_result_rows = []
    for item in (apply_report.get("applied_items", []) if apply_report else []):
        apply_result_rows.append({**item, "status": "applied", "applied_fields": item.get("applied_properties", [])})
    for item in (apply_report.get("skipped_items", []) if apply_report else []):
        apply_result_rows.append({**item, "status": "skipped", "applied_fields": []})
    for item in (apply_report.get("failed_items", []) if apply_report else []):
        apply_result_rows.append({**item, "status": "failed", "applied_fields": []})
    filtered_apply_result_rows = [
        item for item in apply_result_rows
        if status_filter == "all" or normalize_movie_category(item.get("status", "")) == status_filter
    ]
    preview_counts = {
        "high": sum(1 for item in report["rows"] if normalize_movie_category(item.get("confidence", "")) == "strong"),
        "medium": sum(1 for item in report["rows"] if normalize_movie_category(item.get("confidence", "")) == "possible"),
        "all": len(report["rows"]),
        "real": sum(1 for item in report["rows"] if normalize_movie_category(item.get("row_type", "")) == "real"),
        "non_movie": sum(1 for item in report["rows"] if normalize_movie_category(item.get("row_type", "")) == "non_movie"),
    }
    result_counts = {
        "applied": sum(1 for item in apply_result_rows if normalize_movie_category(item.get("status", "")) == "applied"),
        "skipped": sum(1 for item in apply_result_rows if normalize_movie_category(item.get("status", "")) == "skipped"),
        "failed": sum(1 for item in apply_result_rows if normalize_movie_category(item.get("status", "")) == "failed"),
        "all": len(apply_result_rows),
    }
    return render_template(
        "movies_corrections_preview.html",
        summary=report["summary"],
        apply_plan_summary=apply_plan["summary"],
        planned_updates=filtered_planned_updates,
        selected_default_count=sum(1 for item in filtered_planned_updates if item.get("default_selected")),
        selected_high_only=bool(selected_default_count) and selected_default_count == len(apply_plan["items"]),
        strong_matches=filtered_strong_matches,
        possible_matches=filtered_possible_matches,
        conflicts=filtered_conflicts,
        wrong_section_rows=filtered_wrong_section_rows,
        no_match_rows=filtered_no_match_rows,
        filter_confidence=confidence_filter,
        filter_row_type=row_type_filter,
        filter_status=status_filter,
        preview_counts=preview_counts,
        result_counts=result_counts,
        success_message=request.args.get("success", ""),
        error_message=request.args.get("error", ""),
        backup_path=request.args.get("backup_path", ""),
        applied_path=applied_path,
        apply_report=apply_report,
        apply_result_rows=filtered_apply_result_rows,
        ai_default_mode="cinematic",
        ai_page_context="movie"
    )


@app.route("/movies-corrections-apply", methods=["POST"])
def movies_corrections_apply():
    try:
        selected_rows = request.form.getlist("selected_rows")
        result = apply_strong_csv_corrections(selected_rows)
    except requests.RequestException as exc:
        return redirect(url_for("movies_corrections_preview", error=f"Apply failed while updating Notion: {exc}"))
    except RuntimeError as exc:
        return redirect(url_for("movies_corrections_preview", error=str(exc)))
    success_message = f"Updated {result['applied']} items in Notion. Skipped {result['skipped']} rows. Failed {result['failed']} rows."
    return redirect(url_for(
        "movies_corrections_preview",
        success=success_message,
        backup_path=result.get("backup_path", ""),
        applied_path=result.get("applied_path", ""),
        confidence=request.form.get("confidence", "all"),
        row_type=request.form.get("row_type", "all"),
        status=request.form.get("status", "all")
    ))

@app.route("/video/<entry_id>")
def video_detail(entry_id):
    context = get_video_detail_context(entry_id)
    if not context:
        missing_entry = {
            "entry_id": entry_id,
            "title": "Missing Video",
            "name": "Missing Video",
            "playlist_name": "",
            "playlist_url": "",
            "video_id": "",
            "duration": "",
            "section": "",
            "url": "",
        }
        return render_template(
            "video_detail.html",
            missing=True,
            entry=missing_entry,
            entry_type="youtube",
            related_title="Missing video",
            player_video_id="",
            prev_entry=None,
            next_entry=None,
            related_entries=[],
            related_total_pages=0,
            related_page=1,
            pagination_numbers=[],
            related_order="normal",
            related_seed="",
            delete_endpoint=False,
            ai_default_mode="cinematic",
            ai_page_context="general",
        ), 404
    try:
        page = max(int(request.args.get("page", 1)), 1)
    except (TypeError, ValueError):
        page = 1
    related_order = (request.args.get("related_order") or "normal").strip().lower()
    if related_order not in ("normal", "shuffle"):
        related_order = "normal"
    related_seed = (request.args.get("related_seed") or "").strip()
    seed_value = related_seed or context["entry"].get("entry_id", entry_id)
    related_entries_full = list(context["related_entries"])

    if context.get("entry_type") == "youtube":
        playlist_entries_full = list(context.get("playlist_entries", []))
        if related_order == "shuffle":
            playlist_entries_full = build_shuffled_related_entries(playlist_entries_full, seed_value)
        current_playlist_index = next((
            index for index, video in enumerate(playlist_entries_full)
            if video.get("entry_id") == context["entry"].get("entry_id")
        ), 0)
        context["prev_entry"] = playlist_entries_full[current_playlist_index - 1] if current_playlist_index > 0 else None
        context["next_entry"] = (
            playlist_entries_full[current_playlist_index + 1]
            if current_playlist_index < len(playlist_entries_full) - 1 else None
        )
        related_entries_full = [
            video for video in playlist_entries_full
            if video.get("entry_id") != context["entry"].get("entry_id")
        ]
    elif related_order == "shuffle":
        related_entries_full = build_shuffled_related_entries(related_entries_full, seed_value)

    related_page = paginate_items(related_entries_full, page, 10)
    context["related_entries"] = []
    for item in related_page["items"]:
        item_copy = dict(item)
        item_copy["detail_url"] = build_related_video_detail_url(
            item_copy.get("entry_id", ""),
            related_order=related_order,
            related_seed=seed_value if related_order == "shuffle" else ""
        )
        context["related_entries"].append(item_copy)
    context["related_entries_full"] = related_entries_full
    context["related_page"] = related_page["page"]
    context["related_total_pages"] = related_page["total_pages"]
    context["pagination_numbers"] = related_page["pagination"]
    context["related_order"] = related_order
    context["related_seed"] = seed_value if related_order == "shuffle" else ""
    context["related_random_entry"] = related_entries_full[0] if related_entries_full else None
    context["related_random_url"] = (
        build_related_video_detail_url(
            context["related_random_entry"].get("entry_id", ""),
            related_order=related_order,
            related_seed=seed_value if related_order == "shuffle" else ""
        )
        if context["related_random_entry"] else ""
    )
    context["prev_entry_url"] = (
        build_related_video_detail_url(
            context["prev_entry"].get("entry_id", ""),
            related_order=related_order,
            related_seed=seed_value if related_order == "shuffle" else ""
        )
        if context.get("prev_entry") else None
    )
    context["next_entry_url"] = (
        build_related_video_detail_url(
            context["next_entry"].get("entry_id", ""),
            related_order=related_order,
            related_seed=seed_value if related_order == "shuffle" else ""
        )
        if context.get("next_entry") else None
    )
    context["delete_endpoint"] = (
        url_for("delete_from_youtube", playlist_item_id=context["entry"].get("playlist_item_id") or "__lookup__")
        if context.get("entry_type") == "youtube"
        else None
    )
    context["section_route"] = get_section_route(context["entry"].get("section", ""))
    ai_context = ai_context_for_video(context.get("entry_type"), context["entry"].get("section", ""))
    return render_template("video_detail.html", missing=False, **context,
                           score_display=SCORE_DISPLAY, score_color=SCORE_COLOR, yts_url=yts_url,
                           build_query_url=build_query_url,
                           ai_default_mode=ai_context["mode"], ai_page_context=ai_context["page_context"])


@app.route("/director/<director_page_id>")
def director_detail(director_page_id):
    context = get_director_detail_context(director_page_id)
    if not context:
        return Response("Director not found.", status=404)
    return render_template(
        "director_detail.html",
        **context,
        score_display=SCORE_DISPLAY,
        score_color=SCORE_COLOR,
        ai_default_mode="cinematic",
        ai_page_context="movie"
    )


@app.route("/genre/<genre_page_id>")
def genre_detail(genre_page_id):
    films = [build_film_entry(film) for film in fetch_all_films()]
    genre_label = resolve_genre_filter_label(films, genre_page_id=genre_page_id)
    if not genre_label:
        return Response("Genre not found.", status=404)
    query_args = {}
    for key in ("search", "category", "status", "source", "score", "sort", "per_page", "page", "limit"):
        value = request.args.get(key)
        if value not in (None, ""):
            query_args[key] = value
    query_args["genre"] = genre_label
    query_args["genre_id"] = genre_page_id
    return redirect(url_for("library", **query_args))


@app.route("/history")
def history():
    deleted_entries = load_json_file(DELETED_HISTORY_PATH, [])
    if not isinstance(deleted_entries, list):
        deleted_entries = []
    return render_template("history.html", deleted_entries=deleted_entries, ai_default_mode="cinematic", ai_page_context="general")


@app.route("/youtube_auth/start")
def youtube_auth_start():
    if not youtube_dependencies_ready():
        return Response(
            "Missing Google API dependencies. Install google-api-python-client, google-auth-oauthlib, and google-auth-httplib2.",
            status=500
        )
    if not YOUTUBE_CLIENT_SECRET_PATH.exists():
        return Response(
            f"Missing OAuth client file at {YOUTUBE_CLIENT_SECRET_PATH}. Add your Google OAuth desktop/web client JSON first.",
            status=500
        )
    next_url = request.args.get("next") or url_for("home")
    flow = Flow.from_client_secrets_file(
        str(YOUTUBE_CLIENT_SECRET_PATH),
        scopes=YOUTUBE_OAUTH_SCOPES,
        redirect_uri=url_for("youtube_auth_callback", _external=True)
    )
    flow.code_verifier = secrets.token_urlsafe(64)
    authorization_url, state = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent"
    )
    session["youtube_oauth_state"] = state
    session["youtube_oauth_next"] = next_url
    session["youtube_oauth_code_verifier"] = flow.code_verifier
    return redirect(authorization_url)


@app.route("/youtube_auth/callback")
def youtube_auth_callback():
    if not youtube_oauth_ready():
        return redirect(url_for("home"))
    code_verifier = session.get("youtube_oauth_code_verifier")
    if not code_verifier:
        return Response("Missing PKCE code verifier in session. Restart the YouTube authorization flow.", status=400)
    flow = Flow.from_client_secrets_file(
        str(YOUTUBE_CLIENT_SECRET_PATH),
        scopes=YOUTUBE_OAUTH_SCOPES,
        state=session.get("youtube_oauth_state"),
        redirect_uri=url_for("youtube_auth_callback", _external=True)
    )
    flow.code_verifier = code_verifier
    flow.fetch_token(authorization_response=request.url)
    YOUTUBE_TOKEN_PATH.write_text(flow.credentials.to_json(), encoding="utf-8")
    session.pop("youtube_oauth_code_verifier", None)
    session.pop("youtube_oauth_state", None)
    return redirect(session.pop("youtube_oauth_next", url_for("home")))


@app.route("/delete_from_youtube/<playlist_item_id>", methods=["POST"])
def delete_from_youtube(playlist_item_id):
    payload = request.get_json(silent=True) or {}
    next_url = payload.get("next") or request.referrer or url_for("home")
    requested_video_id = payload.get("video_id", "")
    requested_playlist_id = payload.get("playlist_id", "")
    if not youtube_dependencies_ready():
        return jsonify({
            "ok": False,
            "error": "Missing Google API dependencies. Install google-api-python-client, google-auth-oauthlib, and google-auth-httplib2."
        }), 500
    if not YOUTUBE_CLIENT_SECRET_PATH.exists():
        return jsonify({
            "ok": False,
            "error": f"Missing OAuth client file at {YOUTUBE_CLIENT_SECRET_PATH}.",
        }), 500
    if not load_youtube_credentials():
        return jsonify({
            "ok": False,
            "auth_required": True,
            "auth_url": get_youtube_auth_url(next_url)
        }), 401

    youtube_entries = collect_all_youtube_entries()
    detail = next((video for video in youtube_entries if video.get("playlist_item_id") == playlist_item_id), None)
    if not detail and requested_video_id:
        detail = next((
            video for video in youtube_entries
            if video.get("video_id") == requested_video_id and (
                not requested_playlist_id or video.get("playlist_id") == requested_playlist_id
            )
        ), None)
    if not detail:
        return jsonify({"ok": False, "error": "This playlist item could not be found locally."}), 404

    try:
        service = build_youtube_service()
        if not service:
            return jsonify({"ok": False, "auth_required": True, "auth_url": get_youtube_auth_url(next_url)}), 401
        effective_playlist_item_id = detail.get("playlist_item_id") or playlist_item_id
        if not effective_playlist_item_id or effective_playlist_item_id == "__lookup__":
            effective_playlist_item_id = find_playlist_item_id(service, detail.get("playlist_id"), detail.get("video_id"))
        if not effective_playlist_item_id:
            app.logger.error(
                "YouTube delete lookup failed: playlist_item_id missing for video_id=%s playlist_id=%s",
                detail.get("video_id"),
                detail.get("playlist_id")
            )
            return jsonify({
                "ok": False,
                "error": "Could not resolve the playlist item ID from Google for this video."
            }), 404

        detail["playlist_item_id"] = effective_playlist_item_id
        append_deleted_history(detail)
        service.playlistItems().delete(id=effective_playlist_item_id).execute()
    except HttpError as exc:
        status, google_error = describe_http_error(exc)
        app.logger.error(
            "YouTube delete failed with status=%s playlist_item_id=%s video_id=%s playlist_id=%s response=%s",
            status,
            detail.get("playlist_item_id") or playlist_item_id,
            detail.get("video_id"),
            detail.get("playlist_id"),
            google_error
        )
        return jsonify({
            "ok": False,
            "error": f"YouTube delete failed ({status or 'unknown'}): {google_error}"
        }), 502
    except Exception as exc:
        app.logger.exception(
            "Unexpected YouTube delete failure for playlist_item_id=%s video_id=%s playlist_id=%s",
            detail.get("playlist_item_id") or playlist_item_id,
            detail.get("video_id"),
            detail.get("playlist_id")
        )
        return jsonify({"ok": False, "error": f"Unexpected delete failure: {exc}"}), 500

    remove_video_from_local_playlists_cache(detail)
    sync_deleted_video_to_playlists(detail)
    return jsonify({
        "ok": True,
        "playlist_item_id": detail.get("playlist_item_id") or playlist_item_id,
        "entry_id": detail.get("entry_id"),
        "redirect_url": get_section_route(detail.get("section", "")),
        "history_url": url_for("history")
    })

@app.route("/german")
def german():
    return render_section_page("German", title="🇩🇪 German Study", quick_delete_enabled=False)

@app.route("/chess")
def chess():
    return render_section_page("Chess", title="Chess", quick_delete_enabled=False)

@app.route("/library_yt")
def library_yt():
    return render_section_page("Library", title="Library", quick_delete_enabled=False)

@app.route("/watchlater")
def watchlater():
    return render_section_page("YouTube Watch Later", title="YouTube Watch Later", quick_delete_enabled=True)


@app.route("/section/<section_slug>")
def section_page(section_slug):
    section_slug_value = section_slug
    section_slug_fn = globals().get("section_slug")
    section = next((item for item in build_combined_sections() if item.get("slug") == section_slug_value), None)
    if not section:
        pockettube_section = _pockettube_section_membership_context(section_slug_value, admin_data=load_admin_data())
        if pockettube_section.get("channel_count", 0):
            section_name = pockettube_section.get("section_name", section_slug_value) or section_slug_value
            section = normalize_youtube_section_record({
                "name": section_name,
                "slug": section_slug_fn(section_name) if callable(section_slug_fn) else normalize_section_name(section_name),
                "playlists": [],
                "section_kind": "curated",
                "section_scope": "group",
                "channel_group_key": pockettube_section.get("group_key", ""),
                "channel_group_label": pockettube_section.get("group_name", ""),
                "section_order": 500,
            })
    if not section:
        return Response("Section not found.", status=404)
    quick_delete_enabled = normalize_section_name(section.get("name", "")) == normalize_section_name("YouTube Watch Later")
    return render_section_page(section.get("name", ""), title=section.get("name", ""), quick_delete_enabled=quick_delete_enabled)


@app.route("/pockettube")
def pockettube_groups():
    latest, imported_sections = _pockettube_latest_import_snapshot()
    search_query = str(request.args.get("q", "") or "").strip().lower()
    sort_key = str(request.args.get("sort", "default") or "default").strip().lower()
    cached_counts = {}
    for section_key, feed_context in _iter_cached_pockettube_group_feeds():
        if not isinstance(feed_context, dict):
            continue
        group_name = _pockettube_display_name(feed_context.get("group_name", "") or feed_context.get("name", "") or section_key)
        video_count = int(feed_context.get("feed_count", feed_context.get("video_count", 0)) or 0)
        cached_counts[normalize_pockettube_group_key(group_name)] = video_count
        cached_counts[normalize_pockettube_group_key(section_key)] = video_count
    pockettube_sections = []
    for section in build_combined_sections():
        if str(section.get("source", "") or "").strip().lower() != "pockettube":
            continue
        section_name = section.get("name", "")
        section_key = normalize_pockettube_group_key(section_name)
        group_label = section.get("channel_group_label", "") or section.get("channel_group_key", "")
        video_count = int(section.get("pockettube_video_count", 0) or section.get("video_count", 0) or cached_counts.get(section_key, 0) or cached_counts.get(normalize_pockettube_group_key(group_label), 0) or 0)
        item = {
            "name": section_name,
            "slug": section.get("slug", section_slug(section.get("name", ""))),
            "channel_count": int(section.get("pockettube_channel_count", 0) or 0),
            "video_count": video_count,
            "section_kind": section.get("section_kind", ""),
            "section_scope": section.get("section_scope", ""),
            "group_label": group_label,
        }
        haystack = " ".join([
            item["name"],
            item["group_label"],
            item["section_kind"],
            item["section_scope"],
        ]).lower()
        if search_query and search_query not in haystack:
            continue
        pockettube_sections.append(item)
    if sort_key == "channels":
        pockettube_sections.sort(key=lambda item: (-int(item.get("channel_count", 0) or 0), -int(item.get("video_count", 0) or 0), item.get("name", "").lower()))
    elif sort_key == "videos":
        pockettube_sections.sort(key=lambda item: (-int(item.get("video_count", 0) or 0), -int(item.get("channel_count", 0) or 0), item.get("name", "").lower()))
    elif sort_key in {"name", "az"}:
        pockettube_sections.sort(key=lambda item: item.get("name", "").lower())
    elif sort_key in {"name_desc", "za"}:
        pockettube_sections.sort(key=lambda item: item.get("name", "").lower(), reverse=True)
    else:
        sort_key = "default"
    return render_template(
        "pockettube_groups.html",
        title="PocketTube Groups",
        pockettube_sections=pockettube_sections,
        pockettube_import=latest,
        pockettube_source_sections=imported_sections,
        pockettube_search_query=search_query,
        pockettube_sort_key=sort_key,
        ai_default_mode="study",
        ai_page_context="study",
    )

@app.route("/proxy_notebook")
def proxy_notebook():
    mode = request.args.get("mode", "page")
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/135.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9"
    }
    try:
        upstream = requests.get(NOTEBOOKLM_URL, headers=headers, timeout=20, allow_redirects=True)
    except requests.RequestException as exc:
        preview = build_notebook_proxy_preview(f"Proxy request failed: {exc}")
        return Response(preview, status=200, content_type="text/html; charset=utf-8")

    content_type = upstream.headers.get("Content-Type", "").lower()
    final_url = upstream.url or NOTEBOOKLM_URL
    if upstream.status_code != 200 or "text/html" not in content_type:
        preview = build_notebook_proxy_preview(
            f"Google returned status {upstream.status_code}, so the panel switched to a pinned-tab style preview."
        )
        return Response(preview, status=200, content_type="text/html; charset=utf-8")

    html = prepare_notebook_proxy_html(upstream.text)
    if "accounts.google.com" in final_url:
        html = build_notebook_proxy_preview(
            "Google redirected the proxy to an authenticated sign-in flow, which cannot be completed reliably inside this embedded panel."
        )

    if mode == "raw":
        return Response(html, status=200, content_type="text/plain; charset=utf-8")

    response = Response(html, status=200, content_type="text/html; charset=utf-8")
    response.headers["Cache-Control"] = "no-store"
    return response

if __name__ == "__main__":
    debug_enabled = config_flag("FLASK_DEBUG", False) and not IS_PRODUCTION
    port = int(config_value("PORT", "5000") or "5000")
    app.run(host="0.0.0.0", port=port, debug=debug_enabled)

