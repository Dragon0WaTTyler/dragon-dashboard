# Dragon PythonAnywhere Deployment

Dragon is a private personal dashboard. This guide is for the free PythonAnywhere tier, with private login enabled.

## Files to upload

Upload the application source, templates, static files, and lightweight configuration files only.

Safe to upload:

- `app.py`
- `wsgi.py`
- `requirements.txt`
- `templates/`
- `static/`
- `Procfile` is optional and not used by PythonAnywhere
- this deployment guide

Do not upload secrets or local runtime data files:

- `.env`
- `youtube_token.json`
- `client_secret.json`
- `client_secret.json.json`
- `client_secrets.json`
- `cache_data.json`
- `youtube_duration_cache.json`
- `chat_history.db`
- `admin_data.json`
- `reading_data.json`
- `deleted_history.json`
- any backup, cache, database, or token files

These files are already covered by [`.gitignore`](C:/Users/walid/Desktop/FlaskDashboard/.gitignore).

## Virtualenv setup

1. Create a Python virtual environment on PythonAnywhere.
2. Open a Bash console.
3. Activate your virtualenv.

Example:

```bash
mkvirtualenv dragon --python=python3.11
workon dragon
```

If you already created the virtualenv from the Web tab, just activate that one.

## Install requirements

Inside the virtualenv:

```bash
pip install -r /home/<your-username>/path/to/Dragon/requirements.txt
```

If you copied the project to your PythonAnywhere home directory, use that absolute path.

## WSGI configuration

In the PythonAnywhere Web tab:

1. Create a new Web app.
2. Choose Manual configuration.
3. Pick the Python version that matches your virtualenv.
4. Set the virtualenv path.
5. Open the WSGI configuration file and point it at the app.

Use a WSGI file like this:

```python
import os
import sys

project_home = "/home/<your-username>/Dragon"
if project_home not in sys.path:
    sys.path.append(project_home)

os.environ.setdefault("FLASK_ENV", "production")

from wsgi import application
```

You can also import directly from `app` if you prefer:

```python
from app import app as application
```

## Environment variables

Set these in the PythonAnywhere Web tab or in a server-side `.env` file that is never committed:

- `FLASK_ENV=production`
- `FLASK_SECRET_KEY=<long random value>`
- `DRAGON_ADMIN_USERNAME=<private username>`
- `DRAGON_ADMIN_PASSWORD=<private password>`
- `DRAGON_PROTECT_WHOLE_SITE=1`
- `YOUTUBE_API_KEY`
- `GEMINI_API_KEY`
- `GEMINI_PROJECT_NAME`
- `GEMINI_PROJECT_NUMBER`
- `NOTION_TOKEN`
- `NOTION_DATABASE_ID`
- `NOTION_BOOKS_DATABASE_ID`
- `NOTION_BOOK_QUOTES_DATABASE_ID`
- `NOTION_BOOK_QUOTES_SOURCE_PAGE_ID`
- `NOTION_BOOK_QUOTES_SOURCE_PAGE_TITLE`
- `TMDB_API_KEY`
- `NOTION_DIRECTORS_DATABASE_ID`
- `NOTION_DIRECTORS_PARENT_PAGE_ID`
- `NOTION_GENRES_DATABASE_ID`
- `MOVIE_WANT_TO_UNION_FETCH_ENABLED`
- `NOTEBOOKLM_URL`

The app will refuse to start in production if `FLASK_SECRET_KEY` or the Dragon admin credentials are missing.

## Static files setup

In the PythonAnywhere Web tab, map static files manually:

- URL: `/static/`
- Directory: `/home/<your-username>/Dragon/static/`

If you add other local static assets later, keep them under `static/` so PythonAnywhere can serve them directly.

## Security warning

Never commit secret or token files to GitHub.

Keep these private:

- `.env`
- OAuth client JSON files
- YouTube token files
- cache databases
- local JSON data stores that contain personal data

Dragon is a private dashboard. Keep it behind the login gate and avoid exposing it publicly.

## Local file storage warning

Dragon still uses local JSON and cache files. On PythonAnywhere free hosting:

- files on the home directory can persist
- cache files can still be cleared by your own deploy or maintenance
- SQLite and JSON data should be treated as single-user local storage

If you need extra safety for important local state, keep regular backups.

## Summary

For a basic private deploy:

1. Upload the app and templates.
2. Create a virtualenv.
3. Install `requirements.txt`.
4. Configure the WSGI file.
5. Set the environment variables.
6. Configure `/static/`.
7. Log in with your private Dragon credentials.
