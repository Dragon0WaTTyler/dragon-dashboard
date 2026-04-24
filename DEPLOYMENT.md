# Dragon Online V1 Deployment

This project is a private personal dashboard. Online V1 should stay locked down and accessible only from your own devices.

## What must never be committed

Keep these files local only:

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
- `backups/`
- `cache/`
- `exports/`
- `csv_corrections/`
- `correction_reports/`
- any local database, cache, token, backup, or temporary files

The repository now ignores these through [`.gitignore`](C:/Users/walid/Desktop/FlaskDashboard/.gitignore).

## Required environment variables

Minimum for a private Render deploy:

- `FLASK_ENV=production`
- `FLASK_SECRET_KEY=<long random secret>`
- `DRAGON_ADMIN_USERNAME=<your private username>`
- `DRAGON_ADMIN_PASSWORD=<your private password>`
- `DRAGON_PROTECT_WHOLE_SITE=1`

Add the service keys your dashboard actually uses:

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

Optional local-only files still expected by some features:

- `youtube_token.json`
- `client_secret.json` or equivalent Google OAuth client file

If you want those features online, store the OAuth client file securely on the server disk and do not commit it.

## Local run

1. Create and activate a virtual environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Create `.env` with your private keys and credentials.
4. Run locally:

```bash
python app.py
```

The app reads `PORT` if set and otherwise defaults to `5000`.

## Render deploy

1. Push only safe source files to GitHub.
2. Create a new private Web Service on Render from the repo.
3. Runtime: `Python 3`.
4. Build command:

```bash
pip install -r requirements.txt
```

5. Start command:

```bash
gunicorn app:app --bind 0.0.0.0:$PORT --workers 1 --threads 4 --timeout 120
```

6. Add the environment variables listed above in the Render dashboard.
7. Set the health check path to `/healthz` if you want a simple unauthenticated uptime check.
8. If you need local JSON/cache state to persist across deploys, attach a Render persistent disk and mount it where the app can access its data files.

## Local file persistence warning

Dragon currently stores state in local JSON and cache files. On a stateless host:

- cache files may reset on deploy or restart
- uploaded/generated local state can disappear without a persistent disk
- SQLite and JSON-backed data should be treated as single-instance local storage only

For V1, that is acceptable if you understand the reset risk. If the hosted copy needs to preserve state, use a Render persistent disk and keep the file paths on that mounted volume.

## Current access model

- `DRAGON_ADMIN_USERNAME` and `DRAGON_ADMIN_PASSWORD` gate access
- `DRAGON_PROTECT_WHOLE_SITE=1` protects the whole dashboard, not just `/admin`
- if you prefer later, you can switch to admin-only protection by setting `DRAGON_PROTECT_WHOLE_SITE=0`
