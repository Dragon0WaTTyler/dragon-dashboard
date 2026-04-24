# Reading Sync on GitHub Actions

Dragon runs on PythonAnywhere free, and free accounts cannot reliably fetch many external RSS feeds because outbound internet access is restricted to an allowlist. That means the Reading page can work online, but direct RSS refresh from PythonAnywhere is not dependable.

## V1 approach

GitHub Actions performs the RSS sync every 2 hours and writes the updated snapshot into `reading_data.json`.

PythonAnywhere then only needs to:

- pull the latest Git changes
- reload the web app

This keeps `reading_data.json` as the V1 source of truth for Reading online without adding a database.

## Workflow behavior

The workflow lives at:

- `.github/workflows/sync-reading.yml`

It:

- runs every 2 hours
- can also be triggered manually with `workflow_dispatch`
- installs Python dependencies from `requirements.txt`
- runs `python scripts/sync_reading_feeds.py`
- commits `reading_data.json` only if it changed

Commit message:

- `Sync reading feeds`

## Manual trigger in GitHub

1. Open the GitHub repository.
2. Go to `Actions`.
3. Open `Sync Reading Feeds`.
4. Click `Run workflow`.

## Check whether reading data changed

After a run, inspect:

- the latest commit history for `Sync reading feeds`
- the `reading_data.json` diff in GitHub
- the workflow log output from `scripts/sync_reading_feeds.py`

## PythonAnywhere after GitHub Action runs

On PythonAnywhere, update the app with:

```bash
cd ~/Dragon
git pull
```

Then reload the web app from the PythonAnywhere Web tab.

## Security notes

Do not commit:

- `.env`
- `youtube_token.json`
- `client_secret*.json`
- local cache/token/database files

`reading_data.json` should stay tracked because it is the online Reading snapshot for V1.

## Local test

Run the sync locally with:

```bash
python scripts/sync_reading_feeds.py
```
