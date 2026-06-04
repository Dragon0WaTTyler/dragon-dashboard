# Reading Sync on GitHub Actions

Dragon runs on PythonAnywhere free, and free accounts cannot reliably fetch many external RSS feeds because outbound internet access is restricted to an allowlist. That means the Reading page can work online, but direct RSS refresh from PythonAnywhere is not dependable.

## V1 approach

GitHub Actions performs the RSS sync every 2 hours, builds a lightweight `reading_data.json` snapshot, and publishes that file to the `runtime-data` branch.

PythonAnywhere keeps its own local ignored `reading_data.json` and pulls the latest remote snapshot into that file when needed.

## Workflow behavior

The workflow lives at:

- `.github/workflows/sync-reading.yml`

It:

- runs every 2 hours
- can also be triggered manually with `workflow_dispatch`
- installs Python dependencies from `requirements.txt`
- runs `python scripts/sync_reading_feeds.py`
- runs `python scripts/export_reading_runtime_snapshot.py`
- publishes the lightweight snapshot to the `runtime-data` branch only if it changed

Runtime branch commit message:

- `Sync reading runtime snapshot`

## Manual trigger in GitHub

1. Open the GitHub repository.
2. Go to `Actions`.
3. Open `Sync Reading Feeds`.
4. Click `Run workflow`.

## Check whether reading data changed

After a run, inspect:

- the latest commit history on the `runtime-data` branch
- the `reading_data.json` diff on that branch
- the workflow log output from `scripts/sync_reading_feeds.py`

## Security notes

Do not commit:

- `.env`
- `youtube_token.json`
- `client_secret*.json`
- local cache/token/database files

`reading_data.json` should stay ignored on `main`. The runtime snapshot now lives on the separate `runtime-data` branch and is pulled into the live local file on PythonAnywhere.

## Local test

Run the sync locally with:

```bash
python scripts/sync_reading_feeds.py
```
