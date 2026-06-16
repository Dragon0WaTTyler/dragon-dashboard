# Dragon Core Snapshot Publisher

This workflow generates the backend Dragon Core snapshot with the existing exporter and publishes `exports/dragon_core_snapshot.json`.

Manual run:
- GitHub Actions -> `Export Dragon Core Snapshot` -> `Run workflow`

Output:
- Snapshot file: `exports/dragon_core_snapshot.json`
- Workflow artifact: `dragon_core_snapshot`

What it does:
- installs backend requirements
- compile-checks the exporter path
- runs `tests.test_dragon_core_snapshot_export`
- exports the snapshot
- validates schema, required containers, counts, and minimum size
- audits the JSON for secrets, tokens, runtime paths, tracebacks, and transport/runtime leaks
- commits only `exports/dragon_core_snapshot.json` when it changed

What it does not do yet:
- no Notion sync
- no YouTube OAuth flow
- no live RSS fetch
- no PythonAnywhere notify step
- no iOS download step

Security rule:
- the published snapshot must not contain secrets, tokens, local paths, runtime/session data, or other internal leakage

Future:
- iOS can later download the raw GitHub snapshot, but that is out of scope for this task
