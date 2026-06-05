# Articles V1 Freeze Checkpoint

This note records the current Articles/Reading V1 contract before any further refresh work lands.

Contract:
- `Sync Latest Articles` triggers RSS or GitHub sync only.
- `Pull Latest Articles` downloads or reloads the latest `reading_data.json` snapshot locally.
- The shared refresh layer only exposes state fields: `refresh_status`, `refresh_error`, `is_stale`, and `last_refreshed_at`.
- The Reading page keeps the existing snapshot freshness fields alongside the shared refresh state.
- Missing or stale snapshots must not crash the Reading page.
- Failed full-article cache diagnostics must stay internal and not be shown to users.
- A successful cached full article must still render normally.

Implementation notes:
- Keep `app.py` changes minimal for this area.
- Do not repurpose the pull route to perform sync work unless it truly performs the same snapshot pull.
- Treat this file as a freeze checkpoint for regression checks, not as a feature spec.
