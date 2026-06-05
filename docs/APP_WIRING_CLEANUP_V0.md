# App Wiring Cleanup V0

This document records the internal checkpoint for Dragon's App Wiring Cleanup V0.

## What Changed

- app.py remains the Flask route/orchestration layer.
- app.py still owns the lazy singleton wrappers.
- dragon/wiring.py now owns constructor wiring only.
- The cleanup is not route extraction.
- The cleanup is not business-logic extraction.
- Reading and Articles service wiring moved behind explicit builder functions.
- YouTube freshness service wiring moved behind explicit builder functions.

## What Must Stay Stable

- Runtime behavior must remain unchanged.
- runtime behavior must remain unchanged.
- Sync behavior must remain unchanged.
- Refresh behavior must remain unchanged.
- Cache semantics must remain unchanged.

## Scope Boundaries

- No route extraction was done.
- No runtime behavior was intentionally changed.
- Do not touch movies, playback runtime, books, chess, Articles logic, YouTube UI, iOS, EXE, debrid, or global search during this wiring cleanup.

## Future Rule

- `app.py` may get thinner incrementally.
- Extract only one ownership area at a time.
- Do not move routes until service boundaries are stable.
- Do not touch playback, chess, or books during unrelated wiring cleanup.
