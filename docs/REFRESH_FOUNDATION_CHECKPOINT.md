# Refresh Foundation Checkpoint

This document captures the current internal checkpoint for Dragon's shared refresh foundation.

## Shared Refresh Layer

The shared refresh layer lives under `domains/shared/refresh/`.

Current adopters:
- Articles/Reading
- YouTube/PocketTube freshness

What the refresh layer owns:
- `refresh_status`
- `refresh_error`
- `is_stale`
- `last_refreshed_at`
- stale detection
- state description

What the refresh layer does not own:
- GitHub Action dispatch
- Pull Latest Articles snapshot download
- RSS fetching
- YouTube API fetching
- recipe generation
- cache file writes beyond existing domain services
- runtime/session state

## Articles Contract

- Sync Latest Articles remains separate.
- Pull Latest Articles remains separate.
- Recipe V0 reads local snapshot only.
- The shared refresh layer describes state for Articles, but it does not replace the existing sync or pull flow.

## YouTube Contract

- Existing freshness snapshot flow remains unchanged.
- Opening the PocketTube page must not trigger sync.
- Missing snapshot should surface safe missing/stale state, not crash.
- The shared refresh layer describes state for YouTube/PocketTube freshness snapshots, but it does not change YouTube API fetching.

## Future Adopters

- Books may adopt refresh state later only after its snapshot contract is stable.
- Do not adopt the refresh layer into Movies playback runtime yet.

## Rule

Refresh describes state. It does not perform domain sync unless a domain explicitly owns that behavior.
