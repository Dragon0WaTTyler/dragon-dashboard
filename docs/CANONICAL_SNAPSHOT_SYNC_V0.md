# Canonical Snapshot Sync V0

This document defines the difference between canonical synced state and local runtime state in Dragon.

## Canonical Synced State

Canonical synced state is the user or system state that should survive across devices and deploys. It is the source of truth when a workflow intentionally publishes or pulls a shared snapshot.

## Local Runtime State

Local runtime state includes cache, temp, session, helper, and machine-specific files. These files support the current host, but they are not the canonical source of truth and should not be treated as sync targets.

## Current Articles Model

- GitHub RSS sync updates `reading_data.json` remotely.
- `Pull Latest Articles` downloads or reloads `reading_data.json` locally.
- The shared refresh layer only describes refresh state, including `refresh_status`, `refresh_error`, `is_stale`, and `last_refreshed_at`.
- The Reading page keeps its older snapshot freshness fields for compatibility.

## Current YouTube Model

- `cache/youtube_latest_snapshot.json` is a generated snapshot.
- `cache/youtube_latest_sync_status.json` is a generated sync-status artifact.

## Runtime Files That Must Not Be Committed

- `.env`
- `youtube_token.json`
- `client_secret.json`
- `cache_data.json`
- `reading_data.json` unless it is intentionally being used as the synced snapshot in the current workflow
- `chess_data.json`
- logs
- tmp files
- `__pycache__`

## Guardrail Rule

Sync canonical state only.

Never sync runtime sessions, temp playback files, secret files, local tokens, or machine-specific paths.

If a file is a runtime artifact, keep it local unless a workflow explicitly publishes it as canonical synced state.
