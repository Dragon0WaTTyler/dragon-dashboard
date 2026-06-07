# Dragon iOS API Contract V1

This document describes the current read-only JSON API foundation for the future native Dragon iOS client.

This is not a WebView app.
This is not a Capacitor app.
This is not React Native or Flutter.
This is a future SwiftUI native client.

## Scope

The current backend foundation exposes safe, read-only endpoints for:

- Service health
- Session/auth state
- Chess home summary
- Chess games list
- Chess game detail shell
- Chess courses list

The contract is intentionally minimal so the native iOS app can be built against stable JSON before any SwiftUI screens exist.

## Current Endpoints

### `GET /api/v1/health`

Purpose: simple service health check.

Example response:

```json
{
  "ok": true,
  "service": "dragon",
  "api_version": "v1"
}
```

### `GET /api/v1/me`

Purpose: safe session/debug surface only.

Example response:

```json
{
  "ok": true,
  "authenticated": true,
  "production": false
}
```

Notes:

- `authenticated` reflects the current session state.
- `production` reflects whether the app is running in production mode.
- No secrets, tokens, env values, API keys, filesystem paths, or passwords are returned.

### `GET /api/v1/chess/home`

Purpose: minimal chess home projection for the future iOS home screen.

Example response:

```json
{
  "ok": true,
  "section": "chess",
  "title": "Lotus Chess",
  "available": true,
  "summary": {
    "games_count": 12,
    "profiles_count": 2,
    "courses_count": 4,
    "training_available": true
  },
  "next_actions": [
    { "key": "train_today", "label": "Train Today" },
    { "key": "games", "label": "Games" },
    { "key": "openings", "label": "Openings" }
  ]
}
```

Notes:

- The response is safe and read-only.
- If chess data is missing or empty, the endpoint still returns `ok: true` with zero counts.

### `GET /api/v1/chess/games`

Purpose: read-only chess games list for the iOS games screen.

Query parameters:

- `limit` default `50`, max `100`
- `offset` default `0`
- `source` optional `chess.com` or `lichess`
- `result` optional `win`, `loss`, `draw`, or `unknown`

Example response:

```json
{
  "ok": true,
  "section": "chess",
  "items": [
    {
      "id": "lichess:game-123",
      "source": "lichess",
      "white": "Alpha",
      "black": "Beta",
      "user_color": "white",
      "user_result": "win",
      "result": "1-0",
      "date": "2026-01-05",
      "time_class": "rapid",
      "opening": {
        "name": "French Defense",
        "eco": "C00"
      }
    }
  ],
  "count": 1,
  "limit": 50,
  "offset": 0
}
```

Notes:

- The list response does not include raw PGN.
- The list response does not include full moves.
- The list response does not include `raw_source`.
- The list response stays safe for mobile consumption and pagination.

### `GET /api/v1/chess/games/<game_id>`

Purpose: read-only game detail shell for the future iOS detail screen.

Example response:

```json
{
  "ok": true,
  "section": "chess",
  "item": {
    "id": "lichess:game 1",
    "source": "lichess",
    "white": "Alpha",
    "black": "Beta",
    "user_color": "black",
    "user_result": "win",
    "result": "0-1",
    "date": "2026-01-05",
    "time_class": "rapid",
    "time_control": "10+0",
    "opening": {
      "name": "French Defense",
      "eco": "C00",
      "variation": "Advance"
    },
    "url": "https://lichess.org/game1",
    "rated": true,
    "pgn_available": true,
    "moves_available": true
  }
}
```

Missing game response:

```json
{
  "ok": false,
  "error": "game_not_found"
}
```

Notes:

- The endpoint accepts URL-encoded IDs.
- The endpoint does not return raw PGN yet.
- The endpoint does not return full moves yet.
- The endpoint does not return `raw_source`.

### `GET /api/v1/chess/train-today`

Purpose: safe projection of the current training candidates for the iOS Train Today screen.

Example response:

```json
{
  "ok": true,
  "section": "chess",
  "title": "Train Today",
  "available": true,
  "items": [
    {
      "id": "review-game-123",
      "type": "review",
      "title": "Alpha vs Beta",
      "subtitle": "Review from your games.",
      "source_game_id": "game-123",
      "opening": {
        "name": "French Defense",
        "eco": "C00"
      },
      "priority": 0,
      "completed": false
    }
  ],
  "count": 1
}
```

Notes:

- The endpoint is read-only and safe.
- If no candidates exist, it still returns `ok: true` with an empty `items` array.

### `GET /api/v1/chess/openings`

Purpose: opening summary list for the future iOS openings screen.

Query parameters:

- `limit` default `50`, max `100`
- `offset` default `0`
- `side` optional `white` or `black`
- `needs_work` optional `true` or `false`

Example response:

```json
{
  "ok": true,
  "section": "chess",
  "title": "Openings",
  "items": [
    {
      "key": "c00|french defense",
      "name": "French Defense",
      "eco": "C00",
      "side": "white",
      "games_count": 12,
      "wins": 5,
      "losses": 6,
      "draws": 1,
      "score_label": "50.0%",
      "needs_work": true
    }
  ],
  "count": 1
}
```

Notes:

- The endpoint is read-only and safe.
- It is derived from normalized chess game data only.
- No raw PGN, moves, or `raw_source` are returned.

### `GET /api/v1/chess/courses`

Purpose: course list for the future iOS courses screen.

Query parameters:

- `limit` default `50`, max `100`
- `offset` default `0`
- `category` optional `opening`, `calculation`, `endgame`, `strategy`, or `other`
- `status` optional `planned`, `active`, or `finished`

Example response:

```json
{
  "ok": true,
  "section": "chess",
  "title": "Courses",
  "items": [
    {
      "id": "course-1",
      "title": "Opening Principles",
      "category": "opening",
      "source": "youtube",
      "url": "https://example.com/opening-principles",
      "related_opening_key": "c00|french defense",
      "related_opening_label": "French Defense",
      "level": "beginner",
      "status": "active",
      "notes": "Intro to core ideas."
    }
  ],
  "count": 1
}
```

Notes:

- The endpoint is read-only and safe.
- It uses stored course URLs only and does not fetch anything remotely.
- If course data is missing or empty, it still returns `ok: true` with an empty `items` array.

## Safety Rules

The iOS API foundation must remain safe by default.

- No secrets
- No raw PGN yet
- No moves yet
- No `raw_source`
- No filesystem paths
- No env values
- No API keys
- No passwords

If a field is not required for the first native client screens, do not expose it.

## Native iOS V0 Screens

These are the first planned SwiftUI-native screens.

### Chess Home

Use `GET /api/v1/chess/home`.

Goal:

- Show the chess landing screen
- Present the current training status
- Offer a few clear next actions

### Games List

Use `GET /api/v1/chess/games`.

Goal:

- Show a paginated list of chess games
- Support simple source/result filtering
- Keep the row design compact and scan-friendly

### Game Detail Shell

Use `GET /api/v1/chess/games/<game_id>`.

Goal:

- Show the high-level game metadata
- Indicate whether PGN and moves exist
- Leave room for richer detail in later backend versions

### Courses List

Use `GET /api/v1/chess/courses`.

Goal:

- Show a paginated list of chess courses
- Support category and status filtering
- Keep the row design compact and scan-friendly

## Suggested Swift Models

These are the first model shapes to plan around in the future native client.

```swift
struct ApiHealthResponse: Decodable {
    let ok: Bool
    let service: String
    let apiVersion: String
}
```

```swift
struct ApiMeResponse: Decodable {
    let ok: Bool
    let authenticated: Bool
    let production: Bool
}
```

```swift
struct ChessHomeResponse: Decodable {
    let ok: Bool
    let section: String
    let title: String
    let available: Bool
    let summary: ChessHomeSummary
    let nextActions: [ChessNextAction]
}

struct ChessHomeSummary: Decodable {
    let gamesCount: Int
    let profilesCount: Int
    let coursesCount: Int
    let trainingAvailable: Bool
}

struct ChessNextAction: Decodable {
    let key: String
    let label: String
}
```

```swift
struct ChessGameListResponse: Decodable {
    let ok: Bool
    let section: String
    let items: [ChessGameSummary]
    let count: Int
    let limit: Int
    let offset: Int
}
```

```swift
struct ChessGameSummary: Decodable, Identifiable {
    let id: String
    let source: String
    let white: String
    let black: String
    let userColor: String
    let userResult: String
    let result: String
    let date: String
    let timeClass: String
    let opening: ChessOpeningSummary
}

struct ChessOpeningSummary: Decodable {
    let name: String
    let eco: String
}
```

```swift
struct ChessGameDetailResponse: Decodable {
    let ok: Bool
    let section: String
    let item: ChessGameDetailItem
}

struct ChessGameDetailItem: Decodable, Identifiable {
    let id: String
    let source: String
    let white: String
    let black: String
    let userColor: String
    let userResult: String
    let result: String
    let date: String
    let timeClass: String
    let timeControl: String
    let opening: ChessOpeningDetail
    let url: String
    let rated: Bool
    let pgnAvailable: Bool
    let movesAvailable: Bool
}

struct ChessOpeningDetail: Decodable {
    let name: String
    let eco: String
    let variation: String
}
```

## Planned Next Backend Endpoints

These are not implemented yet, but they are the natural next steps for the native client:

- `GET /api/v1/chess/puzzles`

## Implementation Notes

- The backend should continue to favor isolated projection code for iOS-facing responses.
- The native client should treat all responses as read-only.
- Future versions can add richer chess detail, but V1 should stay intentionally small and safe.
