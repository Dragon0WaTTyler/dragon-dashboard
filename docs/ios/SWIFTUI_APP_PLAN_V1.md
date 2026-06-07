# Dragon SwiftUI App Plan V1

This document outlines the future native iOS client for Dragon.

The app is planned as a native SwiftUI client.
It is not a WebView app.
It is not Capacitor.
It is not React Native.
It is not Flutter.

The public read-only iOS API V1 already exists and is the backend contract for this plan.

## App Identity

Dragon iOS V1 should be a clean, native SwiftUI app with a small and stable first surface.

Core identity:

- Native SwiftUI app
- Source-driven
- Read-only at first
- Lightweight and deterministic
- Built around the public API V1 endpoints

## V1 App Screens

The first SwiftUI app should focus on these screens:

1. Launch / API Health Check
2. Chess Home
3. Games List
4. Game Detail Shell
5. Train Today
6. Openings
7. Courses
8. Progress

## Navigation Structure

Recommended first structure:

- `TabView` for the initial app shell
- Or a sidebar-style layout if the app later targets iPad and macOS-style navigation patterns

Suggested tabs for the first pass:

- Home
- Games
- Train
- Openings
- Courses
- Progress

Recommended first implementation order:

1. Launch / API Health Check
2. Chess Home
3. Games List
4. Game Detail Shell
5. Train Today
6. Openings
7. Courses
8. Progress

This order keeps the first experience simple and lets later screens reuse the same API client and data models.

## API Client Plan

Use a dedicated client type:

- `DragonAPIClient`

Planned responsibilities:

- Store `baseURL`
- Perform async/await networking
- Decode JSON responses
- Translate transport and decoding failures into small user-facing errors
- Keep request logic isolated from UI code

Recommended client shape:

- `baseURL` should be configurable
- Use `URLSession`
- Use `async` / `await`
- Prefer one request method per endpoint family only when needed
- Keep decoding strict enough to catch contract drift early

Simple error handling should cover:

- Network unavailable
- Server unreachable
- Invalid response
- Decoding failure
- Unexpected HTTP status

## Swift Model Names

The first Swift model set should mirror the public API V1 contract.

Planned models:

- `ApiHealthResponse`
- `ApiMeResponse`
- `ChessHomeResponse`
- `ChessGameListResponse`
- `ChessGameSummary`
- `ChessGameDetailResponse`
- `ChessTrainingResponse`
- `ChessOpeningListResponse`
- `ChessCourseListResponse`
- `ChessProgressResponse`

These names should map directly to the JSON returned by the API so the UI layer remains simple.

## Loading and Error States

Every screen should handle a small set of consistent states.

Loading states:

- Initial loading
- Refresh loading

Empty states:

- No games
- No training items
- No openings
- No courses
- Zero progress data

Connectivity and server states:

- Offline or unreachable
- Invalid response
- Unexpected status code

UI behavior should stay calm and clear. The app should never require the user to understand backend details to recover from a problem.

## API Endpoints Used by V1

The SwiftUI app should start by using these public endpoints:

- `GET /api/v1/health`
- `GET /api/v1/me`
- `GET /api/v1/chess/home`
- `GET /api/v1/chess/games`
- `GET /api/v1/chess/games/<game_id>`
- `GET /api/v1/chess/train-today`
- `GET /api/v1/chess/openings`
- `GET /api/v1/chess/courses`
- `GET /api/v1/chess/progress`

## Screen-by-Screen Plan

### Launch / API Health Check

Purpose:

- Verify the API is reachable
- Establish the app’s basic online/offline state

Uses:

- `GET /api/v1/health`

### Chess Home

Purpose:

- Present the main entry point into Lotus Chess
- Show a small summary and next actions

Uses:

- `GET /api/v1/chess/home`

### Games List

Purpose:

- Show a paginated chess games list
- Support simple source/result filtering later if needed

Uses:

- `GET /api/v1/chess/games`

### Game Detail Shell

Purpose:

- Show high-level metadata for a selected game
- Indicate whether richer content exists without exposing it yet

Uses:

- `GET /api/v1/chess/games/<game_id>`

### Train Today

Purpose:

- Show the current safe training recommendation list

Uses:

- `GET /api/v1/chess/train-today`

### Openings

Purpose:

- Show an opening summary list
- Help users understand what is repeated, weak, or important

Uses:

- `GET /api/v1/chess/openings`

### Courses

Purpose:

- Show a curated course list
- Keep courses separate from training data

Uses:

- `GET /api/v1/chess/courses`

### Progress

Purpose:

- Show a compact chess progress snapshot
- Provide a high-level summary without exposing sensitive data

Uses:

- `GET /api/v1/chess/progress`

## Explicit Non-Goals

The first SwiftUI version should not include:

- Write actions yet
- Offline database yet
- Push notifications yet
- Stockfish local engine yet
- Movie playback
- Magnet runtime
- WebView fallback

These can be considered later only after the read-only client is stable.

## First Xcode Session Checklist

When Xcode becomes available, the first implementation session should do the following:

1. Create the SwiftUI project
2. Set a placeholder bundle identifier
3. Add the API client
4. Add the models
5. Implement Chess Home first
6. Then implement Games List
7. Then implement Game Detail Shell

That gives the app an immediate usable core without overcommitting to future features.

## Testing Checklist

The first native iOS verification pass should confirm:

- Simulator can reach the production API
- Health endpoint works
- Chess home loads
- Games list paginates with `limit=50`
- Detail screen handles missing game
- Progress screen displays counts

## Implementation Notes

- Keep the UI small and direct.
- Reuse the API contract rather than inventing new shapes in the app.
- Keep networking and decoding isolated from SwiftUI views.
- Treat the backend as read-only for V1.
- Add richer capabilities only after the first stable native slice is working.
