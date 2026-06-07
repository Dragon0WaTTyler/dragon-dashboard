# Local Movie Runtime V0

## 1. Status

Movie Runtime V0.2 is local-only.

It works on a local PC for tested MP4 magnet sources.

It must not be treated as a PythonAnywhere production runtime.

## 2. What Works Locally

- Movie detail creates the `/watch` handoff.
- The WebTorrent helper starts locally.
- The selected MP4 is materialized on local disk.
- `/watch` polling refreshes session state.
- MP4 browser readiness waits for head, tail, and the initial playback window.
- Tail bytes are prioritized.
- The Flask stream endpoint returns `206 Partial Content`.
- Chrome and Edge playback works on PC for tested sources.

## 3. What Must Stay Out of PythonAnywhere

- WebTorrent helper.
- Torrent sessions.
- Magnet streaming.
- Background Node runtime.
- Local file materialization.
- Browser stream endpoint as a production media engine.

## 4. Dependencies

- Python Flask app.
- Node.js.
- `npm` package `webtorrent`.
- Local disk cache under `cache/magnets/playback_runtime`.
- PC browser, preferably Chrome or Edge.

## 5. Known Limits

- Torrent metadata can still timeout depending on source and peers.
- Magnet availability depends on peers and trackers.
- Audio may fail for unsupported audio codecs like AC3, EAC3, DTS, or TrueHD.
- The best browser path is MP4 with H264/x264 video and AAC audio.
- No transcoding.
- No subtitles automation.
- No iPhone or Safari target.
- No Debrid in this runtime.

## 6. Current Safeguards

- One metadata retry for magnet sources.
- Readiness guard before redirecting.
- Head, tail, and initial-window readiness.
- Tail writer ENOENT fix.
- Codec risk diagnostics.

## 7. Future Packaging Plan

- Windows EXE later.
- macOS app later.
- The app should run Flask plus the Node/WebTorrent helper locally.
- Hosted Dragon can remain the metadata and control layer.
- The desktop app owns local playback runtime.

## 8. Explicit Non-Goals

- No PythonAnywhere magnet runtime.
- No public torrent streaming service.
- No provider scraping expansion.
- No ffmpeg/transcoding pipeline for V0.
- No iOS magnet runtime.
