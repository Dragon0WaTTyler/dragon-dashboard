import threading
import time


class WatchLaterSyncService:
    def __init__(
        self,
        *,
        runtime_cache,
        runtime_cache_lock,
        current_timestamp,
        is_watchlater_playlist,
        is_stale_snapshot,
        fetch_remote_marker,
        refresh_cooldown_seconds=30,
        marker_check_cooldown_seconds=300,
    ):
        self.runtime_cache = runtime_cache
        self.runtime_cache_lock = runtime_cache_lock
        self.current_timestamp = current_timestamp
        self.is_watchlater_playlist = is_watchlater_playlist
        self.is_stale_snapshot = is_stale_snapshot
        self.fetch_remote_marker = fetch_remote_marker
        self.refresh_cooldown_seconds = max(float(refresh_cooldown_seconds or 0.0), 0.0)
        self.marker_check_cooldown_seconds = max(float(marker_check_cooldown_seconds or 0.0), 0.0)

    def owns_playlist(self, playlist_id):
        playlist_value = str(playlist_id or "").strip()
        return bool(playlist_value) and bool(self.is_watchlater_playlist(playlist_value))

    def build_snapshot_marker(self, videos):
        first = videos[0] if isinstance(videos, list) and videos else {}
        if not isinstance(first, dict):
            first = {}
        playlist_item_id = str(first.get("playlist_item_id", "") or "").strip()
        video_id = str(first.get("video_id", "") or "").strip()
        published_at = str(first.get("published_at", "") or "").strip()
        if not (playlist_item_id or video_id or published_at):
            return ""
        return "|".join([playlist_item_id, video_id, published_at])

    def get_latest_remote_marker(self, playlist_id):
        try:
            marker_payload = self.fetch_remote_marker(str(playlist_id or "").strip()) or {}
        except Exception as exc:
            marker_payload = {
                "error": type(exc).__name__,
                "marker": "",
                "fetched_at": self.current_timestamp(),
            }
        if not isinstance(marker_payload, dict):
            marker_payload = {}
        playlist_item_id = str(marker_payload.get("playlist_item_id", "") or "").strip()
        video_id = str(marker_payload.get("video_id", "") or "").strip()
        published_at = str(marker_payload.get("published_at", "") or "").strip()
        marker_payload["playlist_item_id"] = playlist_item_id
        marker_payload["video_id"] = video_id
        marker_payload["published_at"] = published_at
        marker_payload["marker"] = str(marker_payload.get("marker", "") or "").strip() or "|".join(
            [playlist_item_id, video_id, published_at]
        ).strip("|")
        marker_payload["fetched_at"] = str(marker_payload.get("fetched_at", "") or "").strip() or self.current_timestamp()
        return marker_payload

    def should_allow_background_refresh(self, playlist_id, trigger="interactive_read"):
        cache_key = self._cache_key(playlist_id)
        now_value = time.monotonic()
        with self.runtime_cache_lock:
            meta = self._meta_for_update(cache_key)
            if meta.get("refresh_inflight"):
                self._log("refresh coalesced", playlist_id=playlist_id, reason="inflight", trigger=trigger)
                return False
            last_started_at = float(meta.get("last_refresh_started_at_monotonic", 0.0) or 0.0)
            if last_started_at and (now_value - last_started_at) < self.refresh_cooldown_seconds:
                self._log("refresh skipped", playlist_id=playlist_id, reason="cooldown", trigger=trigger)
                return False
        self._log("incremental refresh allowed", playlist_id=playlist_id, trigger=trigger)
        return True

    def should_refresh(self, playlist_id, snapshot_entry, cached_videos, trigger="interactive_read"):
        if not self.owns_playlist(playlist_id):
            return {"should_refresh": False, "reason": "not_watchlater"}
        if not isinstance(cached_videos, list) or not cached_videos:
            return {"should_refresh": True, "reason": "missing_snapshot"}
        if not self.is_stale_snapshot(snapshot_entry):
            self._log("refresh skipped", playlist_id=playlist_id, reason="fresh_snapshot", trigger=trigger)
            return {"should_refresh": False, "reason": "fresh_snapshot"}
        if not self.should_allow_background_refresh(playlist_id, trigger=trigger):
            self._log("incremental refresh blocked", playlist_id=playlist_id, reason="gated", trigger=trigger)
            return {"should_refresh": False, "reason": "gated"}

        cache_key = self._cache_key(playlist_id)
        now_value = time.monotonic()
        with self.runtime_cache_lock:
            meta = self._meta_for_update(cache_key)
            last_marker_check_at = float(meta.get("last_marker_check_at_monotonic", 0.0) or 0.0)
            if last_marker_check_at and (now_value - last_marker_check_at) < self.marker_check_cooldown_seconds:
                self._log("refresh skipped", playlist_id=playlist_id, reason="marker_check_cooldown", trigger=trigger)
                return {"should_refresh": False, "reason": "marker_check_cooldown"}

        remote_marker = self.get_latest_remote_marker(playlist_id)
        snapshot_marker = self.build_snapshot_marker(cached_videos)
        remote_marker_value = str(remote_marker.get("marker", "") or "").strip()
        marker_error = str(remote_marker.get("error", "") or "").strip()
        with self.runtime_cache_lock:
            meta = self._meta_for_update(cache_key)
            meta["last_marker_check_at"] = remote_marker.get("fetched_at", "")
            meta["last_marker_check_at_monotonic"] = now_value
            meta["last_remote_marker"] = remote_marker_value
            meta["last_marker_error"] = marker_error
        if marker_error:
            self._log("incremental refresh allowed", playlist_id=playlist_id, reason="marker_fetch_failed", trigger=trigger)
            return {
                "should_refresh": True,
                "reason": "marker_fetch_failed",
                "remote_marker": remote_marker,
            }
        if remote_marker_value and snapshot_marker and remote_marker_value == snapshot_marker:
            self._log("remote marker unchanged", playlist_id=playlist_id, trigger=trigger)
            return {
                "should_refresh": False,
                "reason": "remote_marker_unchanged",
                "remote_marker": remote_marker,
            }
        self._log("incremental refresh allowed", playlist_id=playlist_id, reason="remote_marker_changed", trigger=trigger)
        return {
            "should_refresh": True,
            "reason": "remote_marker_changed" if remote_marker_value else "remote_marker_missing",
            "remote_marker": remote_marker,
        }

    def schedule_refresh_from_snapshot(self, playlist_id, snapshot_entry, cached_videos, refresh_fn, *, trigger="interactive_read"):
        decision = self.should_refresh(
            playlist_id,
            snapshot_entry=snapshot_entry,
            cached_videos=cached_videos,
            trigger=trigger,
        )
        return self.schedule_refresh_from_decision(
            playlist_id,
            decision,
            refresh_fn,
            trigger=trigger,
            refresh_reason=str(decision.get("reason", "") or "incremental"),
        )

    def schedule_refresh_from_decision(self, playlist_id, decision, refresh_fn, *, trigger="interactive_read", refresh_reason="incremental"):
        if not self.owns_playlist(playlist_id):
            return False
        if not isinstance(decision, dict) or not decision.get("should_refresh"):
            return False
        cache_key = self._cache_key(playlist_id)
        now_value = time.monotonic()
        with self.runtime_cache_lock:
            meta = self._meta_for_update(cache_key)
            if meta.get("refresh_inflight"):
                self._log("refresh coalesced", playlist_id=playlist_id, reason="inflight", trigger=trigger)
                return False
            last_started_at = float(meta.get("last_refresh_started_at_monotonic", 0.0) or 0.0)
            if last_started_at and (now_value - last_started_at) < self.refresh_cooldown_seconds:
                self._log("refresh skipped", playlist_id=playlist_id, reason="cooldown", trigger=trigger)
                return False
            meta["refresh_inflight"] = True
            meta["last_refresh_started_at"] = self.current_timestamp()
            meta["last_refresh_started_at_monotonic"] = now_value
            meta["last_refresh_reason"] = str(refresh_reason or "").strip() or "incremental"
            meta["last_refresh_trigger"] = str(trigger or "").strip() or "interactive_read"

        def _runner():
            try:
                refresh_fn()
            finally:
                with self.runtime_cache_lock:
                    meta = self._meta_for_update(cache_key)
                    meta["refresh_inflight"] = False
                    meta["last_refresh_completed_at"] = self.current_timestamp()

        threading.Thread(target=_runner, daemon=True).start()
        return True

    def record_snapshot_update(self, playlist_id, videos):
        if not self.owns_playlist(playlist_id):
            return
        cache_key = self._cache_key(playlist_id)
        marker = self.build_snapshot_marker(videos)
        with self.runtime_cache_lock:
            meta = self._meta_for_update(cache_key)
            meta["last_snapshot_updated_at"] = self.current_timestamp()
            meta["last_snapshot_marker"] = marker

    def _cache_key(self, playlist_id):
        return f"watchlater:{str(playlist_id or '').strip()}"

    def _meta_for_update(self, cache_key):
        sync_meta = self.runtime_cache.setdefault("watchlater_sync_meta", {})
        return sync_meta.setdefault(cache_key, {})

    def _log(self, message, **fields):
        parts = [f"[watchlater-sync] {message}"]
        for key, value in fields.items():
            if value in ("", None):
                continue
            parts.append(f"{key}={value}")
        print(" ".join(parts))
