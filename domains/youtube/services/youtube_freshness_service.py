from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
import re
import tempfile

POCKETTUBE_GROUP_VIDEO_LIMIT = 200
POCKETTUBE_ALL_FEED_VIDEO_LIMIT = 200
POCKETTUBE_DISPLAY_LIMIT_OPTIONS = (50, 100, 150, 200)
POCKETTUBE_DEFAULT_DISPLAY_LIMIT = 50


class YouTubeFreshnessService:
    def __init__(
        self,
        *,
        load_admin_data,
        pockettube_latest_import_snapshot,
        get_persisted_youtube_channel_latest_entry,
        refresh_pockettube_section_latest_uploads,
        trigger_github_actions_sync,
        refresh_snapshot_from_github,
        build_youtube_channel_video_summary,
        canonical_section_name,
        normalize_pockettube_group_key,
        format_timestamp_label,
        current_timestamp,
        load_json_file,
        save_json_file,
        snapshot_path,
        sync_status_path,
        snapshot_raw_url="",
        sync_status_raw_url="",
        requests_module=None,
        registry_path=None,
        app_logger,
        refresh_service=None,
    ):
        self.load_admin_data = load_admin_data
        self.pockettube_latest_import_snapshot = pockettube_latest_import_snapshot
        self.get_persisted_youtube_channel_latest_entry = get_persisted_youtube_channel_latest_entry
        self.refresh_pockettube_section_latest_uploads = refresh_pockettube_section_latest_uploads
        self.trigger_github_actions_sync = trigger_github_actions_sync
        self.refresh_snapshot_from_github = refresh_snapshot_from_github
        self.build_youtube_channel_video_summary = build_youtube_channel_video_summary
        self.canonical_section_name = canonical_section_name
        self.normalize_pockettube_group_key = normalize_pockettube_group_key
        self.format_timestamp_label = format_timestamp_label
        self.current_timestamp = current_timestamp
        self.load_json_file = load_json_file
        self.save_json_file = save_json_file
        self.snapshot_path = Path(snapshot_path)
        self.sync_status_path = Path(sync_status_path)
        self.snapshot_raw_url = str(snapshot_raw_url or "").strip()
        self.sync_status_raw_url = str(sync_status_raw_url or "").strip()
        self.requests_module = requests_module
        self.registry_path = Path(registry_path) if registry_path else Path(__file__).resolve().parents[1] / "data" / "pockettube_registry.json"
        self.app_logger = app_logger
        self.refresh_service = refresh_service
        if self.refresh_service is None:
            from domains.shared.refresh import RefreshService

            self.refresh_service = RefreshService(format_timestamp_label=self.format_timestamp_label)

    def empty_snapshot(self):
        return {
            "version": 2,
            "generated_at": "",
            "synced_at": "",
            "group_video_limit": POCKETTUBE_GROUP_VIDEO_LIMIT,
            "all_feed_video_limit": POCKETTUBE_ALL_FEED_VIDEO_LIMIT,
            "warnings": [],
            "groups": {},
            "channels": {},
            "errors": [],
        }

    def empty_sync_status(self):
        return {
            "status": "idle",
            "requested_at": "",
            "started_at": "",
            "completed_at": "",
            "last_error": "",
            "warnings": [],
            "scope": "",
            "run_id": "",
            "run_url": "",
            "source": "",
            "updated_at": "",
        }

    def load_snapshot(self):
        payload = self.load_json_file(self.snapshot_path, self.empty_snapshot())
        return self._normalize_snapshot(payload)

    def save_snapshot(self, snapshot):
        payload = self.finalize_snapshot(snapshot)
        self.save_json_file(self.snapshot_path, payload)
        return payload

    def load_sync_status(self):
        payload = self.load_json_file(self.sync_status_path, self.empty_sync_status())
        return self._normalize_sync_status(payload)

    def save_sync_status(self, status):
        payload = self._normalize_sync_status(status)
        self.save_json_file(self.sync_status_path, payload)
        return payload

    def _load_remote_json_object(self, url, label):
        if not url:
            raise RuntimeError(f"GitHub raw URL missing for {label}.")
        if self.requests_module is None:
            raise RuntimeError(f"HTTP client unavailable for {label}.")

        try:
            response = self.requests_module.get(url, timeout=15)
        except self.requests_module.RequestException as exc:
            raise RuntimeError(f"Download failed for {label}: {exc}") from exc

        status_code = getattr(response, "status_code", None)
        if status_code != 200:
            raise RuntimeError(f"Download failed for {label}: GitHub returned status {status_code}")

        raw_text = response.text
        if not isinstance(raw_text, str) or not raw_text.strip():
            raise RuntimeError(f"Download failed for {label}: response was empty")

        try:
            payload = json.loads(raw_text)
        except Exception as exc:
            raise RuntimeError(f"Download failed for {label}: invalid JSON ({exc})") from exc

        if not isinstance(payload, dict) or not payload:
            raise RuntimeError(f"Download failed for {label}: payload must be a non-empty object")
        return payload

    def _stage_json_atomic_payload(self, destination_path, payload, *, normalizer):
        destination_path = Path(destination_path)
        normalized_payload = normalizer(payload)
        if not isinstance(normalized_payload, dict) or not normalized_payload:
            raise RuntimeError(f"Refusing to replace {destination_path.name}: normalized payload was invalid")

        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            delete=False,
            dir=str(destination_path.parent),
            prefix=f"{destination_path.stem}.",
            suffix=".tmp",
        ) as temp_file:
            temp_path = Path(temp_file.name)
            json.dump(normalized_payload, temp_file, indent=2, ensure_ascii=False)
            temp_file.flush()
        return normalized_payload, temp_path

    def refresh_local_snapshot_from_github(self):
        snapshot_payload = self._load_remote_json_object(self.snapshot_raw_url, "youtube_latest_snapshot.json")
        sync_status_payload = self._load_remote_json_object(self.sync_status_raw_url, "youtube_latest_sync_status.json")
        snapshot_temp_path = None
        sync_status_temp_path = None
        try:
            saved_snapshot, snapshot_temp_path = self._stage_json_atomic_payload(
                self.snapshot_path,
                snapshot_payload,
                normalizer=self.finalize_snapshot,
            )
            saved_sync_status, sync_status_temp_path = self._stage_json_atomic_payload(
                self.sync_status_path,
                sync_status_payload,
                normalizer=self._normalize_sync_status,
            )
            snapshot_temp_path.replace(self.snapshot_path)
            sync_status_temp_path.replace(self.sync_status_path)
        finally:
            for temp_path in (snapshot_temp_path, sync_status_temp_path):
                if temp_path is not None and temp_path.exists():
                    try:
                        temp_path.unlink()
                    except Exception:
                        pass
        return {
            "ok": True,
            "status": "updated",
            "snapshot_path": str(self.snapshot_path),
            "sync_status_path": str(self.sync_status_path),
            "group_count": len(saved_snapshot.get("groups", {}) or {}),
            "channel_count": len(saved_snapshot.get("channels", {}) or {}),
            "sync_state": str(saved_sync_status.get("status", "") or "").strip(),
        }

    def _iter_snapshot_latest_videos(self, snapshot):
        snapshot = snapshot if isinstance(snapshot, dict) else self.empty_snapshot()
        groups = snapshot.get("groups", {})
        if isinstance(groups, dict):
            for group_key, group in groups.items():
                if not isinstance(group, dict):
                    continue
                group_name = str(group.get("group_name", "") or group.get("section_name", "") or group_key or "").strip()
                group_key_text = str(group.get("group_key", "") or group_key or "").strip()
                group_videos = [video for video in (group.get("videos", []) or []) if isinstance(video, dict)]
                for video in group_videos:
                    normalized_video = self._normalize_group_video(video, group_name, group_key_text, prefer_snapshot_fields=True)
                    if not normalized_video:
                        continue
                    yield {
                        "source": "group_video",
                        "group_key": group_key_text,
                        "group_name": group_name,
                        "group_names": list(dict.fromkeys([
                            str(name or "").strip()
                            for name in list(normalized_video.get("group_names", []) or []) + ([group_name] if group_name else [])
                            if str(name or "").strip()
                        ])),
                        "group_keys": list(dict.fromkeys([
                            str(key or "").strip()
                            for key in list(normalized_video.get("group_keys", []) or []) + ([group_key_text] if group_key_text else [])
                            if str(key or "").strip()
                        ])),
                        "channel_id": str(normalized_video.get("channel_id", "") or "").strip(),
                        "channel_title": str(normalized_video.get("channel_title", "") or normalized_video.get("channel_name", "") or "").strip(),
                        "latest_video": dict(normalized_video),
                    }
                channels = [channel for channel in (group.get("channels", []) or []) if isinstance(channel, dict)]
                for channel in channels:
                    latest_video = channel.get("latest_video", {})
                    if not isinstance(latest_video, dict) or not latest_video:
                        continue
                    yield {
                        "source": "group_channel",
                        "group_key": group_key_text,
                        "group_name": group_name,
                        "group_names": list(dict.fromkeys([
                            str(name or "").strip()
                            for name in list(channel.get("group_names", []) or []) + ([group_name] if group_name else [])
                            if str(name or "").strip()
                        ])),
                        "group_keys": list(dict.fromkeys([
                            str(key or "").strip()
                            for key in list(channel.get("group_keys", []) or []) + ([group_key_text] if group_key_text else [])
                            if str(key or "").strip()
                        ])),
                        "channel_id": str(channel.get("channel_id", "") or "").strip(),
                        "channel_title": str(channel.get("channel_title", "") or "").strip(),
                        "latest_video": dict(latest_video),
                    }

        channels = snapshot.get("channels", {})
        if isinstance(channels, dict):
            for channel_id, channel in channels.items():
                if not isinstance(channel, dict):
                    continue
                latest_video = channel.get("latest_video", {})
                if not isinstance(latest_video, dict) or not latest_video:
                    continue
                yield {
                    "source": "channel",
                    "group_key": "",
                    "group_name": "",
                    "group_names": list(dict.fromkeys([
                        str(name or "").strip()
                        for name in list(channel.get("group_names", []) or [])
                        if str(name or "").strip()
                    ])),
                    "group_keys": [],
                    "channel_id": str(channel.get("channel_id", "") or channel_id or "").strip(),
                    "channel_title": str(channel.get("channel_title", "") or "").strip(),
                    "latest_video": dict(latest_video),
                }

    def _snapshot_latest_video_matches(self, latest_video, lookup_entry_id="", lookup_video_id="", lookup_watch_key=""):
        latest_video = latest_video if isinstance(latest_video, dict) else {}
        candidate_entry_id = str(latest_video.get("entry_id", "") or "").strip()
        candidate_video_id = str(latest_video.get("video_id", "") or "").strip()
        candidate_watch_key = str(latest_video.get("watch_key", "") or "").strip()
        return any(
            candidate
            and candidate in {
                str(lookup_entry_id or "").strip(),
                str(lookup_video_id or "").strip(),
                str(lookup_watch_key or "").strip(),
            }
            for candidate in (candidate_entry_id, candidate_video_id, candidate_watch_key)
        )

    def _snapshot_title_tokens(self, value):
        raw_tokens = re.findall(r"[a-z0-9]+", str(value or "").lower())
        stop_words = {
            "the",
            "and",
            "for",
            "with",
            "from",
            "this",
            "that",
            "video",
            "channel",
            "news",
            "latest",
            "pockettube",
        }
        return {token for token in raw_tokens if len(token) > 2 and token not in stop_words}

    def _snapshot_video_pool(self, snapshot):
        pool = {}
        for candidate in self._iter_snapshot_latest_videos(snapshot):
            latest_video = candidate.get("latest_video", {})
            if not isinstance(latest_video, dict) or not latest_video:
                continue
            summary = self.build_youtube_channel_video_summary(latest_video)
            if not isinstance(summary, dict):
                continue
            video_id = str(summary.get("video_id", "") or "").strip()
            if not video_id:
                continue
            entry = pool.setdefault(video_id, dict(summary))
            entry["source_type"] = "youtube"
            entry["entry_type"] = "youtube"
            entry_id = str(entry.get("entry_id", "") or summary.get("entry_id", "") or summary.get("id", "") or "").strip()
            if not entry_id:
                entry_id = f"yt-{video_id}"
            entry["id"] = str(entry.get("id", "") or entry_id).strip() or entry_id
            entry["entry_id"] = entry_id
            entry["watch_key"] = str(entry.get("watch_key", "") or summary.get("watch_key", "") or video_id or entry_id).strip()
            entry["state_key"] = str(entry.get("state_key", "") or summary.get("state_key", "") or entry["watch_key"] or video_id or entry_id).strip()
            entry["detail_url"] = str(entry.get("detail_url", "") or summary.get("detail_url", "") or "").strip() or f"/video/{entry_id}"
            entry["url"] = str(entry.get("url", "") or summary.get("url", "") or "").strip()
            entry["thumb"] = str(entry.get("thumb", "") or summary.get("thumb", "") or "").strip()
            entry["thumbnail_url"] = str(entry.get("thumbnail_url", "") or summary.get("thumbnail_url", "") or "").strip()
            entry["thumbnail"] = str(entry.get("thumbnail", "") or summary.get("thumbnail", "") or entry["thumbnail_url"] or "").strip()
            entry["image_url"] = str(entry.get("image_url", "") or summary.get("image_url", "") or entry["thumbnail_url"] or "").strip()
            entry["channel_id"] = str(entry.get("channel_id", "") or summary.get("channel_id", "") or "").strip()
            entry["channel_name"] = str(entry.get("channel_name", "") or summary.get("channel_name", "") or entry.get("channel_title", "") or "").strip()
            entry["channel_title"] = str(entry.get("channel_title", "") or summary.get("channel_title", "") or entry.get("channel_name", "") or "").strip()
            entry["published_at"] = str(entry.get("published_at", "") or summary.get("published_at", "") or "").strip()
            entry["published_display"] = str(entry.get("published_display", "") or summary.get("published_display", "") or "").strip()
            entry["group_names"] = list(dict.fromkeys([
                str(name or "").strip()
                for name in list(entry.get("group_names", []) or [])
                + list(candidate.get("group_names", []) or [])
                + ([candidate.get("group_name", "")] if candidate.get("group_name", "") else [])
                if str(name or "").strip()
            ]))
            entry["group_keys"] = list(dict.fromkeys([
                str(key or "").strip()
                for key in list(entry.get("group_keys", []) or [])
                + list(candidate.get("group_keys", []) or [])
                + ([candidate.get("group_key", "")] if candidate.get("group_key", "") else [])
                if str(key or "").strip()
            ]))
            if not str(entry.get("group_name", "") or "").strip():
                entry["group_name"] = str(candidate.get("group_name", "") or "").strip()
            if not str(entry.get("group_key", "") or "").strip():
                entry["group_key"] = str(candidate.get("group_key", "") or "").strip()
            if not str(entry.get("watch_key", "") or "").strip():
                entry["watch_key"] = str(summary.get("watch_key", "") or "").strip()
            if not str(entry.get("state_key", "") or "").strip():
                entry["state_key"] = str(summary.get("state_key", "") or entry.get("watch_key", "") or "").strip()
            entry["published_sort"] = self._published_sort_key(entry.get("published_at", ""))
            entry["title_tokens"] = self._snapshot_title_tokens(entry.get("title", ""))
            entry["has_thumbnail"] = bool(str(entry.get("thumbnail", "") or entry.get("thumbnail_url", "") or entry.get("image_url", "") or "").strip())
            entry["has_title"] = bool(str(entry.get("title", "") or "").strip())
        return pool

    def _snapshot_related_video_score(self, candidate, current_video_id, current_channel_id, current_group_names, current_group_keys, current_title_tokens, current_group_name="", current_group_key=""):
        candidate = candidate if isinstance(candidate, dict) else {}
        candidate_video_id = str(candidate.get("video_id", "") or "").strip()
        if not candidate_video_id or candidate_video_id == current_video_id:
            return None
        candidate_channel_id = str(candidate.get("channel_id", "") or "").strip()
        candidate_group_name = str(candidate.get("group_name", "") or "").strip().lower()
        candidate_group_key = str(candidate.get("group_key", "") or "").strip().lower()
        candidate_group_names = {str(name or "").strip().lower() for name in candidate.get("group_names", []) or [] if str(name or "").strip()}
        candidate_group_keys = {str(key or "").strip().lower() for key in candidate.get("group_keys", []) or [] if str(key or "").strip()}
        current_group_names = {str(name or "").strip().lower() for name in (current_group_names or []) if str(name or "").strip()}
        current_group_keys = {str(key or "").strip().lower() for key in (current_group_keys or []) if str(key or "").strip()}
        current_group_name = str(current_group_name or "").strip().lower()
        current_group_key = str(current_group_key or "").strip().lower()
        same_primary_group = bool(
            (current_group_key and current_group_key == candidate_group_key)
            or (current_group_name and current_group_name == candidate_group_name)
        )
        same_channel = bool(current_channel_id and candidate_channel_id == str(current_channel_id or "").strip())
        shared_group_tags = bool((candidate_group_names | candidate_group_keys) & (current_group_names | current_group_keys))
        title_overlap = len(set(candidate.get("title_tokens", []) or []) & set(current_title_tokens or []))
        return (
            1 if same_primary_group else 0,
            1 if same_channel else 0,
            1 if shared_group_tags else 0,
            int(candidate.get("published_sort", 0) or 0),
            title_overlap,
            1 if bool(candidate.get("has_thumbnail")) else 0,
            1 if bool(candidate.get("has_title")) else 0,
        )

    def _build_snapshot_related_entries(self, snapshot, *, current_latest_video, current_group_name="", current_group_key="", current_channel_id="", limit=12):
        pool = self._snapshot_video_pool(snapshot)
        current_latest_video = current_latest_video if isinstance(current_latest_video, dict) else {}
        current_video_id = str(current_latest_video.get("video_id", "") or "").strip()
        current_title = str(current_latest_video.get("title", "") or current_latest_video.get("name", "") or "").strip()
        current_title_tokens = self._snapshot_title_tokens(current_title)
        current_group_names = list(dict.fromkeys([
            str(name or "").strip()
            for name in list(current_latest_video.get("group_names", []) or []) + ([current_group_name] if current_group_name else [])
            if str(name or "").strip()
        ]))
        current_group_keys = list(dict.fromkeys([
            str(key or "").strip()
            for key in list(current_latest_video.get("group_keys", []) or []) + ([current_group_key] if current_group_key else [])
            if str(key or "").strip()
        ]))
        ranked_candidates = []
        for candidate in pool.values():
            score = self._snapshot_related_video_score(
                candidate,
                current_video_id,
                current_channel_id,
                current_group_names,
                current_group_keys,
                current_title_tokens,
                current_group_name=current_group_name,
                current_group_key=current_group_key,
            )
            if score is None:
                continue
            candidate_copy = dict(candidate)
            candidate_copy["detail_url"] = str(candidate_copy.get("detail_url", "") or "").strip() or f"/video/{candidate_copy.get('entry_id', '')}"
            candidate_copy["url"] = str(candidate_copy.get("url", "") or "").strip()
            candidate_copy["group_names"] = list(dict.fromkeys([str(name or "").strip() for name in candidate_copy.get("group_names", []) or [] if str(name or "").strip()]))
            candidate_copy["group_keys"] = list(dict.fromkeys([str(key or "").strip() for key in candidate_copy.get("group_keys", []) or [] if str(key or "").strip()]))
            ranked_candidates.append((score, candidate_copy))
        ranked_candidates.sort(
            key=lambda item: (
                -item[0][0],
                -item[0][1],
                -item[0][2],
                -item[0][3],
                -item[0][4],
                -item[0][5],
                -item[0][6],
                str(item[1].get("title", "") or "").lower(),
                str(item[1].get("video_id", "") or "").lower(),
            )
        )
        related_entries = []
        seen_video_ids = set()
        for _score, candidate in ranked_candidates:
            video_id = str(candidate.get("video_id", "") or "").strip()
            if not video_id or video_id in seen_video_ids:
                continue
            seen_video_ids.add(video_id)
            related_entries.append(candidate)
            if len(related_entries) >= max(int(limit or 12), 1):
                break
        if len(related_entries) < max(int(limit or 12), 1):
            fallback_candidates = [
                candidate
                for candidate in sorted(
                    pool.values(),
                    key=lambda item: (
                        -int(item.get("published_sort", 0) or 0),
                        str(item.get("title", "") or "").lower(),
                        str(item.get("video_id", "") or "").lower(),
                    ),
                )
                if str(candidate.get("video_id", "") or "").strip() and str(candidate.get("video_id", "") or "").strip() != current_video_id
            ]
            for candidate in fallback_candidates:
                video_id = str(candidate.get("video_id", "") or "").strip()
                if not video_id or video_id in seen_video_ids:
                    continue
                related_entries.append(dict(candidate))
                seen_video_ids.add(video_id)
                if len(related_entries) >= max(int(limit or 12), 1):
                    break
        return related_entries

    def _build_snapshot_video_detail_context(self, latest_video, *, lookup_entry_id="", group_name="", group_key="", channel_id="", channel_title=""):
        latest_video = latest_video if isinstance(latest_video, dict) else {}
        video_id = str(latest_video.get("video_id", "") or "").strip()
        entry_id = str(latest_video.get("entry_id", "") or lookup_entry_id or "").strip()
        if not entry_id and video_id:
            entry_id = f"yt-{video_id}"
        watch_key = str(latest_video.get("watch_key", "") or video_id or entry_id or "").strip()
        published_at = str(latest_video.get("published_at", "") or "").strip()
        title = str(latest_video.get("title", "") or latest_video.get("name", "") or channel_title or "Untitled video").strip() or "Untitled video"
        resolved_channel_name = str(
            latest_video.get("channel_name", "")
            or latest_video.get("channel_title", "")
            or channel_title
            or "Unknown Channel"
        ).strip() or "Unknown Channel"
        detail = dict(latest_video)
        detail.update({
            "id": entry_id,
            "entry_id": entry_id,
            "title": title,
            "name": detail.get("name", "") or title,
            "video_id": video_id,
            "watch_key": watch_key,
            "state_key": watch_key,
            "url": str(detail.get("url", "") or "").strip() or (f"https://www.youtube.com/watch?v={video_id}" if video_id else ""),
            "detail_url": str(detail.get("detail_url", "") or "").strip() or (f"/video/{entry_id}" if entry_id else ""),
            "thumb": str(detail.get("thumb", "") or "").strip() or str(detail.get("thumbnail", "") or detail.get("thumbnail_url", "") or detail.get("image_url", "") or "").strip(),
            "thumbnail_url": str(detail.get("thumbnail_url", "") or detail.get("thumbnail", "") or detail.get("image_url", "") or detail.get("thumb", "") or "").strip(),
            "thumbnail": str(detail.get("thumbnail", "") or detail.get("thumbnail_url", "") or detail.get("image_url", "") or detail.get("thumb", "") or "").strip(),
            "image_url": str(detail.get("image_url", "") or detail.get("thumbnail_url", "") or detail.get("thumbnail", "") or detail.get("thumb", "") or "").strip(),
            "channel_id": str(detail.get("channel_id", "") or channel_id or "").strip(),
            "channel_name": resolved_channel_name,
            "channel_title": resolved_channel_name,
            "published_at": published_at,
            "published_display": str(detail.get("published_display", "") or "").strip() or self.format_timestamp_label(published_at, default=""),
            "source_type": "youtube",
            "entry_type": "youtube",
            "playlist_name": "PocketTube Freshness",
            "playlist_url": "/pockettube",
            "playlist_id": str(detail.get("playlist_id", "") or group_key or "").strip(),
            "playlist_item_id": str(detail.get("playlist_item_id", "") or "").strip(),
            "section": "PocketTube",
            "status": str(detail.get("status", "") or "PocketTube Freshness").strip() or "PocketTube Freshness",
            "category": str(detail.get("category", "") or resolved_channel_name or "PocketTube").strip() or "PocketTube",
            "group_name": str(detail.get("group_name", "") or group_name or "").strip(),
            "group_key": str(detail.get("group_key", "") or group_key or "").strip(),
            "group_names": list(dict.fromkeys([
                str(name or "").strip()
                for name in list(detail.get("group_names", []) or []) + ([group_name] if group_name else [])
                if str(name or "").strip()
            ])),
            "group_keys": list(dict.fromkeys([
                str(key or "").strip()
                for key in list(detail.get("group_keys", []) or []) + ([group_key] if group_key else [])
                if str(key or "").strip()
            ])),
            "source_name": str(detail.get("source_name", "") or "PocketTube").strip() or "PocketTube",
            "feed_source": "pockettube_snapshot",
            "group_back_url": "/pockettube",
            "group_back_context": group_name or resolved_channel_name or "PocketTube Freshness",
        })
        related_entries = self._build_snapshot_related_entries(
            self.load_snapshot(),
            current_latest_video=detail,
            current_group_name=group_name,
            current_group_key=group_key,
            current_channel_id=detail.get("channel_id", ""),
            limit=12,
        )
        related_title = group_name or resolved_channel_name or "PocketTube related videos"
        template_keys = [
            "related_entries",
            "related_entries_full",
            "related_videos",
            "related_items",
            "related_title",
            "related_page",
            "related_total_pages",
            "pagination_numbers",
        ]
        self.app_logger.info(
            "pockettube_video_detail_related entry_id=%s video_id=%s groups=%s related_count=%s template_keys=%s",
            entry_id,
            video_id,
            ",".join([
                value
                for value in [
                    str(group_name or "").strip(),
                    str(detail.get("group_name", "") or "").strip(),
                    ",".join([name for name in detail.get("group_names", []) or [] if str(name or "").strip()]),
                ]
                if value
            ]),
            len(related_entries),
            ",".join(template_keys),
        )
        return {
            "entry": detail,
            "entry_type": "youtube",
            "player_video_id": video_id,
            "related_entries": related_entries,
            "related_entries_full": related_entries,
            "related_videos": related_entries,
            "related_items": related_entries,
            "related_title": related_title,
            "playlist_entries": [detail] + related_entries,
            "prev_entry": None,
            "next_entry": None,
            "related_total_pages": 1 if related_entries else 0,
            "related_page": 1,
            "pagination_numbers": [1] if related_entries else [],
            "recommendations": related_entries,
            "entries": related_entries,
            "related_order": "normal",
            "related_seed": "",
            "delete_endpoint": False,
            "ai_default_mode": "study",
            "ai_page_context": "study",
        }

    def build_snapshot_video_detail_context(self, entry_id):
        lookup_entry_id = str(entry_id or "").strip()
        if not lookup_entry_id:
            return None
        lookup_video_id = lookup_entry_id[3:] if lookup_entry_id.startswith("yt-") else lookup_entry_id
        lookup_watch_key = lookup_video_id or lookup_entry_id
        snapshot = self.load_snapshot()
        for candidate in self._iter_snapshot_latest_videos(snapshot):
            latest_video = candidate.get("latest_video", {})
            if not self._snapshot_latest_video_matches(latest_video, lookup_entry_id, lookup_video_id, lookup_watch_key):
                continue
            return self._build_snapshot_video_detail_context(
                latest_video,
                lookup_entry_id=lookup_entry_id,
                group_name=candidate.get("group_name", ""),
                group_key=candidate.get("group_key", ""),
                channel_id=candidate.get("channel_id", ""),
                channel_title=candidate.get("channel_title", ""),
            )
        return None

    def find_snapshot_video_detail_context(self, entry_id):
        return self.build_snapshot_video_detail_context(entry_id)

    def build_snapshot_from_local_cache(self, admin_data=None, latest_import=None, sections=None, errors=None):
        admin_data = admin_data if isinstance(admin_data, dict) else self.load_admin_data()
        latest_import = latest_import if isinstance(latest_import, dict) else {}
        if sections is None:
            resolved_import, resolved_sections, _source = self._resolve_pockettube_import_source(admin_data=admin_data, latest_import=latest_import)
            latest_import = resolved_import
            sections = resolved_sections
        section_records = [section for section in (sections or []) if isinstance(section, dict)]
        section_records.sort(key=lambda item: (
            self._section_sort_key(item),
            self._group_display_name(item).lower(),
        ))

        snapshot = self.empty_snapshot()
        snapshot["generated_at"] = self.current_timestamp()
        snapshot["synced_at"] = str(latest_import.get("imported_at", "") or "").strip()
        snapshot["errors"] = list(errors or [])

        for section in section_records:
            group_name = self._group_display_name(section)
            group_key = self.normalize_pockettube_group_key(section.get("group_key", "") or group_name)
            group_channels = []
            group_latest_video = {}
            group_latest_sort = None
            seen_channels = set()
            channels = [channel for channel in (section.get("channels", []) or []) if isinstance(channel, dict)]
            channels.sort(key=lambda item: (
                str(item.get("channel_name", "") or item.get("channel_title", "") or "").lower(),
                str(item.get("channel_id", "") or "").lower(),
            ))

            for channel in channels:
                channel_id = str(channel.get("channel_id", "") or "").strip()
                if not channel_id or channel_id in seen_channels:
                    continue
                seen_channels.add(channel_id)
                latest_video = self._latest_video_for_channel(channel_id)
                latest_summary = self.build_youtube_channel_video_summary(latest_video) if latest_video else {}
                latest_published = str(latest_summary.get("published_at", "") or "").strip()
                latest_sort = self._published_sort_key(latest_published)
                if latest_summary and (group_latest_sort is None or latest_sort > group_latest_sort):
                    group_latest_sort = latest_sort
                    group_latest_video = dict(latest_summary)

                channel_payload = self._build_channel_payload(channel, group_name, group_key, latest_summary)
                group_channels.append(channel_payload)
                self._merge_channel_snapshot(snapshot["channels"], channel_payload)

                group_payload = {
                "group_name": group_name,
                "group_key": group_key,
                "section_name": self.canonical_section_name(section.get("section_name", "") or group_name),
                "section_key": self.normalize_pockettube_group_key(section.get("section_key", "") or group_name),
                "source_name": str(section.get("source_name", "") or latest_import.get("source_name", "") or "PocketTube").strip() or "PocketTube",
                "imported_at": str(section.get("imported_at", "") or latest_import.get("imported_at", "") or "").strip(),
                "channel_count": len(group_channels),
                "latest_video_count": sum(1 for item in group_channels if isinstance(item.get("latest_video"), dict) and item["latest_video"]),
                "latest_video": group_latest_video,
                "channels": group_channels,
            }
            snapshot["groups"][group_key] = group_payload

        snapshot["groups"] = dict(sorted(snapshot["groups"].items(), key=lambda item: (item[1].get("group_name", "") or item[0]).lower()))
        return self.finalize_snapshot(snapshot)

    def _apply_latest_results_to_snapshot(self, snapshot, latest_results_by_group=None):
        snapshot = snapshot if isinstance(snapshot, dict) else self.empty_snapshot()
        groups = snapshot.get("groups", {})
        if not isinstance(groups, dict):
            return snapshot

        normalized_latest_results = {}
        for group_key, latest_result in (latest_results_by_group or {}).items():
            if not isinstance(latest_result, dict):
                continue
            normalized_group_key = self.normalize_pockettube_group_key(group_key)
            normalized_latest_results[normalized_group_key] = dict(latest_result)
            result_group_name = str(latest_result.get("group_name", "") or latest_result.get("section_name", "") or "").strip()
            if result_group_name:
                normalized_latest_results[self.normalize_pockettube_group_key(result_group_name)] = dict(latest_result)

        for group_key, group in groups.items():
            if not isinstance(group, dict):
                continue
            group_name = str(group.get("group_name", "") or group.get("section_name", "") or group_key or "").strip() or str(group_key or "").strip()
            latest_result = (
                normalized_latest_results.get(self.normalize_pockettube_group_key(group_key))
                or normalized_latest_results.get(self.normalize_pockettube_group_key(group_name))
                or {}
            )
            if not latest_result:
                continue
            latest_items = [item for item in (latest_result.get("latest_items", []) or []) if isinstance(item, dict)]
            group_videos = self._normalize_group_videos(latest_items, group_name, group_key)
            diagnostics = self._build_group_sync_diagnostics(group_key, group_name, latest_result, len(group_videos))
            group["videos"] = group_videos
            group["diagnostics"] = diagnostics
            group["latest_video_count"] = len(group_videos)
            if group_videos:
                group["latest_video"] = dict(group_videos[0])
            if not latest_items:
                continue

            latest_by_channel = {}
            for latest_summary in group_videos:
                channel_id = str(latest_summary.get("channel_id", "") or "").strip()
                if not channel_id:
                    continue
                existing_summary = latest_by_channel.get(channel_id)
                if existing_summary and self._published_sort_key(existing_summary.get("published_at", "")) > self._published_sort_key(latest_summary.get("published_at", "")):
                    continue
                latest_by_channel[channel_id] = dict(latest_summary)

            if not latest_by_channel:
                continue

            group_latest_video = {}
            group_latest_sort = None
            group_channels = group.get("channels", []) or []
            for channel in group_channels:
                if not isinstance(channel, dict):
                    continue
                channel_id = str(channel.get("channel_id", "") or "").strip()
                if not channel_id:
                    channel_id, _channel_title = self._pockettube_channel_identity(channel)
                latest_summary = latest_by_channel.get(channel_id)
                if not latest_summary:
                    continue
                current_latest = channel.get("latest_video", {}) if isinstance(channel.get("latest_video", {}), dict) else {}
                current_has_latest = bool(current_latest.get("video_id"))
                new_has_latest = bool(latest_summary.get("video_id"))
                current_sort = self._published_sort_key(str(channel.get("published_at", "") or current_latest.get("published_at", "") or ""))
                new_sort = self._published_sort_key(str(latest_summary.get("published_at", "") or ""))
                if new_has_latest and (not current_has_latest or new_sort >= current_sort):
                    merged_channel = dict(channel)
                    merged_channel["channel_id"] = channel_id or merged_channel.get("channel_id", "")
                    merged_channel["channel_title"] = str(
                        merged_channel.get("channel_title", "")
                        or latest_summary.get("channel_name", "")
                        or latest_summary.get("channel_title", "")
                        or ""
                    ).strip()
                    merged_channel["latest_video"] = dict(latest_summary)
                    merged_channel["latest_video_id"] = str(latest_summary.get("video_id", "") or "").strip()
                    merged_channel["published_at"] = str(latest_summary.get("published_at", "") or "").strip()
                    merged_channel["published_display"] = self.format_timestamp_label(merged_channel["published_at"], default="") if merged_channel["published_at"] else ""
                    merged_channel["thumbnail"] = str(
                        latest_summary.get("thumbnail", "")
                        or latest_summary.get("thumbnail_url", "")
                        or latest_summary.get("thumb", "")
                        or ""
                    ).strip()
                    merged_channel["url"] = str(latest_summary.get("url", "") or "").strip()
                    reason_tags = list(merged_channel.get("reason_tags", []) or [])
                    for tag in ("latest-sync", "latest-video"):
                        if tag not in reason_tags:
                            reason_tags.append(tag)
                    merged_channel["reason_tags"] = reason_tags
                    channel.clear()
                    channel.update(merged_channel)
                elif not current_has_latest and not new_has_latest:
                    channel["channel_id"] = channel_id or channel.get("channel_id", "")
                    if not str(channel.get("channel_title", "") or "").strip():
                        channel["channel_title"] = str(
                            latest_summary.get("channel_name", "")
                            or latest_summary.get("channel_title", "")
                            or channel_id
                        ).strip() or channel_id

                if new_has_latest:
                    latest_sort = self._published_sort_key(str(latest_summary.get("published_at", "") or ""))
                    if group_latest_sort is None or latest_sort >= group_latest_sort:
                        group_latest_sort = latest_sort
                        group_latest_video = dict(latest_summary)

            group["channel_count"] = len(group_channels)
            group["latest_video_count"] = len(group_videos)
            group["latest_video"] = group_latest_video if isinstance(group_latest_video, dict) and group_latest_video else (dict(group_videos[0]) if group_videos else {})

        return snapshot

    def _build_refresh_admin_data(self, admin_data=None, latest_import=None):
        admin_data = admin_data if isinstance(admin_data, dict) else self.load_admin_data()
        latest_import = latest_import if isinstance(latest_import, dict) else {}
        normalized_latest = self._normalize_pockettube_import_source(latest_import)
        latest_payload = dict(normalized_latest)
        latest_payload["sections"] = [dict(section) for section in normalized_latest.get("sections", []) or []]
        latest_payload["channels"] = [dict(channel) for channel in normalized_latest.get("channels", []) or []]
        merged_admin_data = dict(admin_data)
        pockettube_imports = merged_admin_data.get("youtube_pockettube_imports", {})
        if not isinstance(pockettube_imports, dict):
            pockettube_imports = {}
        pockettube_imports = dict(pockettube_imports)
        pockettube_imports["latest"] = latest_payload
        merged_admin_data["youtube_pockettube_imports"] = pockettube_imports
        return merged_admin_data

    def _refresh_input_channel_count(self, admin_data, section_name):
        _latest, sections, _source = self._resolve_pockettube_import_source(admin_data=admin_data)
        wanted = self.normalize_pockettube_group_key(section_name)
        for section in sections or []:
            if not isinstance(section, dict):
                continue
            candidate_section_name = self.canonical_section_name(section.get("section_name", "") or section.get("group_name", "") or "")
            candidate_group_name = self.canonical_section_name(section.get("group_name", "") or candidate_section_name or "")
            candidate_section_key = self.normalize_pockettube_group_key(section.get("section_key", "") or candidate_section_name)
            candidate_group_key = self.normalize_pockettube_group_key(section.get("group_key", "") or candidate_group_name)
            candidate_keys = {
                candidate_section_key,
                candidate_group_key,
                self.normalize_pockettube_group_key(candidate_section_name),
                self.normalize_pockettube_group_key(candidate_group_name),
            }
            if wanted and wanted not in {key for key in candidate_keys if key}:
                continue
            return len([channel for channel in (section.get("channels", []) or []) if isinstance(channel, dict)])
        return 0

    def finalize_snapshot(self, snapshot):
        snapshot = self._normalize_snapshot(snapshot)
        finalized_channels = {}
        existing_channels = snapshot.get("channels", {})
        if isinstance(existing_channels, dict):
            for channel_id, channel in existing_channels.items():
                normalized_channel = self._normalize_channel_entry(channel, channel_id_hint=channel_id)
                if normalized_channel:
                    finalized_channels[normalized_channel["channel_id"]] = normalized_channel
        groups = snapshot.get("groups", {})
        if isinstance(groups, dict):
            for group_key, group in groups.items():
                if not isinstance(group, dict):
                    continue
                group_name = str(group.get("group_name", "") or group.get("section_name", "") or group_key or "").strip() or str(group_key or "").strip()
                for channel in group.get("channels", []) or []:
                    if not isinstance(channel, dict):
                        continue
                    channel_payload = dict(channel)
                    group_names = list(channel_payload.get("group_names", []) or [])
                    if group_name and group_name not in group_names:
                        group_names.append(group_name)
                    channel_payload["group_names"] = group_names
                    self._merge_channel_snapshot(finalized_channels, channel_payload)
        snapshot["channels"] = dict(sorted(finalized_channels.items(), key=lambda item: item[0].lower()))
        return snapshot

    def sync_snapshot(self, scope="", max_channels=200):
        admin_data = self.load_admin_data()
        latest_import, sections, registry_source = self._resolve_pockettube_import_source(admin_data=admin_data)
        filtered_sections = self._filter_sections_for_scope(sections, scope)
        refresh_admin_data = self._build_refresh_admin_data(admin_data=admin_data, latest_import=latest_import)
        errors = []
        latest_results_by_group = {}
        if registry_source == "registry" and not sections:
            errors.append(f"PocketTube registry file missing or empty: {self.registry_path.name}")
        for section in filtered_sections:
            group_name = self._group_display_name(section)
            normalized_channel_count = len([channel for channel in (section.get("channels", []) or []) if isinstance(channel, dict)])
            refresh_input_channel_count = self._refresh_input_channel_count(refresh_admin_data, group_name)
            self.app_logger.info(
                "youtube_freshness_section_refresh_input section_name=%s normalized_channel_count=%s refresh_input_channel_count=%s",
                group_name or "-",
                normalized_channel_count,
                refresh_input_channel_count,
            )
            try:
                latest_result = self.refresh_pockettube_section_latest_uploads(
                    group_name,
                    admin_data=refresh_admin_data,
                    max_channels=max_channels,
                )
                if isinstance(latest_result, dict):
                    group_key = self.normalize_pockettube_group_key(section.get("group_key", "") or group_name)
                    latest_results_by_group[group_key] = latest_result
                    latest_results_by_group[self.normalize_pockettube_group_key(latest_result.get("group_name", "") or group_name)] = latest_result
            except Exception as exc:
                error_text = f"{group_name}: {type(exc).__name__}: {exc}"
                errors.append(error_text)
                self.app_logger.warning("youtube freshness sync failed group=%s error=%s", group_name, exc)
        snapshot = self.build_snapshot_from_local_cache(
            admin_data=admin_data,
            latest_import=latest_import,
            sections=filtered_sections,
            errors=errors,
        )
        snapshot["generated_at"] = self.current_timestamp()
        snapshot["synced_at"] = self.current_timestamp()
        snapshot["errors"] = errors
        snapshot = self._apply_latest_results_to_snapshot(snapshot, latest_results_by_group)
        snapshot = self.finalize_snapshot(snapshot)
        warnings = []
        for group_key, group in (snapshot.get("groups", {}) or {}).items():
            if not isinstance(group, dict):
                continue
            channels_assigned = int(group.get("channels_assigned", len(group.get("channels", []) or [])) or 0)
            videos_stored = int(group.get("videos_stored_after_dedupe", len(group.get("videos", []) or [])) or 0)
            if channels_assigned > 0 and videos_stored == 0:
                diagnostics_errors = [str(error or "").strip() for error in list((group.get("diagnostics", {}) or {}).get("errors", []) or []) if str(error or "").strip()]
                warning_text = f"{group.get('group_key', group_key)}: {channels_assigned} channels, 0 stored videos"
                if diagnostics_errors:
                    warning_text = f"{warning_text} ({diagnostics_errors[0]})"
                warnings.append(warning_text)
        snapshot["warnings"] = warnings
        group_channels_total = sum(len(group.get("channels", []) or []) for group in snapshot.get("groups", {}).values() if isinstance(group, dict))
        latest_videos_total = sum(
            1
            for group in snapshot.get("groups", {}).values()
            if isinstance(group, dict)
            for channel in group.get("channels", []) or []
            if isinstance(channel, dict) and isinstance(channel.get("latest_video", {}), dict) and channel["latest_video"].get("video_id")
        )
        self.app_logger.info(
            "youtube_freshness_snapshot_finalized groups=%s group_channels=%s channels=%s latest_videos=%s warnings=%s errors=%s",
            len(snapshot.get("groups", {}) or {}),
            group_channels_total,
            len(snapshot.get("channels", {}) or {}),
            latest_videos_total,
            len(warnings or []),
            len(errors or []),
        )
        self.save_snapshot(snapshot)
        sync_warnings = list(snapshot.get("warnings", []) or [])
        self.save_sync_status({
            "status": "completed",
            "requested_at": self.load_sync_status().get("requested_at", ""),
            "started_at": self.load_sync_status().get("started_at", ""),
            "completed_at": self.current_timestamp(),
            "last_error": "",
            "warnings": sync_warnings,
            "scope": str(scope or "").strip(),
            "run_id": self.load_sync_status().get("run_id", ""),
            "run_url": self.load_sync_status().get("run_url", ""),
            "source": "workflow",
            "updated_at": self.current_timestamp(),
        })
        return snapshot

    def request_sync(self, scope=""):
        scope_value = str(scope or "").strip()
        status = self.load_sync_status()
        status.update({
            "status": "requested",
            "requested_at": self.current_timestamp(),
            "started_at": status.get("started_at", ""),
            "completed_at": status.get("completed_at", ""),
            "last_error": "",
            "scope": scope_value,
            "updated_at": self.current_timestamp(),
            "source": "web_request",
        })
        self.save_sync_status(status)
        trigger_payload, trigger_status_code = self.trigger_github_actions_sync(scope=scope_value)
        trigger_payload = dict(trigger_payload or {})
        if trigger_status_code >= 400 or trigger_payload.get("ok") is False:
            status.update({
                "status": "failed",
                "last_error": str(trigger_payload.get("error") or "Could not queue the YouTube freshness sync.") or "Could not queue the YouTube freshness sync.",
                "updated_at": self.current_timestamp(),
                "source": "web_request",
            })
            self.save_sync_status(status)
        else:
            run_state = str(trigger_payload.get("status", "") or "").strip().lower()
            if run_state in {"started", "queued", "in_progress"}:
                status.update({
                    "status": "queued" if run_state != "in_progress" else "in_progress",
                    "run_id": str(trigger_payload.get("run_id", "") or "").strip(),
                    "run_url": str(trigger_payload.get("run_url", "") or "").strip(),
                    "updated_at": self.current_timestamp(),
                    "source": "github_actions",
                })
                self.save_sync_status(status)
            elif run_state == "already_running":
                status.update({
                    "status": "queued",
                    "updated_at": self.current_timestamp(),
                    "source": "github_actions",
                })
                self.save_sync_status(status)
        return {
            "ok": trigger_status_code < 400 and trigger_payload.get("ok", True) is not False,
            "trigger": trigger_payload,
            "sync_status": self.load_sync_status(),
        }, 200 if trigger_status_code < 400 else trigger_status_code

    def ingest_github_snapshot_update(self, payload):
        payload = payload if isinstance(payload, dict) else {}
        status = self.load_sync_status()
        event_status = str(payload.get("status", "") or "").strip().lower()
        if event_status in {"failed", "error"}:
            status.update({
                "status": "failed",
                "last_error": str(payload.get("error", "") or payload.get("message", "") or "YouTube freshness sync failed.") or "YouTube freshness sync failed.",
                "completed_at": self.current_timestamp(),
                "updated_at": self.current_timestamp(),
                "source": "github_actions",
                "run_id": str(payload.get("run_id", "") or status.get("run_id", "") or "").strip(),
                "run_url": str(payload.get("run_url", "") or status.get("run_url", "") or "").strip(),
            })
            self.save_sync_status(status)
            return {"ok": True, "status": "failed", "sync_status": status}

        try:
            if self.snapshot_raw_url and self.sync_status_raw_url and self.requests_module is not None:
                snapshot_result = self.refresh_local_snapshot_from_github()
                snapshot = self.load_snapshot()
            else:
                snapshot = self.refresh_snapshot_from_github()
                snapshot_result = {"ok": True, "status": "updated"}
        except Exception as exc:
            error_message = self._github_snapshot_download_error_message(exc)
            status.update({
                "status": "failed",
                "last_error": error_message,
                "completed_at": self.current_timestamp(),
                "updated_at": self.current_timestamp(),
                "source": "github_actions",
                "run_id": str(payload.get("run_id", "") or status.get("run_id", "") or "").strip(),
                "run_url": str(payload.get("run_url", "") or status.get("run_url", "") or "").strip(),
            })
            self.save_sync_status(status)
            return {
                "ok": False,
                "status": "failed",
                "error": error_message,
                "sync_status": status,
            }

        status.update({
            "status": "completed",
            "last_error": "",
            "completed_at": self.current_timestamp(),
            "updated_at": self.current_timestamp(),
            "source": "github_actions",
            "run_id": str(payload.get("run_id", "") or status.get("run_id", "") or "").strip(),
            "run_url": str(payload.get("run_url", "") or status.get("run_url", "") or "").strip(),
        })
        self.save_sync_status(status)
        return {
            "ok": True,
            "status": "completed",
            "sync_status": status,
            "snapshot": snapshot,
            "snapshot_refresh": snapshot_result,
        }

    def _resolve_pockettube_import_source(self, admin_data=None, latest_import=None, sections=None):
        admin_data = admin_data if isinstance(admin_data, dict) else self.load_admin_data()
        if isinstance(latest_import, dict) and isinstance(sections, list) and sections:
            normalized_latest = self._normalize_pockettube_import_source(latest_import)
            return normalized_latest, normalized_latest.get("sections", []), "explicit"

        primary_latest, primary_sections = self.pockettube_latest_import_snapshot(admin_data)
        primary_latest = self._normalize_pockettube_import_source(primary_latest)
        if primary_sections:
            return primary_latest, primary_latest.get("sections", []), "admin_data"

        registry_latest = self._load_pockettube_registry_payload()
        registry_latest = self._normalize_pockettube_import_source(registry_latest)
        if registry_latest.get("sections"):
            return registry_latest, registry_latest.get("sections", []), "registry"

        return self._empty_pockettube_import_source(), [], "empty"

    def _load_pockettube_registry_payload(self):
        payload = self.load_json_file(self.registry_path, self._empty_pockettube_import_source())
        if not isinstance(payload, dict):
            return self._empty_pockettube_import_source()
        if isinstance(payload.get("latest"), dict) and isinstance(payload["latest"].get("sections"), list):
            return payload["latest"]
        if isinstance(payload.get("sections"), list):
            return payload
        return self._empty_pockettube_import_source()

    def _empty_pockettube_import_source(self):
        return {
            "source_name": "PocketTube",
            "source_structure": {
                "top_level_groups": [],
                "main_collection_page": "",
                "meta_keys": [],
            },
            "fingerprint": "",
            "imported_at": "",
            "section_count": 0,
            "group_count": 0,
            "channel_count": 0,
            "sections": [],
            "channels": [],
        }

    def _normalize_pockettube_import_source(self, payload):
        if not isinstance(payload, dict):
            return self._empty_pockettube_import_source()
        normalized = self._empty_pockettube_import_source()
        normalized["source_name"] = str(payload.get("source_name", "") or payload.get("source", "") or "PocketTube").strip() or "PocketTube"
        normalized["fingerprint"] = str(payload.get("fingerprint", "") or "").strip()
        normalized["imported_at"] = str(payload.get("imported_at", "") or "").strip() or self.current_timestamp()
        source_structure = payload.get("source_structure", {})
        normalized["source_structure"] = source_structure if isinstance(source_structure, dict) else normalized["source_structure"]
        sections = []
        channels = []
        for section in payload.get("sections", []) or []:
            if not isinstance(section, dict):
                continue
            normalized_section = dict(section)
            normalized_channels = []
            for channel in section.get("channels", []) or []:
                if not isinstance(channel, dict):
                    continue
                channel_name = str(channel.get("channel_name", "") or channel.get("channel_title", "") or "").strip()
                channel_id = str(channel.get("channel_id", "") or channel.get("channelId", "") or "").strip()
                if not channel_id and channel_name and re.fullmatch(r"UC[a-zA-Z0-9_-]{20,}", channel_name):
                    channel_id = channel_name
                if not channel_id:
                    inferred_id, inferred_title = self._pockettube_channel_identity(channel)
                    channel_id = str(inferred_id or "").strip()
                    if inferred_title and not channel_name:
                        channel_name = inferred_title
                normalized_channel = dict(channel)
                normalized_channel["channel_name"] = channel_name or channel_id
                normalized_channel["channel_id"] = channel_id
                normalized_channel["channel_key"] = str(channel.get("channel_key", "") or channel_id or channel_name or "").strip()
                normalized_channel["section_name"] = str(channel.get("section_name", "") or normalized_section.get("section_name", "") or "").strip()
                normalized_channel["section_key"] = str(channel.get("section_key", "") or normalized_section.get("section_key", "") or "").strip()
                normalized_channel["group_name"] = str(channel.get("group_name", "") or normalized_section.get("group_name", "") or "").strip()
                normalized_channel["group_key"] = str(channel.get("group_key", "") or normalized_section.get("group_key", "") or "").strip()
                normalized_channels.append(normalized_channel)
                channels.append(normalized_channel)
            normalized_section["channels"] = normalized_channels
            normalized_section["section_name"] = str(normalized_section.get("section_name", "") or normalized_section.get("group_name", "") or "").strip()
            normalized_section["section_key"] = str(normalized_section.get("section_key", "") or normalized_section["section_name"] or "").strip()
            normalized_section["group_name"] = str(normalized_section.get("group_name", "") or normalized_section["section_name"] or "").strip()
            normalized_section["group_key"] = str(normalized_section.get("group_key", "") or normalized_section["group_name"] or "").strip()
            normalized_section["channel_count"] = int(normalized_section.get("channel_count", len(normalized_channels)) or len(normalized_channels))
            sections.append(normalized_section)
        normalized["sections"] = sections
        normalized["channels"] = channels
        normalized["section_count"] = int(payload.get("section_count", len(sections)) or len(sections))
        normalized["group_count"] = int(payload.get("group_count", len(sections)) or len(sections))
        normalized["channel_count"] = int(payload.get("channel_count", len(channels)) or len(channels))
        if not normalized["source_structure"].get("top_level_groups"):
            normalized["source_structure"] = {
                "top_level_groups": [str(section.get("section_name", "") or section.get("group_name", "") or "").strip() for section in sections if str(section.get("section_name", "") or section.get("group_name", "") or "").strip()],
                "main_collection_page": str(normalized["source_structure"].get("main_collection_page", "") or "").strip(),
                "meta_keys": list(normalized["source_structure"].get("meta_keys", []) or []),
            }
        return normalized

    def _pockettube_channel_identity(self, channel):
        channel = channel if isinstance(channel, dict) else {}
        candidate_values = [
            channel.get("channel_id", ""),
            channel.get("channelId", ""),
            channel.get("id", ""),
            channel.get("browse_id", ""),
            channel.get("browseId", ""),
            channel.get("channel_name", ""),
            channel.get("channel_title", ""),
            channel.get("title", ""),
            channel.get("label", ""),
            channel.get("channel_key", ""),
            channel.get("url", ""),
        ]
        channel_id = ""
        for value in candidate_values:
            text = str(value or "").strip()
            if not text:
                continue
            if text.startswith("http") and "/channel/" in text:
                candidate = text.split("/channel/", 1)[1].split("?", 1)[0].split("/", 1)[0].strip()
                if candidate:
                    channel_id = candidate
                    break
            if re.fullmatch(r"UC[a-zA-Z0-9_-]{20,}", text):
                channel_id = text
                break
            if not channel_id and text:
                channel_id = text
        channel_title = ""
        for value in (
            channel.get("channel_title", ""),
            channel.get("title", ""),
            channel.get("name", ""),
            channel.get("label", ""),
            channel.get("channel_name", ""),
        ):
            text = str(value or "").strip()
            if text:
                channel_title = text
                break
        if not channel_title:
            channel_title = channel_id or "Unknown Channel"
        if not channel_id:
            channel_id = channel_title
        return channel_id, channel_title

    def build_page_context(self):
        return self.build_page_context_for_filter()

    def build_page_context_for_filter(self, selected_filter="all", display_limit=None):
        snapshot = self.load_snapshot()
        sync_status = self.load_sync_status()
        resolved_display_limit = self._normalize_display_limit(display_limit)
        groups = []
        has_latest = False
        for group_key, group in snapshot.get("groups", {}).items():
            if not isinstance(group, dict):
                continue
            channels = []
            for channel in group.get("channels", []) or []:
                if not isinstance(channel, dict):
                    continue
                latest_video = channel.get("latest_video", {})
                latest_exists = bool(isinstance(latest_video, dict) and latest_video.get("video_id"))
                has_latest = has_latest or latest_exists
                channels.append({
                    "channel_id": channel.get("channel_id", ""),
                    "channel_title": channel.get("channel_title", ""),
                    "group_names": list(channel.get("group_names", []) or []),
                    "latest_video": latest_video if isinstance(latest_video, dict) else {},
                    "published_display": channel.get("published_display", ""),
                    "published_at": channel.get("published_at", ""),
                    "latest_video_id": channel.get("latest_video_id", ""),
                    "thumbnail": channel.get("thumbnail", ""),
                    "url": channel.get("url", ""),
                    "reason_tags": list(channel.get("reason_tags", []) or []),
                })
            group_latest_video = group.get("latest_video", {}) if isinstance(group.get("latest_video", {}), dict) else {}
            group_latest_exists = bool(group_latest_video.get("video_id"))
            has_latest = has_latest or group_latest_exists
            if not channels and group_latest_exists:
                synthetic_channel_title = str(
                    group_latest_video.get("channel_name", "")
                    or group.get("group_name", "")
                    or group.get("section_name", "")
                    or group_key
                ).strip() or group_key
                channels.append({
                    "channel_id": str(group_latest_video.get("channel_id", "") or "").strip(),
                    "channel_title": synthetic_channel_title,
                    "group_names": [group.get("group_name", group_key)],
                    "latest_video": group_latest_video,
                    "published_display": self.format_timestamp_label(
                        str(group_latest_video.get("published_at", "") or "").strip(),
                        default="",
                    ),
                    "published_at": str(group_latest_video.get("published_at", "") or "").strip(),
                    "latest_video_id": str(group_latest_video.get("video_id", "") or "").strip(),
                    "thumbnail": str(
                        group_latest_video.get("thumbnail", "")
                        or group_latest_video.get("thumbnail_url", "")
                        or group_latest_video.get("thumb", "")
                        or ""
                    ).strip(),
                    "url": str(group_latest_video.get("url", "") or "").strip(),
                    "reason_tags": ["cached-latest"],
                })
            groups.append({
                "group_key": group.get("group_key", group_key),
                "group_name": group.get("group_name", group_key),
                "section_name": group.get("section_name", group.get("group_name", group_key)),
                "channel_count": int(group.get("channel_count", len(channels)) or 0),
                "latest_video_count": int(group.get("latest_video_count", 0) or 0),
                "latest_video": group.get("latest_video", {}) if isinstance(group.get("latest_video", {}), dict) else {},
                "channels": channels,
                "source_name": group.get("source_name", "PocketTube"),
                "imported_at": group.get("imported_at", ""),
            })

        groups.sort(key=lambda item: (item.get("group_name", "") or item.get("group_key", "")).lower())
        feed_context = self._build_freshness_feed_context(snapshot)
        feed_videos = list(feed_context.get("videos", []) or [])
        feed_groups = list(feed_context.get("groups", []) or [])
        empty_channels = list(feed_context.get("empty_channels", []) or [])
        filter_context = self._build_snapshot_filter_context(feed_groups, feed_videos, selected_filter=selected_filter)
        filtered_videos_all = list(filter_context.get("filtered_videos", []) or [])
        filtered_videos = filtered_videos_all[:resolved_display_limit] if resolved_display_limit > 0 else list(filtered_videos_all)
        selected_filter_record = dict(filter_context.get("selected_filter_record", {}) or {})
        selected_filter_key = str(filter_context.get("selected_filter_key", "all") or "all").strip() or "all"
        selected_filter_label = str(selected_filter_record.get("label", "All") or "All").strip() or "All"
        feed_video_count = len(filtered_videos)
        selected_filter_available_count = len(filtered_videos_all)
        empty_channel_count = len(empty_channels)
        empty_group_count = len([group for group in feed_groups if int(group.get("empty_channel_count", 0) or 0) > 0])
        has_latest = bool(feed_video_count)
        snapshot_status = self._build_snapshot_status(snapshot, sync_status, has_latest=bool(feed_videos))
        refresh_state = self._build_refresh_state(snapshot, sync_status)
        freshness_note = self._build_freshness_note(snapshot_status, refresh_state, sync_status)
        return {
            "title": "PocketTube Freshness",
            "snapshot": snapshot,
            "sync_status": sync_status,
            "groups": groups,
            "group_count": len(groups),
            "channel_count": sum(len(group.get("channels", [])) for group in groups),
            "feed_videos": filtered_videos,
            "feed_videos_all": feed_videos,
            "feed_groups": feed_groups,
            "feed_video_count": feed_video_count,
            "feed_video_count_total": len(feed_videos),
            "selected_filter_available_count": selected_filter_available_count,
            "selected_filter_display_count": feed_video_count,
            "feed_empty_channels": empty_channels,
            "feed_empty_channel_count": empty_channel_count,
            "feed_empty_group_count": empty_group_count,
            "feed_filters": list(filter_context.get("filters", []) or []),
            "selected_filter_key": selected_filter_key,
            "selected_filter_label": selected_filter_label,
            "selected_filter_count": int(selected_filter_record.get("video_count", selected_filter_available_count) or 0),
            "display_limit": resolved_display_limit,
            "display_limit_options": list(POCKETTUBE_DISPLAY_LIMIT_OPTIONS),
            "group_video_limit": int(snapshot.get("group_video_limit", POCKETTUBE_GROUP_VIDEO_LIMIT) or POCKETTUBE_GROUP_VIDEO_LIMIT),
            "all_feed_video_limit": int(snapshot.get("all_feed_video_limit", POCKETTUBE_ALL_FEED_VIDEO_LIMIT) or POCKETTUBE_ALL_FEED_VIDEO_LIMIT),
            "has_latest": has_latest,
            "generated_at": snapshot.get("generated_at", ""),
            "synced_at": snapshot.get("synced_at", ""),
            "last_refreshed_at": refresh_state.last_refreshed_at,
            "last_refreshed_at_display": refresh_state.last_refreshed_at_display,
            "refresh_status": refresh_state.refresh_status,
            "refresh_error": refresh_state.refresh_error,
            "is_stale": bool(getattr(getattr(refresh_state, "stale", None), "is_stale", False)),
            "errors": list(snapshot.get("errors", []) or []),
            "empty_state": not has_latest,
            "empty_reason": "no_snapshot" if not snapshot.get("groups") else "no_cached_latest",
            "sync_notice": self._sync_notice(sync_status),
            "snapshot_status": snapshot_status,
            "freshness_note": freshness_note,
        }

    def _build_freshness_feed_context(self, snapshot):
        snapshot = snapshot if isinstance(snapshot, dict) else self.empty_snapshot()
        groups = snapshot.get("groups", {})
        if not isinstance(groups, dict):
            groups = {}
        feed_by_video_id = {}
        feed_groups = []
        empty_channels = []
        sorted_groups = sorted(
            groups.items(),
            key=lambda item: (str((item[1] or {}).get("group_name", "") or item[0]).lower(), str(item[0] or "").lower()),
        )
        for group_key, group in sorted_groups:
            if not isinstance(group, dict):
                continue
            group_name = str(group.get("group_name", "") or group.get("section_name", "") or group_key or "").strip() or str(group_key or "").strip()
            group_channels = [channel for channel in (group.get("channels", []) or []) if isinstance(channel, dict)]
            group_video_ids = set()
            group_empty_channels = 0
            group_latest_video = group.get("latest_video", {}) if isinstance(group.get("latest_video", {}), dict) else {}
            group_videos = [video for video in (group.get("videos", []) or []) if isinstance(video, dict)]
            if group_videos:
                for video in group_videos:
                    normalized_video = self._normalize_group_video(video, group_name, group_key, prefer_snapshot_fields=True)
                    video_id = str(normalized_video.get("video_id", "") or "").strip()
                    if not video_id:
                        continue
                    group_video_ids.add(video_id)
                    existing = feed_by_video_id.get(video_id)
                    if not isinstance(existing, dict):
                        feed_by_video_id[video_id] = dict(normalized_video)
                        continue
                    existing["group_names"] = sorted(dict.fromkeys(list(existing.get("group_names", []) or []) + list(normalized_video.get("group_names", []) or [])), key=lambda value: str(value or "").lower())
                    existing["group_keys"] = sorted(dict.fromkeys(list(existing.get("group_keys", []) or []) + list(normalized_video.get("group_keys", []) or [])), key=lambda value: str(value or "").lower())
                    existing["reason_tags"] = sorted(dict.fromkeys(list(existing.get("reason_tags", []) or []) + list(normalized_video.get("reason_tags", []) or [])), key=lambda value: str(value or "").lower())
                    if self._published_sort_key(normalized_video.get("published_at", "")) > self._published_sort_key(existing.get("published_at", "")):
                        feed_by_video_id[video_id] = {**existing, **dict(normalized_video)}
                feed_groups.append({
                    "group_key": group_key,
                    "group_name": group_name,
                    "video_count": len(group_video_ids),
                    "channel_count": len(group_channels),
                    "empty_channel_count": 0,
                })
                continue
            for channel in group_channels:
                latest_video = channel.get("latest_video", {}) if isinstance(channel.get("latest_video", {}), dict) else {}
                latest_video_id = str(channel.get("latest_video_id", "") or latest_video.get("video_id", "") or "").strip()
                if not latest_video_id:
                    group_empty_channels += 1
                    empty_channels.append({
                        "group_key": group_key,
                        "group_name": group_name,
                        "channel_id": str(channel.get("channel_id", "") or "").strip(),
                        "channel_title": str(channel.get("channel_title", "") or "").strip() or "Unknown Channel",
                    })
                    continue

                group_video_ids.add(latest_video_id)
                existing = feed_by_video_id.get(latest_video_id)
                if not isinstance(existing, dict):
                    published_at = str(channel.get("published_at", "") or latest_video.get("published_at", "") or "").strip()
                    thumbnail = str(
                        latest_video.get("thumbnail", "")
                        or latest_video.get("thumbnail_url", "")
                        or latest_video.get("image_url", "")
                        or latest_video.get("thumb", "")
                        or channel.get("thumbnail", "")
                        or ""
                    ).strip()
                    url = str(latest_video.get("url", "") or channel.get("url", "") or "").strip()
                    if not url and latest_video_id:
                        url = f"https://www.youtube.com/watch?v={latest_video_id}"
                    detail_url = str(latest_video.get("detail_url", "") or channel.get("detail_url", "") or "").strip()
                    if not detail_url and latest_video_id:
                        detail_url = f"/video/yt-{latest_video_id}"
                    feed_by_video_id[latest_video_id] = {
                        "video_id": latest_video_id,
                        "title": str(latest_video.get("title", "") or latest_video.get("name", "") or channel.get("channel_title", "") or "Untitled video").strip() or "Untitled video",
                        "channel_id": str(channel.get("channel_id", "") or latest_video.get("channel_id", "") or "").strip(),
                        "channel_title": str(
                            latest_video.get("channel_name", "")
                            or latest_video.get("channel_title", "")
                            or channel.get("channel_title", "")
                            or "Unknown Channel"
                        ).strip() or "Unknown Channel",
                        "published_at": published_at,
                        "published_display": str(channel.get("published_display", "") or latest_video.get("published_display", "") or "").strip() or self.format_timestamp_label(published_at, default="") if published_at else "",
                        "thumbnail": thumbnail,
                        "detail_url": detail_url,
                        "url": url,
                        "group_names": list(dict.fromkeys([
                            str(name or "").strip()
                            for name in list(channel.get("group_names", []) or []) + [group_name]
                            if str(name or "").strip()
                        ])),
                        "group_keys": list(dict.fromkeys([
                            str(key or "").strip()
                            for key in [str(channel.get("group_key", "") or "").strip(), str(group_key or "").strip()]
                            if str(key or "").strip()
                        ])),
                        "reason_tags": list(dict.fromkeys([
                            str(tag or "").strip()
                            for tag in list(channel.get("reason_tags", []) or []) + ["cached-latest"]
                            if str(tag or "").strip()
                        ])),
                    }
                else:
                    group_names = list(existing.get("group_names", []) or [])
                    for name in list(channel.get("group_names", []) or []) + [group_name]:
                        normalized_name = str(name or "").strip()
                        if normalized_name and normalized_name not in group_names:
                            group_names.append(normalized_name)
                    existing["group_names"] = sorted(dict.fromkeys(group_names), key=lambda value: str(value or "").lower())
                    group_keys = list(existing.get("group_keys", []) or [])
                    for key in [str(channel.get("group_key", "") or "").strip(), str(group_key or "").strip()]:
                        if key and key not in group_keys:
                            group_keys.append(key)
                    existing["group_keys"] = sorted(dict.fromkeys(group_keys), key=lambda value: str(value or "").lower())
                    if not str(existing.get("channel_title", "") or "").strip():
                        existing["channel_title"] = str(
                            latest_video.get("channel_name", "")
                            or latest_video.get("channel_title", "")
                            or channel.get("channel_title", "")
                            or "Unknown Channel"
                        ).strip() or "Unknown Channel"
                    if not str(existing.get("thumbnail", "") or "").strip():
                        existing["thumbnail"] = str(
                            latest_video.get("thumbnail", "")
                            or latest_video.get("thumbnail_url", "")
                            or latest_video.get("image_url", "")
                            or latest_video.get("thumb", "")
                            or channel.get("thumbnail", "")
                            or ""
                        ).strip()
                    if not str(existing.get("url", "") or "").strip():
                        existing["url"] = str(latest_video.get("url", "") or channel.get("url", "") or "").strip()
                    if not str(existing.get("detail_url", "") or "").strip():
                        existing["detail_url"] = str(latest_video.get("detail_url", "") or channel.get("detail_url", "") or "").strip() or f"/video/yt-{latest_video_id}"
                    if not str(existing.get("published_at", "") or "").strip():
                        existing["published_at"] = published_at
                    if not str(existing.get("published_display", "") or "").strip() and published_at:
                        existing["published_display"] = self.format_timestamp_label(published_at, default="")
                    existing["reason_tags"] = list(dict.fromkeys([
                        str(tag or "").strip()
                        for tag in list(existing.get("reason_tags", []) or []) + list(channel.get("reason_tags", []) or []) + ["cached-latest"]
                        if str(tag or "").strip()
                    ]))

            group_latest_video_id = str(group_latest_video.get("video_id", "") or "").strip()
            if group_latest_video_id and group_latest_video_id not in feed_by_video_id:
                published_at = str(group_latest_video.get("published_at", "") or "").strip()
                thumbnail = str(
                    group_latest_video.get("thumbnail", "")
                    or group_latest_video.get("thumbnail_url", "")
                    or group_latest_video.get("image_url", "")
                    or group_latest_video.get("thumb", "")
                    or ""
                ).strip()
                url = str(group_latest_video.get("url", "") or "").strip()
                if not url:
                    url = f"https://www.youtube.com/watch?v={group_latest_video_id}"
                detail_url = str(group_latest_video.get("detail_url", "") or "").strip()
                if not detail_url:
                    detail_url = f"/video/yt-{group_latest_video_id}"
                feed_by_video_id[group_latest_video_id] = {
                    "video_id": group_latest_video_id,
                    "title": str(group_latest_video.get("title", "") or group_latest_video.get("name", "") or group_name or "Untitled video").strip() or "Untitled video",
                    "channel_id": str(group_latest_video.get("channel_id", "") or "").strip(),
                    "channel_title": str(
                        group_latest_video.get("channel_name", "")
                        or group_latest_video.get("channel_title", "")
                        or group_name
                        or "Unknown Channel"
                    ).strip() or "Unknown Channel",
                    "published_at": published_at,
                    "published_display": str(group_latest_video.get("published_display", "") or "").strip() or self.format_timestamp_label(published_at, default="") if published_at else "",
                    "thumbnail": thumbnail,
                    "detail_url": detail_url,
                    "url": url,
                    "group_names": [group_name] if group_name else [],
                    "group_keys": [str(group_key or "").strip()] if str(group_key or "").strip() else [],
                    "reason_tags": ["cached-latest"],
                }
                group_video_ids.add(group_latest_video_id)

            feed_groups.append({
                "group_key": group_key,
                "group_name": group_name,
                "video_count": len(group_video_ids),
                "channel_count": len(group_channels),
                "empty_channel_count": group_empty_channels,
            })

        feed_videos = list(feed_by_video_id.values())
        feed_videos.sort(key=lambda item: (
            -self._published_sort_key(str(item.get("published_at", "") or "")),
            str(item.get("title", "") or "").lower(),
            str(item.get("channel_title", "") or "").lower(),
            str(item.get("video_id", "") or "").lower(),
        ))
        feed_groups.sort(key=lambda item: (str(item.get("group_name", "") or item.get("group_key", "")).lower(), str(item.get("group_key", "") or "").lower()))
        empty_channels.sort(key=lambda item: (
            str(item.get("group_name", "") or "").lower(),
            str(item.get("channel_title", "") or "").lower(),
            str(item.get("channel_id", "") or "").lower(),
        ))
        return {
            "videos": feed_videos,
            "groups": feed_groups,
            "empty_channels": empty_channels,
        }

    def _build_snapshot_filter_context(self, feed_groups, feed_videos, selected_filter="all"):
        feed_groups = [group for group in (feed_groups or []) if isinstance(group, dict)]
        feed_videos = [video for video in (feed_videos or []) if isinstance(video, dict)]
        selected_key = self._normalize_snapshot_filter_key(selected_filter)
        available_group_map = {}
        for group in feed_groups:
            group_key = self._normalize_snapshot_filter_key(group.get("group_key", "") or group.get("group_name", ""))
            if not group_key:
                continue
            available_group_map[group_key] = {
                "group_key": group_key,
                "group_name": str(group.get("group_name", "") or group_key).strip() or group_key,
                "video_count": int(group.get("video_count", 0) or 0),
                "filter_keys": [group_key],
                "kind": "group",
            }

        canonical_filters = [
            {"key": "all", "label": "All", "aliases": []},
            {"key": "favorites", "label": "Favorites", "aliases": ["favorites", "favorite", "favourites", "my favorite", "myfavorite", "my favoret", "myfavoret"]},
            {"key": "news", "label": "News", "aliases": ["news"]},
            {"key": "tech", "label": "Tech", "aliases": ["tech", "technology"]},
            {"key": "philosophy", "label": "Philosophy", "aliases": ["philosophy", "philo"]},
            {"key": "cinema", "label": "Cinema", "aliases": ["cinema", "movies", "movie", "movise"]},
        ]

        filters = []
        seen_filter_keys = set()
        canonical_family_map = {}
        for item in canonical_filters:
            filter_key = item["key"]
            aliases = [self._normalize_snapshot_filter_key(alias) for alias in item.get("aliases", []) if self._normalize_snapshot_filter_key(alias)]
            if filter_key == "all":
                count = len(feed_videos)
                match_keys = ["all"]
                canonical_family_map[filter_key] = {"match_keys": match_keys, "count": count, "matched_groups": []}
            else:
                match_keys = list(dict.fromkeys([filter_key] + aliases))
                matched_groups = [
                    dict(group)
                    for group_key, group in available_group_map.items()
                    if group_key in match_keys
                ]
                canonical_family_map[filter_key] = {
                    "match_keys": match_keys,
                    "count": 0,
                    "matched_groups": matched_groups,
                }
                count = len([
                    video
                    for video in feed_videos
                    if self._video_matches_snapshot_filter(video, match_keys)
                ])
                canonical_family_map[filter_key]["count"] = count
            filters.append({
                "key": filter_key,
                "label": item["label"],
                "video_count": count,
                "match_keys": match_keys,
                "kind": "canonical",
            })
            seen_filter_keys.add(filter_key)

        for group_key, group in sorted(available_group_map.items(), key=lambda item: item[1]["group_name"].lower()):
            if group_key in seen_filter_keys:
                continue
            hide_as_empty_duplicate = False
            for family in canonical_family_map.values():
                family_match_keys = set(family.get("match_keys", []) or [])
                family_groups = list(family.get("matched_groups", []) or [])
                if group_key not in family_match_keys:
                    continue
                if int(group.get("video_count", 0) or 0) > 0:
                    continue
                if int(family.get("count", 0) or 0) <= 0:
                    continue
                if not any(int(candidate.get("video_count", 0) or 0) > 0 for candidate in family_groups):
                    continue
                hide_as_empty_duplicate = True
                break
            if hide_as_empty_duplicate:
                continue
            filters.append({
                "key": group_key,
                "label": group["group_name"],
                "video_count": int(group.get("video_count", 0) or 0),
                "match_keys": list(group.get("filter_keys", []) or [group_key]),
                "kind": "group",
            })

        selected_record = next((item for item in filters if item.get("key") == selected_key), None)
        if not selected_record:
            selected_key = "all"
            selected_record = next((item for item in filters if item.get("key") == "all"), None)
        selected_record = dict(selected_record or {"key": "all", "label": "All", "video_count": len(feed_videos), "match_keys": ["all"]})
        if selected_key == "all":
            filtered_videos = list(feed_videos)
        else:
            filtered_videos = [
                video
                for video in feed_videos
                if self._video_matches_snapshot_filter(video, selected_record.get("match_keys", []) or [selected_key])
            ]
        return {
            "filters": filters,
            "selected_filter_key": selected_key,
            "selected_filter_record": selected_record,
            "filtered_videos": filtered_videos,
        }

    def _normalize_snapshot_filter_key(self, value):
        return self.normalize_pockettube_group_key(str(value or "").strip())

    def _normalize_display_limit(self, value):
        try:
            parsed = int(value or 0)
        except (TypeError, ValueError):
            parsed = POCKETTUBE_DEFAULT_DISPLAY_LIMIT
        if parsed not in POCKETTUBE_DISPLAY_LIMIT_OPTIONS:
            return POCKETTUBE_DEFAULT_DISPLAY_LIMIT
        return parsed

    def _video_matches_snapshot_filter(self, video, match_keys):
        normalized_keys = {
            self._normalize_snapshot_filter_key(key)
            for key in (match_keys or [])
            if self._normalize_snapshot_filter_key(key)
        }
        if not normalized_keys or "all" in normalized_keys:
            return True
        video_keys = {
            self._normalize_snapshot_filter_key(video.get("group_key", "")),
            self._normalize_snapshot_filter_key(video.get("group_name", "")),
        }
        for name in list(video.get("group_names", []) or []):
            normalized = self._normalize_snapshot_filter_key(name)
            if normalized:
                video_keys.add(normalized)
        for key in list(video.get("group_keys", []) or []):
            normalized = self._normalize_snapshot_filter_key(key)
            if normalized:
                video_keys.add(normalized)
        return bool(video_keys & normalized_keys)

    def _build_snapshot_status(self, snapshot, sync_status, *, has_latest=False):
        snapshot = snapshot if isinstance(snapshot, dict) else {}
        sync_status = sync_status if isinstance(sync_status, dict) else {}
        groups = snapshot.get("groups", {})
        generated_at = str(snapshot.get("generated_at", "") or "").strip()
        synced_at = str(snapshot.get("synced_at", "") or "").strip()
        status = {
            "state": "ok",
            "message": "",
            "is_stale": False,
            "has_snapshot": bool(isinstance(groups, dict) and groups),
        }
        if not status["has_snapshot"]:
            status["state"] = "missing"
            status["message"] = "Snapshot missing. Feed is waiting for the latest cached PocketTube snapshot."
            return status
        if not has_latest:
            status["state"] = "empty"
            status["message"] = "Snapshot loaded, but it has no cached latest videos yet."
            return status

        freshest = synced_at or generated_at
        age_hours = self._timestamp_age_hours(freshest)
        if age_hours is None:
            status["state"] = "unknown"
            status["message"] = "Snapshot loaded, but its freshness timestamp is unavailable."
            return status
        if age_hours >= 24:
            status["state"] = "stale"
            status["is_stale"] = True
            status["message"] = f"Snapshot is {int(age_hours)}h old. Feed is showing cached results."
            return status
        if str(sync_status.get("status", "") or "").strip().lower() in {"requested", "queued", "in_progress"}:
            status["state"] = "updating"
            status["message"] = "Snapshot refresh is in progress. Feed is showing cached results."
            return status
        status["message"] = "Feed is using the latest cached PocketTube snapshot."
        return status

    def _build_refresh_state(self, snapshot, sync_status):
        snapshot = snapshot if isinstance(snapshot, dict) else {}
        sync_status = sync_status if isinstance(sync_status, dict) else {}
        groups = snapshot.get("groups", {})
        has_snapshot = bool(isinstance(groups, dict) and groups)
        last_refreshed_at = str(snapshot.get("synced_at", "") or snapshot.get("generated_at", "") or "").strip()
        refresh_status = str(sync_status.get("status", "") or "").strip().lower()
        if not has_snapshot:
            refresh_status = "missing"
        elif not refresh_status:
            refresh_status = "idle"
        refresh_error = str(sync_status.get("last_error", "") or "").strip()
        age_seconds = self._timestamp_age_seconds(last_refreshed_at)
        return self.refresh_service.build_state(
            last_refreshed_at=last_refreshed_at,
            age_seconds=age_seconds,
            missing=not has_snapshot,
            refresh_status=refresh_status,
            refresh_error=refresh_error,
            refresh_now_enabled=False,
            background_revalidate_enabled=False,
            background_revalidate_placeholder=True,
        )

    def _cached_channel_latest_entry(self, channel_id, cache_data=None):
        channel_id = str(channel_id or "").strip()
        if not channel_id:
            return {}, False, ""
        candidate_keys = []
        normalized_key = self.normalize_pockettube_group_key(channel_id)
        for candidate in (normalized_key, channel_id):
            key = str(candidate or "").strip()
            if key and key not in candidate_keys:
                candidate_keys.append(key)
        if isinstance(cache_data, dict):
            bucket = cache_data.get("youtube_channel_latest_uploads", {})
            if isinstance(bucket, dict):
                for key in candidate_keys:
                    entry = bucket.get(key)
                    if isinstance(entry, dict):
                        data = entry.get("data", {})
                        if isinstance(data, dict):
                            return data, False, key
                return {}, False, candidate_keys[0] if candidate_keys else ""
        for key in candidate_keys:
            payload, stale = self.get_persisted_youtube_channel_latest_entry(key, allow_stale=True)
            if isinstance(payload, dict):
                return payload, bool(stale), key
        return {}, False, candidate_keys[0] if candidate_keys else ""

    def _pockettube_coverage_reason(self, *, group, channels, group_videos, diagnostics, resolved_channels):
        group = group if isinstance(group, dict) else {}
        diagnostics = diagnostics if isinstance(diagnostics, dict) else {}
        channels = [channel for channel in (channels or []) if isinstance(channel, dict)]
        group_videos = [video for video in (group_videos or []) if isinstance(video, dict)]
        resolved_channels = [channel for channel in (resolved_channels or []) if isinstance(channel, dict)]

        if not channels:
            return "no_channels_mapped"
        if group_videos:
            return "covered"

        errors = [str(error or "").strip().lower() for error in list(diagnostics.get("errors", []) or []) if str(error or "").strip()]
        if errors:
            return "fetch_error"

        upload_playlist_ids = [str(channel.get("uploads_playlist_id", "") or "").strip() for channel in resolved_channels if str(channel.get("uploads_playlist_id", "") or "").strip()]
        if not upload_playlist_ids:
            return "no_upload_playlist"

        videos_collected = int(diagnostics.get("videos_collected", 0) or 0)
        videos_stored = int(diagnostics.get("videos_stored", 0) or 0)
        if videos_collected > 0 and videos_stored == 0:
            return "all_duplicates_removed"

        if int(diagnostics.get("channels_missing_upload_playlist", 0) or 0) > 0 and not upload_playlist_ids:
            return "no_upload_playlist"

        if any(not str(video.get("published_at", "") or "").strip() for video in (group.get("videos", []) or []) if isinstance(video, dict)):
            return "missing_published_at"

        if videos_collected == 0 and videos_stored == 0:
            return "empty_uploads"

        return "unknown"

    def build_pockettube_coverage_report(self, scope="", cache_data=None):
        snapshot = self.load_snapshot()
        sync_status = self.load_sync_status()
        admin_data = self.load_admin_data()
        latest_import, imported_sections = self.pockettube_latest_import_snapshot(admin_data=admin_data)
        scope_value = str(scope or "").strip()
        scope_key = self.normalize_pockettube_group_key(scope_value) if scope_value else ""

        snapshot_groups = snapshot.get("groups", {}) if isinstance(snapshot.get("groups", {}), dict) else {}
        report_groups = {}

        def ensure_group(group_key, *, source="snapshot"):
            normalized_key = self.normalize_pockettube_group_key(group_key)
            if not normalized_key:
                return None
            group = report_groups.setdefault(normalized_key, {
                "group_key": normalized_key,
                "display_label": "",
                "source": source,
                "aliases_matched": [],
                "aliases_available": [],
                "channels_assigned": 0,
                "channels": [],
                "upload_playlist_ids": [],
                "videos_fetched_before_dedupe": 0,
                "videos_stored_after_dedupe": 0,
                "errors_per_channel": [],
                "reason": "unknown",
                "notes": [],
                "diagnostics": {},
                "snapshot_present": False,
                "source_present": False,
            })
            if source == "import" and group.get("source") != "snapshot":
                group["source"] = source
            return group

        for group_key, group in snapshot_groups.items():
            if not isinstance(group, dict):
                continue
            normalized_key = self.normalize_pockettube_group_key(group_key or group.get("group_key", "") or group.get("section_key", "") or group.get("group_name", "") or group.get("section_name", ""))
            if not normalized_key:
                continue
            group_report = ensure_group(normalized_key, source="snapshot")
            if group_report is None:
                continue
            group_report["snapshot_present"] = True
            group_report["display_label"] = self.canonical_section_name(
                group.get("group_name", "") or group.get("section_name", "") or normalized_key
            ) or normalized_key
            group_report["videos_fetched_before_dedupe"] = int(
                (group.get("diagnostics", {}) or {}).get("videos_collected", group.get("latest_video_count", 0))
                or 0
            )
            group_report["videos_stored_after_dedupe"] = len([video for video in (group.get("videos", []) or []) if isinstance(video, dict)])
            group_report["diagnostics"] = self._normalize_group_diagnostics(group.get("diagnostics", {}), normalized_key, group_report["display_label"])
            group_report["source_present"] = True
            aliases = [
                str(group.get("group_name", "") or "").strip(),
                str(group.get("section_name", "") or "").strip(),
                str(group.get("group_key", "") or "").strip(),
                str(group.get("section_key", "") or "").strip(),
                normalized_key,
            ]
            group_report["aliases_available"].extend([alias for alias in aliases if alias])

            channels = [channel for channel in (group.get("channels", []) or []) if isinstance(channel, dict)]
            resolved_channels = []
            for channel in channels:
                channel_id = str(channel.get("channel_id", "") or "").strip()
                channel_title = str(channel.get("channel_title", "") or "").strip() or "Unknown Channel"
                cached_entry, stale, cache_key = self._cached_channel_latest_entry(channel_id, cache_data=cache_data)
                uploads_playlist_id = str(cached_entry.get("uploads_playlist_id", "") or "").strip()
                latest_video = cached_entry.get("latest_video", {}) if isinstance(cached_entry.get("latest_video", {}), dict) else {}
                channel_report = {
                    "channel_id": channel_id,
                    "channel_title": channel_title,
                    "cache_key": cache_key,
                    "uploads_playlist_id": uploads_playlist_id,
                    "latest_video_id": str(latest_video.get("video_id", "") or "").strip(),
                    "latest_published_at": str(latest_video.get("published_at", "") or "").strip(),
                    "latest_source": str(cached_entry.get("latest_source", "") or "").strip(),
                    "cache_stale": bool(stale),
                }
                resolved_channels.append(channel_report)
                if uploads_playlist_id and uploads_playlist_id not in group_report["upload_playlist_ids"]:
                    group_report["upload_playlist_ids"].append(uploads_playlist_id)
                if channel_report["latest_video_id"] and not str(channel_report["latest_published_at"] or "").strip():
                    group_report["errors_per_channel"].append({
                        "channel_id": channel_id,
                        "channel_title": channel_title,
                        "error": "missing_published_at",
                    })

            group_report["channels"] = resolved_channels
            group_report["channels_assigned"] = len(resolved_channels)
            group_report["channels_with_upload_playlist"] = sum(1 for channel in resolved_channels if str(channel.get("uploads_playlist_id", "") or "").strip())
            group_report["channels_missing_upload_playlist"] = max(group_report["channels_assigned"] - group_report["channels_with_upload_playlist"], 0)
            group_report["aliases_matched"] = list(dict.fromkeys([
                alias
                for alias in group_report["aliases_available"]
                if alias and self.normalize_pockettube_group_key(alias) == normalized_key
            ]))
            if not group_report["aliases_matched"] and normalized_key:
                group_report["aliases_matched"] = [normalized_key]
            group_report["reason"] = self._pockettube_coverage_reason(
                group=group,
                channels=channels,
                group_videos=group.get("videos", []) or [],
                diagnostics=group_report["diagnostics"],
                resolved_channels=resolved_channels,
            )
            if not group.get("videos", []) and group_report["channels_assigned"] > 0:
                group_report["notes"].append(
                    "snapshot currently has channel-level latest entries but no group video list"
                )
            if group_report["reason"] == "unknown" and group_report["channels_assigned"] > 0 and not group.get("videos", []):
                group_report["notes"].append("coverage is currently limited by the local snapshot shape")

        for section in imported_sections or []:
            if not isinstance(section, dict):
                continue
            section_group_name = self.canonical_section_name(section.get("group_name", "") or section.get("section_name", "") or "")
            section_section_name = self.canonical_section_name(section.get("section_name", "") or section_group_name or "")
            section_group_key = self.normalize_pockettube_group_key(section.get("group_key", "") or section_group_name)
            section_section_key = self.normalize_pockettube_group_key(section.get("section_key", "") or section_section_name)
            normalized_key = next((key for key in (section_group_key, section_section_key, self.normalize_pockettube_group_key(section_group_name), self.normalize_pockettube_group_key(section_section_name)) if key), "")
            if not normalized_key:
                continue
            group_report = ensure_group(normalized_key, source="import")
            if group_report is None:
                continue
            group_report["source_present"] = True
            group_report["display_label"] = group_report["display_label"] or self.canonical_section_name(section_group_name or section_section_name or normalized_key) or normalized_key
            aliases = [
                section_group_name,
                section_section_name,
                str(section.get("group_key", "") or "").strip(),
                str(section.get("section_key", "") or "").strip(),
                normalized_key,
            ]
            group_report["aliases_available"].extend([alias for alias in aliases if alias])
            if group_report["group_key"] == scope_key or normalized_key == scope_key:
                group_report["aliases_matched"] = list(dict.fromkeys(group_report["aliases_matched"] + [alias for alias in aliases if alias and self.normalize_pockettube_group_key(alias) == normalized_key]))

        groups = []
        for group_key, group_report in sorted(report_groups.items(), key=lambda item: (item[1].get("display_label", "") or item[0]).lower()):
            group_report["aliases_available"] = sorted(dict.fromkeys(group_report["aliases_available"]), key=lambda value: str(value or "").lower())
            group_report["aliases_matched"] = sorted(dict.fromkeys(group_report["aliases_matched"]), key=lambda value: str(value or "").lower())
            group_report["upload_playlist_ids"] = sorted(dict.fromkeys(group_report["upload_playlist_ids"]), key=lambda value: str(value or "").lower())
            group_report["errors_per_channel"] = list(group_report["errors_per_channel"])
            if group_report["channels_assigned"] == 0:
                group_report["reason"] = "no_channels_mapped"
            elif group_report["videos_stored_after_dedupe"] == 0 and group_report["reason"] == "covered":
                group_report["reason"] = "unknown"
            groups.append(group_report)

        if scope_key:
            groups = [group for group in groups if self.normalize_pockettube_group_key(group.get("group_key", "")) == scope_key]

        summary = {
            "group_count": len(groups),
            "channel_count": sum(int(group.get("channels_assigned", 0) or 0) for group in groups),
            "groups_without_channels": sum(1 for group in groups if int(group.get("channels_assigned", 0) or 0) == 0),
            "groups_without_videos": sum(1 for group in groups if int(group.get("videos_stored_after_dedupe", 0) or 0) == 0 and int(group.get("channels_assigned", 0) or 0) > 0),
            "upload_playlist_ids_count": sum(len(group.get("upload_playlist_ids", []) or []) for group in groups),
            "channels_with_upload_playlist_count": sum(int(group.get("channels_with_upload_playlist", 0) or 0) for group in groups),
            "channels_missing_upload_playlist_count": sum(int(group.get("channels_missing_upload_playlist", 0) or 0) for group in groups),
            "generated_at": str(snapshot.get("generated_at", "") or "").strip(),
            "synced_at": str(snapshot.get("synced_at", "") or "").strip(),
            "snapshot_path": str(self.snapshot_path),
            "sync_status_path": str(self.sync_status_path),
        }

        return {
            "ok": True,
            "scope": scope_value,
            "summary": summary,
            "groups": groups,
        }

    def _timestamp_age_hours(self, value):
        text = str(value or "").strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        now_text = str(self.current_timestamp() or "").strip()
        if now_text.endswith("Z"):
            now_text = f"{now_text[:-1]}+00:00"
        try:
            now_value = datetime.fromisoformat(now_text)
        except ValueError:
            now_value = datetime.now(timezone.utc)
        if now_value.tzinfo is None:
            now_value = now_value.replace(tzinfo=timezone.utc)
        return max((now_value - parsed).total_seconds() / 3600.0, 0.0)

    def _build_channel_payload(self, channel, group_name, group_key, latest_summary):
        channel_id, channel_title = self._pockettube_channel_identity(channel)
        latest_summary = latest_summary if isinstance(latest_summary, dict) else {}
        latest_exists = bool(latest_summary.get("video_id"))
        published_at = str(latest_summary.get("published_at", "") or "").strip()
        reason_tags = ["source-diverse", "fresh-24h"] if latest_exists else ["source-diverse", "cached-empty"]
        if latest_exists:
            reason_tags.append("latest-cached")
        return {
            "channel_id": channel_id,
            "channel_title": channel_title,
            "group_names": [group_name] if group_name else [],
            "group_key": group_key,
            "latest_video": dict(latest_summary) if latest_exists else {},
            "latest_video_id": str(latest_summary.get("video_id", "") or "").strip(),
            "published_at": published_at,
            "published_display": self.format_timestamp_label(published_at, default="") if published_at else "",
            "thumbnail": str(latest_summary.get("thumbnail", "") or latest_summary.get("thumbnail_url", "") or latest_summary.get("thumb", "") or "").strip(),
            "url": str(latest_summary.get("url", "") or "").strip(),
            "reason_tags": reason_tags,
        }

    def _merge_channel_snapshot(self, channels_snapshot, channel_payload):
        channel_id = str(channel_payload.get("channel_id", "") or "").strip()
        if not channel_id:
            inferred_id, inferred_title = self._pockettube_channel_identity(channel_payload)
            channel_id = str(inferred_id or "").strip()
            if inferred_title and not str(channel_payload.get("channel_title", "") or "").strip():
                channel_payload = dict(channel_payload)
                channel_payload["channel_title"] = inferred_title
        if not channel_id:
            return
        existing = channels_snapshot.get(channel_id)
        if not isinstance(existing, dict):
            channels_snapshot[channel_id] = {
                "channel_title": channel_payload.get("channel_title", ""),
                "latest_video": dict(channel_payload.get("latest_video", {})) if channel_payload.get("latest_video") else {},
                "group_names": list(channel_payload.get("group_names", []) or []),
                "latest_video_id": channel_payload.get("latest_video_id", ""),
                "published_at": channel_payload.get("published_at", ""),
                "published_display": channel_payload.get("published_display", ""),
                "thumbnail": channel_payload.get("thumbnail", ""),
                "url": channel_payload.get("url", ""),
                "reason_tags": list(channel_payload.get("reason_tags", []) or []),
            }
            return

        existing_groups = list(existing.get("group_names", []) or [])
        for group_name in channel_payload.get("group_names", []) or []:
            if group_name and group_name not in existing_groups:
                existing_groups.append(group_name)
        current_sort = self._published_sort_key(existing.get("published_at", ""))
        next_sort = self._published_sort_key(channel_payload.get("published_at", ""))
        existing_latest = existing.get("latest_video", {}) if isinstance(existing.get("latest_video", {}), dict) else {}
        new_latest = channel_payload.get("latest_video", {}) if isinstance(channel_payload.get("latest_video", {}), dict) else {}
        existing_has_latest = bool(existing_latest.get("video_id"))
        new_has_latest = bool(new_latest.get("video_id"))
        if new_has_latest and (not existing_has_latest or next_sort >= current_sort):
            existing["channel_title"] = channel_payload.get("channel_title", existing.get("channel_title", ""))
            existing["latest_video"] = dict(new_latest)
            existing["latest_video_id"] = channel_payload.get("latest_video_id", "")
            existing["published_at"] = channel_payload.get("published_at", "")
            existing["published_display"] = channel_payload.get("published_display", "")
            existing["thumbnail"] = channel_payload.get("thumbnail", "")
            existing["url"] = channel_payload.get("url", "")
            existing["reason_tags"] = list(channel_payload.get("reason_tags", []) or [])
        elif not existing_has_latest and not new_has_latest:
            existing["channel_title"] = channel_payload.get("channel_title", existing.get("channel_title", ""))
        existing["group_names"] = sorted(dict.fromkeys(existing_groups), key=lambda value: str(value or "").lower())

    def _populate_top_level_channels_from_groups(self, snapshot):
        groups = (snapshot or {}).get("groups", {})
        if not isinstance(groups, dict):
            return
        channels_snapshot = (snapshot or {}).setdefault("channels", {})
        if not isinstance(channels_snapshot, dict):
            snapshot["channels"] = {}
            channels_snapshot = snapshot["channels"]
        for group_key, group in groups.items():
            if not isinstance(group, dict):
                continue
            group_name = str(group.get("group_name", "") or group.get("section_name", "") or group_key or "").strip() or str(group_key or "").strip()
            for channel in group.get("channels", []) or []:
                if not isinstance(channel, dict):
                    continue
                channel_payload = dict(channel)
                channel_payload.setdefault("group_names", [])
                group_names = list(channel_payload.get("group_names", []) or [])
                if group_name and group_name not in group_names:
                    group_names.append(group_name)
                channel_payload["group_names"] = group_names
                self._merge_channel_snapshot(channels_snapshot, channel_payload)

    def _latest_video_for_channel(self, channel_id):
        cache_key = str(channel_id or "").strip()
        if not cache_key:
            return {}
        payload, _stale = self.get_persisted_youtube_channel_latest_entry(cache_key, allow_stale=True)
        if not isinstance(payload, dict):
            return {}
        latest_video = payload.get("latest_video", {})
        return latest_video if isinstance(latest_video, dict) else {}

    def _normalize_group_videos(self, videos, group_name, group_key, limit=POCKETTUBE_GROUP_VIDEO_LIMIT, prefer_snapshot_fields=False):
        deduped = {}
        for video in videos or []:
            normalized_video = self._normalize_group_video(video, group_name, group_key, prefer_snapshot_fields=prefer_snapshot_fields)
            video_id = str(normalized_video.get("video_id", "") or "").strip()
            if not video_id:
                continue
            existing = deduped.get(video_id)
            if existing and self._published_sort_key(existing.get("published_at", "")) > self._published_sort_key(normalized_video.get("published_at", "")):
                continue
            deduped[video_id] = normalized_video
        ordered = list(deduped.values())
        ordered.sort(key=lambda item: (
            -self._published_sort_key(str(item.get("published_at", "") or "")),
            str(item.get("title", "") or "").lower(),
            str(item.get("channel_title", "") or "").lower(),
            str(item.get("video_id", "") or "").lower(),
        ))
        if limit and limit > 0:
            ordered = ordered[:limit]
        return ordered

    def _normalize_group_video(self, video, group_name, group_key, prefer_snapshot_fields=False):
        if not isinstance(video, dict):
            return {}
        merged = dict(video)
        video_id = str(merged.get("video_id", "") or "").strip()
        if not video_id or not prefer_snapshot_fields:
            summary = self.build_youtube_channel_video_summary(video)
            if not isinstance(summary, dict):
                summary = {}
            merged.update(summary)
            video_id = str(merged.get("video_id", "") or "").strip()
        if not video_id:
            return {}
        channel_id = str(merged.get("channel_id", "") or video.get("channel_id", "") or "").strip()
        channel_title = str(
            merged.get("channel_title", "")
            or merged.get("channel_name", "")
            or video.get("channel_title", "")
            or video.get("channel_name", "")
            or channel_id
        ).strip() or channel_id or "Unknown Channel"
        published_at = str(merged.get("published_at", "") or video.get("published_at", "") or "").strip()
        thumbnail = str(
            merged.get("thumbnail", "")
            or merged.get("thumbnail_url", "")
            or merged.get("image_url", "")
            or merged.get("thumb", "")
            or video.get("thumbnail", "")
            or video.get("thumbnail_url", "")
            or video.get("image_url", "")
            or video.get("thumb", "")
            or ""
        ).strip()
        group_names = list(dict.fromkeys([
            str(name or "").strip()
            for name in list(merged.get("group_names", []) or [])
            + list(video.get("group_names", []) or [])
            + ([group_name] if group_name else [])
            if str(name or "").strip()
        ]))
        group_keys = list(dict.fromkeys([
            str(key or "").strip()
            for key in list(merged.get("group_keys", []) or [])
            + list(video.get("group_keys", []) or [])
            + ([group_key] if group_key else [])
            if str(key or "").strip()
        ]))
        watch_key = str(merged.get("watch_key", "") or video.get("watch_key", "") or video_id).strip() or video_id
        return {
            "title": str(merged.get("title", "") or merged.get("name", "") or "Untitled video").strip() or "Untitled video",
            "entry_id": str(merged.get("entry_id", "") or video.get("entry_id", "") or f"yt-{video_id}").strip() or f"yt-{video_id}",
            "video_id": video_id,
            "watch_key": watch_key,
            "state_key": str(merged.get("state_key", "") or video.get("state_key", "") or watch_key).strip() or watch_key,
            "channel_id": channel_id,
            "channel_name": channel_title,
            "channel_title": channel_title,
            "published_at": published_at,
            "published_display": str(merged.get("published_display", "") or video.get("published_display", "") or "").strip() or (self.format_timestamp_label(published_at, default="") if published_at else ""),
            "thumbnail": thumbnail,
            "thumbnail_url": str(merged.get("thumbnail_url", "") or video.get("thumbnail_url", "") or thumbnail).strip(),
            "image_url": str(merged.get("image_url", "") or video.get("image_url", "") or thumbnail).strip(),
            "thumb": str(merged.get("thumb", "") or video.get("thumb", "") or thumbnail).strip(),
            "detail_url": str(merged.get("detail_url", "") or video.get("detail_url", "") or f"/video/yt-{video_id}").strip() or f"/video/yt-{video_id}",
            "url": str(merged.get("url", "") or video.get("url", "") or f"https://www.youtube.com/watch?v={video_id}").strip() or f"https://www.youtube.com/watch?v={video_id}",
            "group_name": str(group_name or "").strip(),
            "group_key": str(group_key or "").strip(),
            "group_names": group_names,
            "group_keys": group_keys,
            "reason_tags": list(dict.fromkeys([
                str(tag or "").strip()
                for tag in list(merged.get("reason_tags", []) or []) + list(video.get("reason_tags", []) or []) + ["cached-latest"]
                if str(tag or "").strip()
            ])),
        }

    def _normalize_group_diagnostics(self, diagnostics, group_key, group_name):
        diagnostics = diagnostics if isinstance(diagnostics, dict) else {}
        return {
            "group_key": str(diagnostics.get("group_key", "") or group_key or "").strip(),
            "group_name": str(diagnostics.get("group_name", "") or group_name or "").strip(),
            "channels_scanned": int(diagnostics.get("channels_scanned", 0) or 0),
            "channels_fetched": int(diagnostics.get("channels_fetched", 0) or 0),
            "channels_with_upload_playlist": int(diagnostics.get("channels_with_upload_playlist", 0) or 0),
            "channels_missing_upload_playlist": int(diagnostics.get("channels_missing_upload_playlist", 0) or 0),
            "videos_collected": int(diagnostics.get("videos_collected", 0) or 0),
            "videos_stored": int(diagnostics.get("videos_stored", 0) or 0),
            "per_channel_candidate_limit": int(diagnostics.get("per_channel_candidate_limit", 0) or 0),
            "initial_per_channel_candidate_limit": int(diagnostics.get("initial_per_channel_candidate_limit", 0) or 0),
            "candidate_limit_schedule": [int(item) for item in list(diagnostics.get("candidate_limit_schedule", []) or []) if int(item or 0) > 0],
            "total_candidates_before_dedupe": int(diagnostics.get("total_candidates_before_dedupe", diagnostics.get("videos_collected", 0)) or 0),
            "upload_playlist_ids": list(dict.fromkeys([str(item or "").strip() for item in diagnostics.get("upload_playlist_ids", []) or [] if str(item or "").strip()])),
            "errors": list(diagnostics.get("errors", []) or []),
            "generated_at": str(diagnostics.get("generated_at", "") or "").strip(),
            "synced_at": str(diagnostics.get("synced_at", "") or "").strip(),
        }

    def _build_group_sync_diagnostics(self, group_key, group_name, latest_result, videos_stored):
        latest_result = latest_result if isinstance(latest_result, dict) else {}
        diagnostics = self._normalize_group_diagnostics(latest_result.get("diagnostics", {}), group_key, group_name)
        diagnostics["channels_scanned"] = int(
            latest_result.get("channels_scanned", latest_result.get("channels_fetched", diagnostics.get("channels_scanned", 0)))
            or 0
        )
        diagnostics["channels_fetched"] = int(
            latest_result.get("channels_fetched", diagnostics.get("channels_fetched", 0))
            or 0
        )
        diagnostics["videos_collected"] = int(
            latest_result.get("videos_collected", latest_result.get("latest_videos_found", diagnostics.get("videos_collected", 0)))
            or 0
        )
        diagnostics["videos_stored"] = int(videos_stored or latest_result.get("videos_stored", diagnostics.get("videos_stored", 0)) or 0)
        diagnostics["per_channel_candidate_limit"] = int(
            latest_result.get("per_channel_candidate_limit", diagnostics.get("per_channel_candidate_limit", 0))
            or 0
        )
        diagnostics["initial_per_channel_candidate_limit"] = int(
            latest_result.get("initial_per_channel_candidate_limit", diagnostics.get("initial_per_channel_candidate_limit", 0))
            or 0
        )
        diagnostics["candidate_limit_schedule"] = [
            int(item)
            for item in list(latest_result.get("candidate_limit_schedule", diagnostics.get("candidate_limit_schedule", [])) or [])
            if int(item or 0) > 0
        ]
        diagnostics["total_candidates_before_dedupe"] = int(
            latest_result.get("total_candidates_before_dedupe", diagnostics.get("total_candidates_before_dedupe", diagnostics.get("videos_collected", 0)))
            or 0
        )
        diagnostics["errors"] = list(dict.fromkeys([
            str(error or "").strip()
            for error in list(diagnostics.get("errors", []) or []) + list(latest_result.get("errors", []) or [])
            if str(error or "").strip()
        ]))
        generated_at = str(latest_result.get("generated_at", "") or latest_result.get("fetched_at", "") or diagnostics.get("generated_at", "") or "").strip()
        synced_at = str(latest_result.get("synced_at", "") or latest_result.get("fetched_at", "") or diagnostics.get("synced_at", "") or generated_at).strip()
        diagnostics["generated_at"] = generated_at
        diagnostics["synced_at"] = synced_at
        return diagnostics

    def _normalize_snapshot(self, payload):
        snapshot = self.empty_snapshot()
        if not isinstance(payload, dict):
            return snapshot
        snapshot["version"] = max(int(payload.get("version", 2) or 2), 2)
        snapshot["generated_at"] = str(payload.get("generated_at", "") or "").strip()
        snapshot["synced_at"] = str(payload.get("synced_at", "") or "").strip()
        snapshot["group_video_limit"] = int(payload.get("group_video_limit", POCKETTUBE_GROUP_VIDEO_LIMIT) or POCKETTUBE_GROUP_VIDEO_LIMIT)
        snapshot["all_feed_video_limit"] = int(payload.get("all_feed_video_limit", POCKETTUBE_ALL_FEED_VIDEO_LIMIT) or POCKETTUBE_ALL_FEED_VIDEO_LIMIT)
        snapshot["warnings"] = list(payload.get("warnings", []) or [])
        snapshot["errors"] = list(payload.get("errors", []) or [])
        snapshot["groups"] = {}
        for group_key, group in (payload.get("groups", {}) or {}).items():
            if not isinstance(group, dict):
                continue
            normalized_group_key = self.normalize_pockettube_group_key(group.get("group_key", "") or group_key)
            group_name = self._group_display_name(group)
            channels = []
            for channel in group.get("channels", []) or []:
                if isinstance(channel, dict):
                    channels.append(self._normalize_channel_payload(channel, group_name, normalized_group_key))
            videos = self._normalize_group_videos(
                group.get("videos", []) or [],
                group_name,
                normalized_group_key,
                prefer_snapshot_fields=int(snapshot.get("version", 2) or 2) >= 2,
            )
            latest_video = group.get("latest_video", {}) if isinstance(group.get("latest_video", {}), dict) else {}
            normalized_latest_video = self._normalize_group_video(latest_video, group_name, normalized_group_key) if latest_video else {}
            snapshot["groups"][normalized_group_key] = {
                "group_name": group_name,
                "group_key": normalized_group_key,
                "section_name": str(group.get("section_name", "") or group_name).strip() or group_name,
                "section_key": self.normalize_pockettube_group_key(group.get("section_key", "") or group_name),
                "source_name": str(group.get("source_name", "") or "PocketTube").strip() or "PocketTube",
                "imported_at": str(group.get("imported_at", "") or "").strip(),
                "channel_count": int(group.get("channel_count", len(channels)) or 0),
                "latest_video_count": int(group.get("latest_video_count", len(videos)) or 0),
                "latest_video": normalized_latest_video or (dict(videos[0]) if videos else {}),
                "channels": channels,
                "videos": videos,
                "diagnostics": self._normalize_group_diagnostics(group.get("diagnostics", {}), normalized_group_key, group_name),
            }
        snapshot["channels"] = {}
        for channel_id, channel in (payload.get("channels", {}) or {}).items():
            if not isinstance(channel, dict):
                continue
            enriched_channel = dict(channel)
            enriched_channel.setdefault("channel_id", channel_id)
            normalized_channel = self._normalize_channel_entry(enriched_channel, channel_id_hint=channel_id)
            if normalized_channel:
                snapshot["channels"][channel_id] = normalized_channel
        return snapshot

    def _normalize_channel_payload(self, channel, group_name, group_key):
        latest_video = channel.get("latest_video", {})
        latest_video = latest_video if isinstance(latest_video, dict) else {}
        return {
            "channel_id": str(channel.get("channel_id", "") or "").strip(),
            "channel_title": str(channel.get("channel_title", "") or "").strip(),
            "group_names": list(dict.fromkeys([str(name or "").strip() for name in channel.get("group_names", []) or [] if str(name or "").strip()] + ([group_name] if group_name else []))),
            "group_key": group_key,
            "latest_video": latest_video,
            "latest_video_id": str(channel.get("latest_video_id", "") or latest_video.get("video_id", "") or "").strip(),
            "published_at": str(channel.get("published_at", "") or latest_video.get("published_at", "") or "").strip(),
            "published_display": str(channel.get("published_display", "") or "").strip(),
            "thumbnail": str(channel.get("thumbnail", "") or latest_video.get("thumbnail", "") or latest_video.get("thumbnail_url", "") or latest_video.get("thumb", "") or "").strip(),
            "url": str(channel.get("url", "") or latest_video.get("url", "") or "").strip(),
            "reason_tags": list(channel.get("reason_tags", []) or []),
        }

    def _normalize_channel_entry(self, channel, channel_id_hint=""):
        channel_id = str(channel.get("channel_id", "") or channel_id_hint or "").strip()
        if not channel_id:
            inferred_id, inferred_title = self._pockettube_channel_identity(channel)
            channel_id = str(inferred_id or "").strip()
            if inferred_title and not str(channel.get("channel_title", "") or "").strip():
                channel = dict(channel)
                channel["channel_title"] = inferred_title
        if not channel_id:
            return None
        latest_video = channel.get("latest_video", {})
        return {
            "channel_id": channel_id,
            "channel_title": str(channel.get("channel_title", "") or "").strip(),
            "latest_video": latest_video if isinstance(latest_video, dict) else {},
            "group_names": list(dict.fromkeys([str(name or "").strip() for name in channel.get("group_names", []) or [] if str(name or "").strip()])),
            "latest_video_id": str(channel.get("latest_video_id", "") or "").strip(),
            "published_at": str(channel.get("published_at", "") or "").strip(),
            "published_display": str(channel.get("published_display", "") or "").strip(),
            "thumbnail": str(channel.get("thumbnail", "") or "").strip(),
            "url": str(channel.get("url", "") or "").strip(),
            "reason_tags": list(channel.get("reason_tags", []) or []),
        }

    def _filter_sections_for_scope(self, sections, scope):
        scope_value = str(scope or "").strip()
        if not scope_value:
            return list(sections or [])
        wanted = self.normalize_pockettube_group_key(scope_value)
        filtered = []
        for section in sections or []:
            if not isinstance(section, dict):
                continue
            candidates = {
                self.normalize_pockettube_group_key(section.get("group_key", "")),
                self.normalize_pockettube_group_key(section.get("section_key", "")),
                self.normalize_pockettube_group_key(section.get("group_name", "")),
                self.normalize_pockettube_group_key(section.get("section_name", "")),
            }
            if wanted in {candidate for candidate in candidates if candidate}:
                filtered.append(section)
        return filtered or list(sections or [])

    def _group_display_name(self, section):
        if isinstance(section, dict):
            return self.canonical_section_name(section.get("group_name", "") or section.get("section_name", "") or "")
        return ""

    def _section_sort_key(self, section):
        return (
            int(section.get("tier", "best") != "best"),
            self._group_display_name(section).lower(),
            str(section.get("section_name", "") or "").lower(),
            str(section.get("group_name", "") or "").lower(),
        )

    def _sync_notice(self, sync_status):
        status = str((sync_status or {}).get("status", "") or "").strip().lower()
        if status == "requested":
            return "Sync requested."
        if status == "queued":
            return "Sync queued in GitHub Actions."
        if status == "in_progress":
            return "Sync is in progress."
        if status == "completed":
            return "Last sync completed."
        if status == "failed":
            return "Last sync failed. Run YouTube freshness sync again if needed."
        return ""

    def _build_freshness_note(self, snapshot_status, refresh_state, sync_status):
        snapshot_status = snapshot_status if isinstance(snapshot_status, dict) else {}
        sync_status = sync_status if isinstance(sync_status, dict) else {}
        refresh_status = str(getattr(refresh_state, "refresh_status", "") or "").strip().lower()
        stale_state = str((getattr(getattr(refresh_state, "stale", None), "state", "") or "")).strip().lower()
        is_stale = bool(getattr(getattr(refresh_state, "stale", None), "is_stale", False) or snapshot_status.get("is_stale"))
        has_snapshot = bool(snapshot_status.get("has_snapshot"))
        last_refreshed_at = str(getattr(refresh_state, "last_refreshed_at", "") or "").strip()
        last_refreshed_at_display = str(getattr(refresh_state, "last_refreshed_at_display", "") or "").strip()

        if not has_snapshot or refresh_status == "missing" or stale_state == "missing":
            return {
                "state": "missing",
                "title": "No local YouTube freshness snapshot yet",
                "message": "No local YouTube freshness snapshot yet. Run YouTube freshness sync, then reload this page.",
                "last_refreshed_at": "",
                "last_refreshed_at_display": "",
                "secondary_message": "",
            }

        if refresh_status == "failed":
            return {
                "state": "error",
                "title": "Refresh error",
                "message": "Last refresh failed. Run YouTube freshness sync again if needed.",
                "last_refreshed_at": last_refreshed_at,
                "last_refreshed_at_display": last_refreshed_at_display if last_refreshed_at else "",
                "secondary_message": "",
            }

        if is_stale or stale_state == "stale":
            return {
                "state": "stale",
                "title": "Snapshot may be stale",
                "message": "Snapshot may be stale. Run YouTube freshness sync if you need the latest videos.",
                "last_refreshed_at": last_refreshed_at,
                "last_refreshed_at_display": last_refreshed_at_display if last_refreshed_at else "",
                "secondary_message": "",
            }

        freshness_message = "Fresh snapshot."
        if last_refreshed_at_display and last_refreshed_at_display != "Unknown":
            freshness_message = f"Fresh snapshot. Last refreshed {last_refreshed_at_display}."
        return {
            "state": "fresh",
            "title": "Fresh snapshot",
            "message": freshness_message,
            "last_refreshed_at": last_refreshed_at,
            "last_refreshed_at_display": last_refreshed_at_display if last_refreshed_at else "",
            "secondary_message": "",
        }

    def _normalize_sync_status(self, payload):
        status = self.empty_sync_status()
        if not isinstance(payload, dict):
            return status
        normalized = str(payload.get("status", "") or "idle").strip().lower() or "idle"
        if normalized not in {"idle", "requested", "queued", "in_progress", "completed", "failed"}:
            normalized = "idle"
        status.update({
            "status": normalized,
            "requested_at": str(payload.get("requested_at", "") or "").strip(),
            "started_at": str(payload.get("started_at", "") or "").strip(),
            "completed_at": str(payload.get("completed_at", "") or "").strip(),
            "last_error": str(payload.get("last_error", "") or "").strip(),
            "warnings": list(payload.get("warnings", []) or []),
            "scope": str(payload.get("scope", "") or "").strip(),
            "run_id": str(payload.get("run_id", "") or "").strip(),
            "run_url": str(payload.get("run_url", "") or "").strip(),
            "source": str(payload.get("source", "") or "").strip(),
            "updated_at": str(payload.get("updated_at", "") or "").strip(),
        })
        return status

    def _published_sort_key(self, value):
        text = str(value or "").strip()
        if not text:
            return 0
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return 0
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp())

    def _timestamp_age_seconds(self, value):
        timestamp = str(value or "").strip()
        if not timestamp:
            return None
        now_value = str(self.current_timestamp() or "").strip()
        now_timestamp = self._published_sort_key(now_value)
        value_timestamp = self._published_sort_key(timestamp)
        if not now_timestamp or not value_timestamp:
            return None
        return max(0, int(now_timestamp - value_timestamp))

    def _github_snapshot_download_error_message(self, exc):
        message = str(exc or "").strip()
        if "GitHub snapshot file missing:" in message:
            return message
        if "GitHub returned status 404" in message or "404" in message:
            return "GitHub snapshot file missing: cache/youtube_latest_snapshot.json"
        if message:
            return f"Download failed: {message}"
        return "Download failed: unable to refresh youtube_latest_snapshot.json"
