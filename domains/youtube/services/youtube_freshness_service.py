from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import re


class YouTubeFreshnessService:
    def __init__(
        self,
        *,
        load_admin_data,
        pockettube_latest_import_snapshot,
        get_persisted_youtube_channel_latest_entry,
        refresh_pockettube_section_latest_uploads,
        build_youtube_channel_video_summary,
        canonical_section_name,
        normalize_pockettube_group_key,
        format_timestamp_label,
        current_timestamp,
        load_json_file,
        save_json_file,
        snapshot_path,
        app_logger,
    ):
        self.load_admin_data = load_admin_data
        self.pockettube_latest_import_snapshot = pockettube_latest_import_snapshot
        self.get_persisted_youtube_channel_latest_entry = get_persisted_youtube_channel_latest_entry
        self.refresh_pockettube_section_latest_uploads = refresh_pockettube_section_latest_uploads
        self.build_youtube_channel_video_summary = build_youtube_channel_video_summary
        self.canonical_section_name = canonical_section_name
        self.normalize_pockettube_group_key = normalize_pockettube_group_key
        self.format_timestamp_label = format_timestamp_label
        self.current_timestamp = current_timestamp
        self.load_json_file = load_json_file
        self.save_json_file = save_json_file
        self.snapshot_path = Path(snapshot_path)
        self.app_logger = app_logger

    def empty_snapshot(self):
        return {
            "version": 1,
            "generated_at": "",
            "synced_at": "",
            "groups": {},
            "channels": {},
            "errors": [],
        }

    def load_snapshot(self):
        payload = self.load_json_file(self.snapshot_path, self.empty_snapshot())
        return self._normalize_snapshot(payload)

    def save_snapshot(self, snapshot):
        payload = self._normalize_snapshot(snapshot)
        self.save_json_file(self.snapshot_path, payload)
        return payload

    def build_snapshot_from_local_cache(self, admin_data=None, latest_import=None, sections=None, errors=None):
        admin_data = admin_data if isinstance(admin_data, dict) else self.load_admin_data()
        latest_import = latest_import if isinstance(latest_import, dict) else {}
        if sections is None:
            _, sections = self.pockettube_latest_import_snapshot(admin_data)
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
        snapshot["channels"] = dict(sorted(snapshot["channels"].items(), key=lambda item: item[0].lower()))
        return snapshot

    def sync_snapshot(self, scope="", max_channels=200):
        admin_data = self.load_admin_data()
        latest_import, sections = self.pockettube_latest_import_snapshot(admin_data)
        filtered_sections = self._filter_sections_for_scope(sections, scope)
        errors = []
        for section in filtered_sections:
            group_name = self._group_display_name(section)
            try:
                self.refresh_pockettube_section_latest_uploads(
                    group_name,
                    admin_data=admin_data,
                    max_channels=max_channels,
                )
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
        self.save_snapshot(snapshot)
        return snapshot

    def build_page_context(self):
        snapshot = self.load_snapshot()
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
        return {
            "title": "PocketTube Freshness",
            "snapshot": snapshot,
            "groups": groups,
            "group_count": len(groups),
            "channel_count": sum(len(group.get("channels", [])) for group in groups),
            "has_latest": has_latest,
            "generated_at": snapshot.get("generated_at", ""),
            "synced_at": snapshot.get("synced_at", ""),
            "errors": list(snapshot.get("errors", []) or []),
            "empty_state": not has_latest,
            "empty_reason": "no_snapshot" if not snapshot.get("groups") else "no_cached_latest",
            "sync_notice": "",
        }

    def _build_channel_payload(self, channel, group_name, group_key, latest_summary):
        channel_id = str(channel.get("channel_id", "") or "").strip()
        channel_title = str(channel.get("channel_name", "") or channel.get("channel_title", "") or "").strip() or channel_id or "Unknown Channel"
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
        if next_sort >= current_sort:
            existing["channel_title"] = channel_payload.get("channel_title", existing.get("channel_title", ""))
            existing["latest_video"] = dict(channel_payload.get("latest_video", {})) if channel_payload.get("latest_video") else {}
            existing["latest_video_id"] = channel_payload.get("latest_video_id", "")
            existing["published_at"] = channel_payload.get("published_at", "")
            existing["published_display"] = channel_payload.get("published_display", "")
            existing["thumbnail"] = channel_payload.get("thumbnail", "")
            existing["url"] = channel_payload.get("url", "")
            existing["reason_tags"] = list(channel_payload.get("reason_tags", []) or [])
        existing["group_names"] = sorted(dict.fromkeys(existing_groups), key=lambda value: str(value or "").lower())

    def _latest_video_for_channel(self, channel_id):
        cache_key = str(channel_id or "").strip()
        if not cache_key:
            return {}
        payload, _stale = self.get_persisted_youtube_channel_latest_entry(cache_key, allow_stale=True)
        if not isinstance(payload, dict):
            return {}
        latest_video = payload.get("latest_video", {})
        return latest_video if isinstance(latest_video, dict) else {}

    def _normalize_snapshot(self, payload):
        snapshot = self.empty_snapshot()
        if not isinstance(payload, dict):
            return snapshot
        snapshot["version"] = int(payload.get("version", 1) or 1)
        snapshot["generated_at"] = str(payload.get("generated_at", "") or "").strip()
        snapshot["synced_at"] = str(payload.get("synced_at", "") or "").strip()
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
            snapshot["groups"][normalized_group_key] = {
                "group_name": group_name,
                "group_key": normalized_group_key,
                "section_name": str(group.get("section_name", "") or group_name).strip() or group_name,
                "section_key": self.normalize_pockettube_group_key(group.get("section_key", "") or group_name),
                "source_name": str(group.get("source_name", "") or "PocketTube").strip() or "PocketTube",
                "imported_at": str(group.get("imported_at", "") or "").strip(),
                "channel_count": int(group.get("channel_count", len(channels)) or 0),
                "latest_video_count": int(group.get("latest_video_count", 0) or 0),
                "latest_video": group.get("latest_video", {}) if isinstance(group.get("latest_video", {}), dict) else {},
                "channels": channels,
            }
        snapshot["channels"] = {}
        for channel_id, channel in (payload.get("channels", {}) or {}).items():
            if not isinstance(channel, dict):
                continue
            normalized_channel = self._normalize_channel_entry(channel)
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

    def _normalize_channel_entry(self, channel):
        channel_id = str(channel.get("channel_id", "") or "").strip()
        if not channel_id:
            return None
        latest_video = channel.get("latest_video", {})
        return {
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
