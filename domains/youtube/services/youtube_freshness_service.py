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
        registry_path=None,
        app_logger,
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
        self.registry_path = Path(registry_path) if registry_path else Path(__file__).resolve().parents[1] / "data" / "pockettube_registry.json"
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

    def empty_sync_status(self):
        return {
            "status": "idle",
            "requested_at": "",
            "started_at": "",
            "completed_at": "",
            "last_error": "",
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
            latest_items = [item for item in (latest_result.get("latest_items", []) or []) if isinstance(item, dict)]
            if not latest_items:
                continue
            normalized_group_key = self.normalize_pockettube_group_key(group_key)
            normalized_latest_results[normalized_group_key] = latest_items
            result_group_name = str(latest_result.get("group_name", "") or latest_result.get("section_name", "") or "").strip()
            if result_group_name:
                normalized_latest_results[self.normalize_pockettube_group_key(result_group_name)] = latest_items

        for group_key, group in groups.items():
            if not isinstance(group, dict):
                continue
            group_name = str(group.get("group_name", "") or group.get("section_name", "") or group_key or "").strip() or str(group_key or "").strip()
            latest_items = (
                normalized_latest_results.get(self.normalize_pockettube_group_key(group_key))
                or normalized_latest_results.get(self.normalize_pockettube_group_key(group_name))
                or []
            )
            if not latest_items:
                continue

            latest_by_channel = {}
            for item in latest_items:
                latest_summary = self.build_youtube_channel_video_summary(item)
                if not isinstance(latest_summary, dict):
                    continue
                channel_id = str(latest_summary.get("channel_id", "") or item.get("channel_id", "") or "").strip()
                if not channel_id:
                    channel_id, inferred_title = self._pockettube_channel_identity(item)
                    if inferred_title and not str(latest_summary.get("channel_name", "") or latest_summary.get("channel_title", "") or "").strip():
                        latest_summary["channel_name"] = inferred_title
                if not channel_id or not str(latest_summary.get("video_id", "") or "").strip():
                    continue
                latest_summary["channel_id"] = channel_id
                latest_summary["channel_name"] = str(
                    latest_summary.get("channel_name", "")
                    or latest_summary.get("channel_title", "")
                    or item.get("channel_name", "")
                    or item.get("channel_title", "")
                    or channel_id
                ).strip() or channel_id
                latest_summary["thumbnail"] = str(
                    latest_summary.get("thumbnail", "")
                    or latest_summary.get("thumbnail_url", "")
                    or latest_summary.get("thumb", "")
                    or item.get("thumbnail", "")
                    or item.get("thumbnail_url", "")
                    or item.get("thumb", "")
                    or ""
                ).strip()
                latest_summary["url"] = str(latest_summary.get("url", "") or item.get("url", "") or "").strip()
                latest_by_channel[channel_id] = latest_summary

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
            group["latest_video_count"] = sum(
                1
                for item in group_channels
                if isinstance(item, dict) and isinstance(item.get("latest_video"), dict) and item["latest_video"].get("video_id")
            )
            group["latest_video"] = group_latest_video if isinstance(group_latest_video, dict) else {}

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
        group_channels_total = sum(len(group.get("channels", []) or []) for group in snapshot.get("groups", {}).values() if isinstance(group, dict))
        latest_videos_total = sum(
            1
            for group in snapshot.get("groups", {}).values()
            if isinstance(group, dict)
            for channel in group.get("channels", []) or []
            if isinstance(channel, dict) and isinstance(channel.get("latest_video", {}), dict) and channel["latest_video"].get("video_id")
        )
        self.app_logger.info(
            "youtube_freshness_snapshot_finalized groups=%s group_channels=%s channels=%s latest_videos=%s errors=%s",
            len(snapshot.get("groups", {}) or {}),
            group_channels_total,
            len(snapshot.get("channels", {}) or {}),
            latest_videos_total,
            len(errors or []),
        )
        self.save_snapshot(snapshot)
        self.save_sync_status({
            "status": "completed",
            "requested_at": self.load_sync_status().get("requested_at", ""),
            "started_at": self.load_sync_status().get("started_at", ""),
            "completed_at": self.current_timestamp(),
            "last_error": "",
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
            snapshot = self.refresh_snapshot_from_github()
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
        snapshot = self.load_snapshot()
        sync_status = self.load_sync_status()
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
        feed_video_count = len(feed_videos)
        empty_channel_count = len(empty_channels)
        empty_group_count = len([group for group in feed_groups if int(group.get("empty_channel_count", 0) or 0) > 0])
        has_latest = bool(feed_video_count)
        return {
            "title": "PocketTube Freshness",
            "snapshot": snapshot,
            "sync_status": sync_status,
            "groups": groups,
            "group_count": len(groups),
            "channel_count": sum(len(group.get("channels", [])) for group in groups),
            "feed_videos": feed_videos,
            "feed_groups": feed_groups,
            "feed_video_count": feed_video_count,
            "feed_empty_channels": empty_channels,
            "feed_empty_channel_count": empty_channel_count,
            "feed_empty_group_count": empty_group_count,
            "has_latest": has_latest,
            "generated_at": snapshot.get("generated_at", ""),
            "synced_at": snapshot.get("synced_at", ""),
            "errors": list(snapshot.get("errors", []) or []),
            "empty_state": not has_latest,
            "empty_reason": "no_snapshot" if not snapshot.get("groups") else "no_cached_latest",
            "sync_notice": self._sync_notice(sync_status),
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
            return str((sync_status or {}).get("last_error", "") or "Last sync failed.") or "Last sync failed."
        return ""

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

    def _github_snapshot_download_error_message(self, exc):
        message = str(exc or "").strip()
        if "GitHub snapshot file missing:" in message:
            return message
        if "GitHub returned status 404" in message or "404" in message:
            return "GitHub snapshot file missing: cache/youtube_latest_snapshot.json"
        if message:
            return f"Download failed: {message}"
        return "Download failed: unable to refresh youtube_latest_snapshot.json"
