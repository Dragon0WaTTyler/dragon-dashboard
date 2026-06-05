import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import app as dragon_app
from domains.reading import ReadingRuntimeService
from domains.youtube.services.youtube_freshness_service import YouTubeFreshnessService
from dragon.wiring import build_reading_runtime_service, build_refresh_service, build_youtube_freshness_service


class AppWiringTests(unittest.TestCase):
    def test_build_reading_runtime_service_from_wiring_module(self):
        service = build_reading_runtime_service(
            app_logger=dragon_app.app.logger,
            load_reading_data_cached=dragon_app.load_reading_data_cached,
            default_reading_data=dragon_app.default_reading_data,
            reading_data_path=dragon_app.READING_DATA_PATH,
            normalize_reading_source=dragon_app.normalize_reading_source,
            normalize_reading_url=dragon_app.normalize_reading_url,
            absolutize_reading_url=dragon_app.absolutize_reading_url,
            reading_hash_key=dragon_app.reading_hash_key,
            reading_runtime_projection_service=dragon_app._get_reading_runtime_projection_service(),
            normalize_reading_list_entry=dragon_app.normalize_reading_list_entry,
            parse_timestamp=dragon_app.parse_timestamp,
            normalize_reading_category=dragon_app.normalize_reading_category,
            normalize_reading_status=dragon_app.normalize_reading_status,
            reading_visible_topic_label=dragon_app.reading_visible_topic_label,
            reading_short_text_direction=dragon_app.reading_short_text_direction,
            reading_title_direction=dragon_app.reading_title_direction,
            reading_entry_matches_filters=dragon_app.reading_entry_matches_filters,
            reading_entry_sort_key=dragon_app.reading_entry_sort_key,
            reading_category_label=dragon_app.reading_category_label,
            format_timestamp_label=dragon_app.format_timestamp_label,
            reading_statuses=dragon_app.READING_STATUSES,
            reading_categories=dragon_app.READING_CATEGORIES,
            reading_list_default_limit=dragon_app.READING_LIST_DEFAULT_LIMIT,
            reading_list_limit_max=dragon_app.READING_LIST_LIMIT_MAX,
            reading_list_limit_step=dragon_app.READING_LIST_LIMIT_STEP,
            reading_remote_snapshot_url=dragon_app.READING_REMOTE_SNAPSHOT_URL,
            reading_remote_snapshot_pull_enabled=dragon_app.reading_remote_snapshot_pull_enabled(),
            reading_backups_dir=dragon_app.READING_BACKUPS_DIR,
            refresh_service=build_refresh_service(format_timestamp_label=dragon_app.format_timestamp_label),
            datetime_module=dragon_app.datetime,
            monotonic=dragon_app.time.monotonic,
        )

        self.assertIsInstance(service, ReadingRuntimeService)

    def test_build_youtube_freshness_service_from_wiring_module(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            service = build_youtube_freshness_service(
                load_admin_data=dragon_app.load_admin_data,
                pockettube_latest_import_snapshot=dragon_app._pockettube_latest_import_snapshot,
                get_persisted_youtube_channel_latest_entry=dragon_app.get_persisted_youtube_channel_latest_entry,
                refresh_pockettube_section_latest_uploads=dragon_app.refresh_pockettube_section_latest_uploads,
                trigger_github_actions_sync=lambda scope="": ({"ok": True, "scope": scope}, 200),
                refresh_snapshot_from_github=lambda: {"ok": True},
                build_youtube_channel_video_summary=dragon_app.build_youtube_channel_video_summary,
                canonical_section_name=dragon_app.canonical_section_name,
                normalize_pockettube_group_key=dragon_app.normalize_pockettube_group_key,
                format_timestamp_label=dragon_app.format_timestamp_label,
                current_timestamp=dragon_app.current_timestamp,
                load_json_file=dragon_app.load_json_file,
                save_json_file=dragon_app.save_json_file,
                snapshot_path=Path(temp_dir) / "youtube_latest_snapshot.json",
                sync_status_path=Path(temp_dir) / "youtube_latest_sync_status.json",
                snapshot_raw_url=dragon_app.YOUTUBE_SYNC_GITHUB_RAW_SNAPSHOT_URL,
                sync_status_raw_url=dragon_app.YOUTUBE_SYNC_GITHUB_RAW_SYNC_STATUS_URL,
                requests_module=dragon_app.requests,
                app_logger=dragon_app.app.logger,
            )

        self.assertIsInstance(service, YouTubeFreshnessService)

    def test_reading_runtime_service_preserves_singleton_behavior(self):
        with patch.object(dragon_app, "_READING_RUNTIME_SERVICE", None), patch.object(
            dragon_app, "_READING_RUNTIME_PROJECTION_SERVICE", None
        ):
            first = dragon_app._get_reading_runtime_service()
            second = dragon_app._get_reading_runtime_service()

        self.assertIs(first, second)


if __name__ == "__main__":
    unittest.main()
