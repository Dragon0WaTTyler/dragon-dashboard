import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, patch

import app as dragon_app
from dragon.cache import load_json_file, save_json_file
from domains.youtube.services.youtube_freshness_service import YouTubeFreshnessService


FIXED_NOW = datetime(2026, 6, 2, 12, 0, 0, tzinfo=timezone.utc)


class _FixedDateTimeModule:
    @staticmethod
    def now(tz=None):
        return FIXED_NOW.astimezone(tz or timezone.utc)


class YouTubeFreshnessServiceTests(unittest.TestCase):
    def _timestamp(self, hours_ago):
        return (FIXED_NOW - timedelta(hours=hours_ago)).isoformat()

    def _build_service(self, temp_dir, *, imported_sections=None, latest_cache=None, refresh_mock=None):
        imported_sections = list(imported_sections or [])
        latest_cache = dict(latest_cache or {})
        state = {
            "latest_import": {
                "source_name": "PocketTube",
                "imported_at": FIXED_NOW.isoformat(),
                "sections": imported_sections,
            },
            "latest_cache": latest_cache,
        }

        def pockettube_latest_import_snapshot(admin_data=None):
            return state["latest_import"], state["latest_import"]["sections"]

        def get_persisted(channel_id, allow_stale=False):
            return state["latest_cache"].get(str(channel_id or "").strip(), {}), False

        def build_summary(video):
            video = dict(video or {})
            return {
                "title": video.get("title", ""),
                "video_id": video.get("video_id", ""),
                "channel_id": video.get("channel_id", ""),
                "channel_name": video.get("channel_name", ""),
                "thumbnail": video.get("thumbnail", video.get("thumb", "")),
                "thumbnail_url": video.get("thumbnail_url", video.get("thumb", "")),
                "thumb": video.get("thumb", ""),
                "published_at": video.get("published_at", ""),
                "url": video.get("url", ""),
            }

        def format_timestamp_label(value, default=""):
            raw = str(value or "").strip()
            if not raw:
                return default
            return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M")

        return YouTubeFreshnessService(
            load_admin_data=lambda: {"youtube_pockettube_imports": state["latest_import"], "youtube_channel_curation": {"channels": []}},
            pockettube_latest_import_snapshot=pockettube_latest_import_snapshot,
            get_persisted_youtube_channel_latest_entry=get_persisted,
            refresh_pockettube_section_latest_uploads=refresh_mock or Mock(),
            build_youtube_channel_video_summary=build_summary,
            canonical_section_name=lambda value: str(value or "").strip(),
            normalize_pockettube_group_key=lambda value: "".join(ch.lower() for ch in str(value or "") if ch.isalnum()),
            format_timestamp_label=format_timestamp_label,
            current_timestamp=lambda: FIXED_NOW.isoformat(),
            load_json_file=load_json_file,
            save_json_file=save_json_file,
            snapshot_path=Path(temp_dir) / "youtube_latest_snapshot.json",
            app_logger=Mock(),
        ), state

    def test_missing_snapshot_returns_safe_empty_state(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            service, _state = self._build_service(temp_dir)

            context = service.build_page_context()

            self.assertTrue(context["empty_state"])
            self.assertEqual(context["groups"], [])
            self.assertEqual(context["synced_at"], "")

    def test_snapshot_read_is_local_only(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            service, _state = self._build_service(temp_dir)
            payload = {
                "version": 1,
                "generated_at": FIXED_NOW.isoformat(),
                "synced_at": FIXED_NOW.isoformat(),
                "groups": {
                    "philosophy": {
                        "group_name": "Philosophy",
                        "group_key": "philosophy",
                        "section_name": "Philosophy",
                        "section_key": "philosophy",
                        "source_name": "PocketTube",
                        "imported_at": FIXED_NOW.isoformat(),
                        "channel_count": 1,
                        "latest_video_count": 1,
                        "latest_video": {
                            "video_id": "v1",
                            "title": "Cached Video",
                            "channel_id": "c1",
                            "channel_name": "Channel One",
                            "published_at": FIXED_NOW.isoformat(),
                            "url": "https://www.youtube.com/watch?v=v1",
                        },
                        "channels": [],
                    }
                },
                "channels": {},
                "errors": [],
            }
            save_json_file(service.snapshot_path, payload)

            context = service.build_page_context()

            self.assertFalse(context["empty_state"])
            self.assertEqual(context["group_count"], 1)
            self.assertEqual(context["groups"][0]["group_name"], "Philosophy")

    def test_latest_videos_group_by_channel_and_group(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            refresh_mock = Mock()
            imported_sections = [
                {
                    "section_name": "Science",
                    "section_key": "science",
                    "group_name": "Science",
                    "group_key": "science",
                    "channels": [
                        {"channel_id": "c1", "channel_name": "Channel One"},
                        {"channel_id": "c2", "channel_name": "Channel Two"},
                    ],
                },
                {
                    "section_name": "Philosophy",
                    "section_key": "philosophy",
                    "group_name": "Philosophy",
                    "group_key": "philosophy",
                    "channels": [
                        {"channel_id": "c2", "channel_name": "Channel Two"},
                        {"channel_id": "c3", "channel_name": "Channel Three"},
                    ],
                },
            ]
            latest_cache = {
                "c1": {
                    "latest_video": {
                        "video_id": "v1",
                        "title": "Science One",
                        "channel_id": "c1",
                        "channel_name": "Channel One",
                        "published_at": FIXED_NOW.isoformat(),
                        "url": "https://www.youtube.com/watch?v=v1",
                        "thumb": "https://img.youtube.com/vi/v1/hqdefault.jpg",
                    }
                },
                "c2": {
                    "latest_video": {
                        "video_id": "v2",
                        "title": "Shared Channel",
                        "channel_id": "c2",
                        "channel_name": "Channel Two",
                        "published_at": (FIXED_NOW + timedelta(hours=1)).isoformat(),
                        "url": "https://www.youtube.com/watch?v=v2",
                        "thumb": "https://img.youtube.com/vi/v2/hqdefault.jpg",
                    }
                },
                "c3": {
                    "latest_video": {
                        "video_id": "v3",
                        "title": "Philosophy Three",
                        "channel_id": "c3",
                        "channel_name": "Channel Three",
                        "published_at": FIXED_NOW.isoformat(),
                        "url": "https://www.youtube.com/watch?v=v3",
                        "thumb": "https://img.youtube.com/vi/v3/hqdefault.jpg",
                    }
                },
            }
            service, state = self._build_service(
                temp_dir,
                imported_sections=imported_sections,
                latest_cache=latest_cache,
                refresh_mock=refresh_mock,
            )

            snapshot = service.sync_snapshot()

            self.assertEqual(refresh_mock.call_count, 2)
            self.assertEqual(list(snapshot["groups"].keys()), ["philosophy", "science"])
            self.assertEqual(snapshot["groups"]["philosophy"]["channel_count"], 2)
            self.assertEqual(snapshot["channels"]["c2"]["group_names"], ["Philosophy", "Science"])
            self.assertEqual(snapshot["groups"]["science"]["latest_video"]["video_id"], "v2")

    def test_sync_output_is_deterministic_for_mocked_input(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            imported_sections = [
                {
                    "section_name": "Science",
                    "section_key": "science",
                    "group_name": "Science",
                    "group_key": "science",
                    "channels": [
                        {"channel_id": "c1", "channel_name": "Channel One"},
                        {"channel_id": "c2", "channel_name": "Channel Two"},
                    ],
                }
            ]
            latest_cache = {
                "c1": {
                    "latest_video": {
                        "video_id": "v1",
                        "title": "Science One",
                        "channel_id": "c1",
                        "channel_name": "Channel One",
                        "published_at": FIXED_NOW.isoformat(),
                        "url": "https://www.youtube.com/watch?v=v1",
                        "thumb": "https://img.youtube.com/vi/v1/hqdefault.jpg",
                    }
                },
                "c2": {
                    "latest_video": {
                        "video_id": "v2",
                        "title": "Science Two",
                        "channel_id": "c2",
                        "channel_name": "Channel Two",
                        "published_at": FIXED_NOW.isoformat(),
                        "url": "https://www.youtube.com/watch?v=v2",
                        "thumb": "https://img.youtube.com/vi/v2/hqdefault.jpg",
                    }
                },
            }
            service, _state = self._build_service(
                temp_dir,
                imported_sections=imported_sections,
                latest_cache=latest_cache,
                refresh_mock=Mock(),
            )

            first = service.sync_snapshot()
            second = service.sync_snapshot()

            self.assertEqual(first["groups"], second["groups"])
            self.assertEqual(first["channels"], second["channels"])

    def test_get_page_builder_does_not_invoke_remote_fetch_method(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            refresh_mock = Mock()
            service, _state = self._build_service(temp_dir, refresh_mock=refresh_mock)
            save_json_file(
                service.snapshot_path,
                {
                    "version": 1,
                    "generated_at": FIXED_NOW.isoformat(),
                    "synced_at": FIXED_NOW.isoformat(),
                    "groups": {},
                    "channels": {},
                    "errors": [],
                },
            )

            context = service.build_page_context()

            self.assertTrue(context["empty_state"])
            refresh_mock.assert_not_called()

    def test_get_route_uses_snapshot_only(self):
        dragon_app.app.config["TESTING"] = True
        client = dragon_app.app.test_client()
        mock_service = Mock()
        mock_service.build_page_context.return_value = {
            "title": "PocketTube Freshness",
            "snapshot": {"version": 1, "groups": {}, "channels": {}, "errors": []},
            "groups": [],
            "group_count": 0,
            "channel_count": 0,
            "has_latest": False,
            "generated_at": "",
            "synced_at": "",
            "errors": [],
            "empty_state": True,
            "empty_reason": "no_snapshot",
            "sync_notice": "",
        }

        with patch.object(dragon_app, "YOUTUBE_FRESHNESS_SERVICE", mock_service):
            response = client.get("/pockettube/freshness")

        self.assertEqual(response.status_code, 200)
        mock_service.build_page_context.assert_called_once()
        mock_service.sync_snapshot.assert_not_called()

    def test_post_sync_redirects_back_to_freshness_page(self):
        dragon_app.app.config["TESTING"] = True
        client = dragon_app.app.test_client()
        mock_service = Mock()
        mock_service.sync_snapshot.return_value = {"version": 1, "groups": {}, "channels": {}, "errors": []}

        with patch.object(dragon_app, "YOUTUBE_FRESHNESS_SERVICE", mock_service):
            response = client.post("/pockettube/freshness/sync", data={"scope": ""})

        self.assertEqual(response.status_code, 302)
        self.assertIn("/pockettube/freshness", response.location)
        mock_service.sync_snapshot.assert_called_once()


if __name__ == "__main__":
    unittest.main()
