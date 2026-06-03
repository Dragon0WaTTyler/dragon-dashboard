import json
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


class _FakeResponse:
    def __init__(self, *, status_code=200, text="{}"):
        self.status_code = status_code
        self.text = text


class _FakeRequestsModule:
    class RequestException(Exception):
        pass

    def __init__(self, responses=None):
        self._responses = dict(responses or {})

    def get(self, url, timeout=15):
        response = self._responses.get(url)
        if isinstance(response, Exception):
            raise response
        if response is None:
            raise self.RequestException(f"unexpected url: {url}")
        return response


class YouTubeFreshnessServiceTests(unittest.TestCase):
    def _timestamp(self, hours_ago):
        return (FIXED_NOW - timedelta(hours=hours_ago)).isoformat()

    def _group_video(self, video_id, *, channel_id="c1", channel_name="Channel One", hours_ago=1, title=None):
        return {
            "video_id": video_id,
            "title": title or f"Video {video_id}",
            "channel_id": channel_id,
            "channel_name": channel_name,
            "published_at": self._timestamp(hours_ago),
            "url": f"https://www.youtube.com/watch?v={video_id}",
            "thumb": f"https://img.youtube.com/vi/{video_id}/hqdefault.jpg",
        }

    def _build_service(
        self,
        temp_dir,
        *,
        imported_sections=None,
        latest_cache=None,
        registry_payload=None,
        refresh_mock=None,
        trigger_mock=None,
        github_refresh_mock=None,
        requests_module=None,
    ):
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

        registry_path = Path(temp_dir) / "pockettube_registry.json"
        if registry_payload is not None:
            save_json_file(registry_path, registry_payload)

        return YouTubeFreshnessService(
            load_admin_data=lambda: {
                "youtube_pockettube_imports": state["latest_import"],
                "youtube_channel_curation": {"channels": []},
            },
            pockettube_latest_import_snapshot=pockettube_latest_import_snapshot,
            get_persisted_youtube_channel_latest_entry=get_persisted,
            refresh_pockettube_section_latest_uploads=refresh_mock or Mock(),
            trigger_github_actions_sync=trigger_mock or Mock(return_value=({"status": "started", "ok": True}, 200)),
            refresh_snapshot_from_github=github_refresh_mock or Mock(return_value={"version": 1, "groups": {}, "channels": {}, "errors": []}),
            build_youtube_channel_video_summary=build_summary,
            canonical_section_name=lambda value: str(value or "").strip(),
            normalize_pockettube_group_key=lambda value: "".join(ch.lower() for ch in str(value or "") if ch.isalnum()),
            format_timestamp_label=format_timestamp_label,
            current_timestamp=lambda: FIXED_NOW.isoformat(),
            load_json_file=load_json_file,
            save_json_file=save_json_file,
            snapshot_path=Path(temp_dir) / "youtube_latest_snapshot.json",
            sync_status_path=Path(temp_dir) / "youtube_latest_sync_status.json",
            snapshot_raw_url="https://example.com/youtube_latest_snapshot.json",
            sync_status_raw_url="https://example.com/youtube_latest_sync_status.json",
            requests_module=requests_module,
            registry_path=registry_path,
            app_logger=Mock(),
        ), state

    def _build_related_snapshot_payload(self):
        return {
            "version": 1,
            "generated_at": FIXED_NOW.isoformat(),
            "synced_at": FIXED_NOW.isoformat(),
            "groups": {
                "news": {
                    "group_name": "News",
                    "group_key": "news",
                    "section_name": "News",
                    "section_key": "news",
                    "source_name": "PocketTube",
                    "imported_at": FIXED_NOW.isoformat(),
                    "channel_count": 4,
                    "latest_video_count": 4,
                    "latest_video": {},
                    "channels": [
                        {
                            "channel_id": "c-current",
                            "channel_title": "News Channel",
                            "group_names": ["News"],
                            "group_key": "news",
                            "latest_video": {
                                "entry_id": "yt-current",
                                "video_id": "current",
                                "watch_key": "current",
                                "title": "Current News Story",
                                "channel_id": "c-current",
                                "channel_name": "News Channel",
                                "published_at": self._timestamp(1),
                                "published_display": "2026-06-02 11:00",
                                "url": "https://www.youtube.com/watch?v=current",
                                "detail_url": "/video/yt-current",
                                "thumbnail": "https://img.youtube.com/vi/current/hqdefault.jpg",
                                "thumbnail_url": "https://img.youtube.com/vi/current/hqdefault.jpg",
                                "image_url": "https://img.youtube.com/vi/current/hqdefault.jpg",
                                "source_type": "youtube",
                            },
                            "latest_video_id": "current",
                            "published_at": self._timestamp(1),
                            "published_display": "2026-06-02 11:00",
                            "thumbnail": "https://img.youtube.com/vi/current/hqdefault.jpg",
                            "url": "https://www.youtube.com/watch?v=current",
                            "reason_tags": ["latest-cached"],
                        },
                        {
                            "channel_id": "c-news-2",
                            "channel_title": "News Desk",
                            "group_names": ["News"],
                            "group_key": "news",
                            "latest_video": {
                                "entry_id": "yt-news-2",
                                "video_id": "news-2",
                                "watch_key": "news-2",
                                "title": "Breaking News Update",
                                "channel_id": "c-news-2",
                                "channel_name": "News Desk",
                                "published_at": self._timestamp(2),
                                "published_display": "2026-06-02 10:00",
                                "url": "https://www.youtube.com/watch?v=news-2",
                                "detail_url": "/video/yt-news-2",
                                "thumbnail": "https://img.youtube.com/vi/news-2/hqdefault.jpg",
                                "thumbnail_url": "https://img.youtube.com/vi/news-2/hqdefault.jpg",
                                "image_url": "https://img.youtube.com/vi/news-2/hqdefault.jpg",
                                "source_type": "youtube",
                            },
                            "latest_video_id": "news-2",
                            "published_at": self._timestamp(2),
                            "published_display": "2026-06-02 10:00",
                            "thumbnail": "https://img.youtube.com/vi/news-2/hqdefault.jpg",
                            "url": "https://www.youtube.com/watch?v=news-2",
                            "reason_tags": ["latest-cached"],
                        },
                        {
                            "channel_id": "c-dup",
                            "channel_title": "News Mirror",
                            "group_names": ["News"],
                            "group_key": "news",
                            "latest_video": {
                                "entry_id": "yt-dup-1",
                                "video_id": "dup-1",
                                "watch_key": "dup-1",
                                "title": "Duplicated Snapshot Story",
                                "channel_id": "c-dup",
                                "channel_name": "News Mirror",
                                "published_at": self._timestamp(3),
                                "published_display": "2026-06-02 09:00",
                                "url": "https://www.youtube.com/watch?v=dup-1",
                                "detail_url": "/video/yt-dup-1",
                                "thumbnail": "https://img.youtube.com/vi/dup-1/hqdefault.jpg",
                                "thumbnail_url": "https://img.youtube.com/vi/dup-1/hqdefault.jpg",
                                "image_url": "https://img.youtube.com/vi/dup-1/hqdefault.jpg",
                                "source_type": "youtube",
                            },
                            "latest_video_id": "dup-1",
                            "published_at": self._timestamp(3),
                            "published_display": "2026-06-02 09:00",
                            "thumbnail": "https://img.youtube.com/vi/dup-1/hqdefault.jpg",
                            "url": "https://www.youtube.com/watch?v=dup-1",
                            "reason_tags": ["latest-cached"],
                        },
                    ],
                },
                "archive": {
                    "group_name": "Archive",
                    "group_key": "archive",
                    "section_name": "Archive",
                    "section_key": "archive",
                    "source_name": "PocketTube",
                    "imported_at": FIXED_NOW.isoformat(),
                    "channel_count": 3,
                    "latest_video_count": 3,
                    "latest_video": {},
                    "channels": [
                        {
                            "channel_id": "c-current",
                            "channel_title": "News Channel",
                            "group_names": ["Archive", "News"],
                            "group_key": "archive",
                            "latest_video": {
                                "entry_id": "yt-current-archive",
                                "video_id": "current-archive",
                                "watch_key": "current-archive",
                                "title": "Current Channel Archive Cut",
                                "channel_id": "c-current",
                                "channel_name": "News Channel",
                                "published_at": self._timestamp(4),
                                "published_display": "2026-06-02 08:00",
                                "url": "https://www.youtube.com/watch?v=current-archive",
                                "detail_url": "/video/yt-current-archive",
                                "thumbnail": "https://img.youtube.com/vi/current-archive/hqdefault.jpg",
                                "thumbnail_url": "https://img.youtube.com/vi/current-archive/hqdefault.jpg",
                                "image_url": "https://img.youtube.com/vi/current-archive/hqdefault.jpg",
                                "source_type": "youtube",
                            },
                            "latest_video_id": "current-archive",
                            "published_at": self._timestamp(4),
                            "published_display": "2026-06-02 08:00",
                            "thumbnail": "https://img.youtube.com/vi/current-archive/hqdefault.jpg",
                            "url": "https://www.youtube.com/watch?v=current-archive",
                            "reason_tags": ["latest-cached"],
                        },
                        {
                            "channel_id": "c-shared",
                            "channel_title": "World News",
                            "group_names": ["Archive", "News", "World"],
                            "group_key": "archive",
                            "latest_video": {
                                "entry_id": "yt-shared-1",
                                "video_id": "shared-1",
                                "watch_key": "shared-1",
                                "title": "World News Bulletin",
                                "channel_id": "c-shared",
                                "channel_name": "World News",
                                "published_at": self._timestamp(5),
                                "published_display": "2026-06-02 07:00",
                                "url": "https://www.youtube.com/watch?v=shared-1",
                                "detail_url": "/video/yt-shared-1",
                                "thumbnail": "https://img.youtube.com/vi/shared-1/hqdefault.jpg",
                                "thumbnail_url": "https://img.youtube.com/vi/shared-1/hqdefault.jpg",
                                "image_url": "https://img.youtube.com/vi/shared-1/hqdefault.jpg",
                                "source_type": "youtube",
                            },
                            "latest_video_id": "shared-1",
                            "published_at": self._timestamp(5),
                            "published_display": "2026-06-02 07:00",
                            "thumbnail": "https://img.youtube.com/vi/shared-1/hqdefault.jpg",
                            "url": "https://www.youtube.com/watch?v=shared-1",
                            "reason_tags": ["latest-cached"],
                        },
                        {
                            "channel_id": "c-dup",
                            "channel_title": "News Mirror",
                            "group_names": ["Archive"],
                            "group_key": "archive",
                            "latest_video": {
                                "entry_id": "yt-dup-1",
                                "video_id": "dup-1",
                                "watch_key": "dup-1",
                                "title": "Duplicated Snapshot Story",
                                "channel_id": "c-dup",
                                "channel_name": "News Mirror",
                                "published_at": self._timestamp(3),
                                "published_display": "2026-06-02 09:00",
                                "url": "https://www.youtube.com/watch?v=dup-1",
                                "detail_url": "/video/yt-dup-1",
                                "thumbnail": "https://img.youtube.com/vi/dup-1/hqdefault.jpg",
                                "thumbnail_url": "https://img.youtube.com/vi/dup-1/hqdefault.jpg",
                                "image_url": "https://img.youtube.com/vi/dup-1/hqdefault.jpg",
                                "source_type": "youtube",
                            },
                            "latest_video_id": "dup-1",
                            "published_at": self._timestamp(3),
                            "published_display": "2026-06-02 09:00",
                            "thumbnail": "https://img.youtube.com/vi/dup-1/hqdefault.jpg",
                            "url": "https://www.youtube.com/watch?v=dup-1",
                            "reason_tags": ["latest-cached"],
                        },
                    ],
                },
            },
            "channels": {},
            "errors": [],
        }

    def test_missing_snapshot_returns_safe_empty_state(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            service, _state = self._build_service(temp_dir)

            context = service.build_page_context()

            self.assertTrue(context["empty_state"])
            self.assertEqual(context["groups"], [])
            self.assertEqual(context["synced_at"], "")
            self.assertEqual(context["sync_status"]["status"], "idle")

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

    def test_build_page_context_for_filter_uses_snapshot_groups_only(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            service, _state = self._build_service(temp_dir)
            payload = {
                "version": 1,
                "generated_at": FIXED_NOW.isoformat(),
                "synced_at": FIXED_NOW.isoformat(),
                "groups": {
                    "news": {
                        "group_name": "News",
                        "group_key": "news",
                        "section_name": "News",
                        "section_key": "news",
                        "source_name": "PocketTube",
                        "imported_at": FIXED_NOW.isoformat(),
                        "channel_count": 1,
                        "latest_video_count": 1,
                        "latest_video": {},
                        "channels": [
                            {
                                "channel_id": "c-news",
                                "channel_title": "News One",
                                "group_names": ["News"],
                                "group_keys": ["news"],
                                "latest_video": {
                                    "entry_id": "yt-news-1",
                                    "video_id": "news-1",
                                    "watch_key": "news-1",
                                    "title": "Cached News Video",
                                    "channel_id": "c-news",
                                    "channel_name": "News One",
                                    "published_at": FIXED_NOW.isoformat(),
                                    "published_display": "2026-06-02 12:00",
                                    "url": "https://www.youtube.com/watch?v=news-1",
                                    "detail_url": "/video/yt-news-1",
                                    "thumbnail": "https://img.youtube.com/vi/news-1/hqdefault.jpg",
                                    "thumbnail_url": "https://img.youtube.com/vi/news-1/hqdefault.jpg",
                                },
                                "latest_video_id": "news-1",
                                "published_at": FIXED_NOW.isoformat(),
                                "published_display": "2026-06-02 12:00",
                                "thumbnail": "https://img.youtube.com/vi/news-1/hqdefault.jpg",
                                "url": "https://www.youtube.com/watch?v=news-1",
                            }
                        ],
                    },
                    "tech": {
                        "group_name": "Tech",
                        "group_key": "tech",
                        "section_name": "Tech",
                        "section_key": "tech",
                        "source_name": "PocketTube",
                        "imported_at": FIXED_NOW.isoformat(),
                        "channel_count": 1,
                        "latest_video_count": 1,
                        "latest_video": {},
                        "channels": [
                            {
                                "channel_id": "c-tech",
                                "channel_title": "Tech One",
                                "group_names": ["Tech"],
                                "group_keys": ["tech"],
                                "latest_video": {
                                    "entry_id": "yt-tech-1",
                                    "video_id": "tech-1",
                                    "watch_key": "tech-1",
                                    "title": "Cached Tech Video",
                                    "channel_id": "c-tech",
                                    "channel_name": "Tech One",
                                    "published_at": FIXED_NOW.isoformat(),
                                    "published_display": "2026-06-02 12:00",
                                    "url": "https://www.youtube.com/watch?v=tech-1",
                                    "detail_url": "/video/yt-tech-1",
                                    "thumbnail": "https://img.youtube.com/vi/tech-1/hqdefault.jpg",
                                    "thumbnail_url": "https://img.youtube.com/vi/tech-1/hqdefault.jpg",
                                },
                                "latest_video_id": "tech-1",
                                "published_at": FIXED_NOW.isoformat(),
                                "published_display": "2026-06-02 12:00",
                                "thumbnail": "https://img.youtube.com/vi/tech-1/hqdefault.jpg",
                                "url": "https://www.youtube.com/watch?v=tech-1",
                            }
                        ],
                    },
                },
                "channels": {},
                "errors": [],
            }
            save_json_file(service.snapshot_path, payload)

            context = service.build_page_context_for_filter("news")

            self.assertEqual(context["selected_filter_key"], "news")
            self.assertEqual(context["selected_filter_label"], "News")
            self.assertEqual(len(context["feed_videos"]), 1)
            self.assertEqual(context["feed_videos"][0]["video_id"], "news-1")
            self.assertTrue(any(item["key"] == "favorites" for item in context["feed_filters"]))
            self.assertTrue(any(item["key"] == "cinema" for item in context["feed_filters"]))

    def test_build_page_context_marks_old_snapshot_as_stale(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            service, _state = self._build_service(temp_dir)
            old_timestamp = (FIXED_NOW - timedelta(hours=30)).isoformat()
            payload = {
                "version": 1,
                "generated_at": old_timestamp,
                "synced_at": old_timestamp,
                "groups": {
                    "news": {
                        "group_name": "News",
                        "group_key": "news",
                        "section_name": "News",
                        "section_key": "news",
                        "source_name": "PocketTube",
                        "imported_at": old_timestamp,
                        "channel_count": 1,
                        "latest_video_count": 1,
                        "latest_video": {},
                        "channels": [
                            {
                                "channel_id": "c-news",
                                "channel_title": "News One",
                                "group_names": ["News"],
                                "group_keys": ["news"],
                                "latest_video": {
                                    "entry_id": "yt-news-1",
                                    "video_id": "news-1",
                                    "watch_key": "news-1",
                                    "title": "Cached News Video",
                                    "channel_id": "c-news",
                                    "channel_name": "News One",
                                    "published_at": old_timestamp,
                                    "published_display": "2026-06-01 06:00",
                                    "url": "https://www.youtube.com/watch?v=news-1",
                                    "detail_url": "/video/yt-news-1",
                                    "thumbnail": "https://img.youtube.com/vi/news-1/hqdefault.jpg",
                                    "thumbnail_url": "https://img.youtube.com/vi/news-1/hqdefault.jpg",
                                },
                                "latest_video_id": "news-1",
                                "published_at": old_timestamp,
                                "published_display": "2026-06-01 06:00",
                                "thumbnail": "https://img.youtube.com/vi/news-1/hqdefault.jpg",
                                "url": "https://www.youtube.com/watch?v=news-1",
                            }
                        ],
                    }
                },
                "channels": {},
                "errors": [],
            }
            save_json_file(service.snapshot_path, payload)

            context = service.build_page_context_for_filter("all")

            self.assertEqual(context["snapshot_status"]["state"], "stale")
            self.assertTrue(context["snapshot_status"]["is_stale"])
            self.assertIn("cached results", context["snapshot_status"]["message"])

    def test_snapshot_video_detail_context_matches_entry_id_video_id_and_watch_key(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            service, _state = self._build_service(temp_dir)
            payload = {
                "version": 1,
                "generated_at": FIXED_NOW.isoformat(),
                "synced_at": FIXED_NOW.isoformat(),
                "groups": {
                    "science": {
                        "group_name": "Science",
                        "group_key": "science",
                        "section_name": "Science",
                        "section_key": "science",
                        "source_name": "PocketTube",
                        "imported_at": FIXED_NOW.isoformat(),
                        "channel_count": 1,
                        "latest_video_count": 1,
                        "latest_video": {},
                        "channels": [
                            {
                                "channel_id": "UC-1",
                                "channel_title": "Science Channel",
                                "group_names": ["Science"],
                                "group_key": "science",
                                "latest_video": {
                                    "entry_id": "yt-NvouldZEM",
                                    "video_id": "NvouldZEM",
                                    "watch_key": "NvouldZEM",
                                    "title": "PocketTube Snapshot Title",
                                    "channel_name": "Science Channel",
                                    "channel_id": "UC-1",
                                    "published_at": FIXED_NOW.isoformat(),
                                    "published_display": "2026-06-02 12:00",
                                    "url": "https://www.youtube.com/watch?v=NvouldZEM",
                                    "detail_url": "/video/yt-NvouldZEM",
                                    "thumbnail": "https://img.youtube.com/vi/NvouldZEM/hqdefault.jpg",
                                    "thumbnail_url": "https://img.youtube.com/vi/NvouldZEM/hqdefault.jpg",
                                    "image_url": "https://img.youtube.com/vi/NvouldZEM/hqdefault.jpg",
                                    "source_type": "youtube",
                                },
                                "latest_video_id": "NvouldZEM",
                                "published_at": FIXED_NOW.isoformat(),
                                "published_display": "2026-06-02 12:00",
                                "thumbnail": "https://img.youtube.com/vi/NvouldZEM/hqdefault.jpg",
                                "url": "https://www.youtube.com/watch?v=NvouldZEM",
                                "reason_tags": ["latest-cached"],
                            }
                        ],
                    }
                },
                "channels": {
                    "UC-1": {
                        "channel_id": "UC-1",
                        "channel_title": "Science Channel",
                        "latest_video": {
                            "entry_id": "yt-NvouldZEM",
                            "video_id": "NvouldZEM",
                            "watch_key": "NvouldZEM",
                            "title": "PocketTube Snapshot Title",
                            "channel_name": "Science Channel",
                            "channel_id": "UC-1",
                            "published_at": FIXED_NOW.isoformat(),
                            "published_display": "2026-06-02 12:00",
                            "url": "https://www.youtube.com/watch?v=NvouldZEM",
                            "detail_url": "/video/yt-NvouldZEM",
                            "thumbnail": "https://img.youtube.com/vi/NvouldZEM/hqdefault.jpg",
                            "thumbnail_url": "https://img.youtube.com/vi/NvouldZEM/hqdefault.jpg",
                            "image_url": "https://img.youtube.com/vi/NvouldZEM/hqdefault.jpg",
                            "source_type": "youtube",
                        },
                    }
                },
                "errors": [],
            }
            save_json_file(service.snapshot_path, payload)

            context = service.find_snapshot_video_detail_context("yt-NvouldZEM")

            self.assertIsNotNone(context)
            self.assertEqual(context["entry_type"], "youtube")
            self.assertEqual(context["player_video_id"], "NvouldZEM")
            self.assertEqual(context["entry"]["entry_id"], "yt-NvouldZEM")
            self.assertEqual(context["entry"]["video_id"], "NvouldZEM")
            self.assertEqual(context["entry"]["watch_key"], "NvouldZEM")
            self.assertEqual(context["entry"]["title"], "PocketTube Snapshot Title")
            self.assertEqual(context["entry"]["detail_url"], "/video/yt-NvouldZEM")
            self.assertEqual(context["entry"]["playlist_url"], "/pockettube")
            self.assertEqual(context["entry"]["source_type"], "youtube")
            self.assertEqual(context["related_entries"], [])

    def test_snapshot_related_videos_prioritize_same_group_and_exclude_current(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            service, _state = self._build_service(temp_dir)
            payload = self._build_related_snapshot_payload()
            save_json_file(service.snapshot_path, payload)

            context = service.find_snapshot_video_detail_context("yt-current")
            related = context["related_entries"]
            related_ids = [item["video_id"] for item in related]

            self.assertGreaterEqual(len(related), 3)
            self.assertEqual(related[0]["video_id"], "news-2")
            self.assertNotIn("current", related_ids)
            self.assertEqual(len(related_ids), len(set(related_ids)))
            self.assertIn("current-archive", related_ids)
            self.assertIn("shared-1", related_ids)

    def test_snapshot_related_videos_include_same_channel_and_dedupe(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            service, _state = self._build_service(temp_dir)
            payload = self._build_related_snapshot_payload()
            save_json_file(service.snapshot_path, payload)

            context = service.find_snapshot_video_detail_context("yt-current")
            related = context["related_entries"]
            related_ids = [item["video_id"] for item in related]

            self.assertIn("current-archive", related_ids)
            self.assertEqual(related_ids.count("dup-1"), 1)
            self.assertTrue(all(item["detail_url"].startswith("/video/yt-") for item in related))

    def test_build_page_context_creates_unified_video_feed(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            service, _state = self._build_service(temp_dir)
            payload = {
                "version": 1,
                "generated_at": FIXED_NOW.isoformat(),
                "synced_at": FIXED_NOW.isoformat(),
                "groups": {
                    "science": {
                        "group_name": "Science",
                        "group_key": "science",
                        "section_name": "Science",
                        "section_key": "science",
                        "source_name": "PocketTube",
                        "imported_at": FIXED_NOW.isoformat(),
                        "channel_count": 2,
                        "latest_video_count": 1,
                        "latest_video": {
                            "video_id": "v1",
                            "title": "Science One",
                            "channel_id": "c1",
                            "channel_name": "Channel One",
                            "published_at": FIXED_NOW.isoformat(),
                            "url": "https://www.youtube.com/watch?v=v1",
                            "thumb": "https://img.youtube.com/vi/v1/hqdefault.jpg",
                        },
                        "channels": [
                            {
                                "channel_id": "c1",
                                "channel_title": "Channel One",
                                "group_names": ["Science"],
                                "group_key": "science",
                                "latest_video": {
                                    "video_id": "v1",
                                    "title": "Science One",
                                    "channel_id": "c1",
                                    "channel_name": "Channel One",
                                    "published_at": FIXED_NOW.isoformat(),
                                    "url": "https://www.youtube.com/watch?v=v1",
                                    "thumb": "https://img.youtube.com/vi/v1/hqdefault.jpg",
                                },
                                "latest_video_id": "v1",
                                "published_at": FIXED_NOW.isoformat(),
                                "published_display": "2026-06-02 12:00",
                                "thumbnail": "https://img.youtube.com/vi/v1/hqdefault.jpg",
                                "url": "https://www.youtube.com/watch?v=v1",
                                "reason_tags": ["latest-cached"],
                            },
                            {
                                "channel_id": "c2",
                                "channel_title": "Channel Two",
                                "group_names": ["Science"],
                                "group_key": "science",
                                "latest_video": {},
                                "latest_video_id": "",
                                "published_at": "",
                                "published_display": "",
                                "thumbnail": "",
                                "url": "",
                                "reason_tags": ["source-diverse"],
                            },
                        ],
                    },
                    "knowledge": {
                        "group_name": "Knowledge",
                        "group_key": "knowledge",
                        "section_name": "Knowledge",
                        "section_key": "knowledge",
                        "source_name": "PocketTube",
                        "imported_at": FIXED_NOW.isoformat(),
                        "channel_count": 1,
                        "latest_video_count": 1,
                        "latest_video": {
                            "video_id": "v1",
                            "title": "Science One",
                            "channel_id": "c1",
                            "channel_name": "Channel One",
                            "published_at": FIXED_NOW.isoformat(),
                            "url": "https://www.youtube.com/watch?v=v1",
                            "thumb": "https://img.youtube.com/vi/v1/hqdefault.jpg",
                        },
                        "channels": [
                            {
                                "channel_id": "c1",
                                "channel_title": "Channel One",
                                "group_names": ["Knowledge"],
                                "group_key": "knowledge",
                                "latest_video": {
                                    "video_id": "v1",
                                    "title": "Science One",
                                    "channel_id": "c1",
                                    "channel_name": "Channel One",
                                    "published_at": FIXED_NOW.isoformat(),
                                    "url": "https://www.youtube.com/watch?v=v1",
                                    "thumb": "https://img.youtube.com/vi/v1/hqdefault.jpg",
                                },
                                "latest_video_id": "v1",
                                "published_at": FIXED_NOW.isoformat(),
                                "published_display": "2026-06-02 12:00",
                                "thumbnail": "https://img.youtube.com/vi/v1/hqdefault.jpg",
                                "url": "https://www.youtube.com/watch?v=v1",
                                "reason_tags": ["latest-cached"],
                            }
                        ],
                    },
                },
                "channels": {
                    "c1": {
                        "channel_id": "c1",
                        "channel_title": "Channel One",
                        "latest_video": {
                            "video_id": "v1",
                            "title": "Science One",
                            "channel_id": "c1",
                            "channel_name": "Channel One",
                            "published_at": FIXED_NOW.isoformat(),
                            "url": "https://www.youtube.com/watch?v=v1",
                            "thumb": "https://img.youtube.com/vi/v1/hqdefault.jpg",
                        },
                        "group_names": ["Knowledge", "Science"],
                        "latest_video_id": "v1",
                        "published_at": FIXED_NOW.isoformat(),
                        "published_display": "2026-06-02 12:00",
                        "thumbnail": "https://img.youtube.com/vi/v1/hqdefault.jpg",
                        "url": "https://www.youtube.com/watch?v=v1",
                        "reason_tags": ["latest-cached"],
                    }
                },
                "errors": [],
            }
            save_json_file(service.snapshot_path, payload)

            context = service.build_page_context()

            self.assertFalse(context["empty_state"])
            self.assertEqual(context["feed_video_count"], 1)
            self.assertEqual(context["feed_empty_channel_count"], 1)
            self.assertEqual(context["feed_groups"][0]["video_count"], 1)
            self.assertEqual(context["feed_videos"][0]["video_id"], "v1")
            self.assertEqual(context["feed_videos"][0]["group_names"], ["Knowledge", "Science"])
            self.assertEqual(context["feed_videos"][0]["channel_title"], "Channel One")
            self.assertEqual(context["feed_videos"][0]["detail_url"], "/video/yt-v1")

    def test_build_page_context_empty_snapshot_remains_safe_state(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            service, _state = self._build_service(temp_dir)
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
            self.assertEqual(context["feed_video_count"], 0)
            self.assertEqual(context["feed_videos"], [])

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
            service, _state = self._build_service(
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

    def test_sync_snapshot_attaches_latest_result_to_group_and_top_level_channels(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            imported_sections = [
                {
                    "section_name": "Science",
                    "section_key": "science",
                    "group_name": "Science",
                    "group_key": "science",
                    "channels": [
                        {"channel_id": "c1", "channel_name": "Channel One"},
                    ],
                }
            ]

            refresh_mock = Mock(
                return_value={
                    "group_name": "Science",
                    "section_name": "Science",
                    "latest_videos_found": 1,
                    "latest_items": [
                        {
                            "video_id": "v1",
                            "title": "Science One",
                            "channel_id": "c1",
                            "channel_name": "Channel One",
                            "published_at": FIXED_NOW.isoformat(),
                            "url": "https://www.youtube.com/watch?v=v1",
                            "thumb": "https://img.youtube.com/vi/v1/hqdefault.jpg",
                        }
                    ],
                }
            )
            service, _state = self._build_service(
                temp_dir,
                imported_sections=imported_sections,
                refresh_mock=refresh_mock,
            )

            snapshot = service.sync_snapshot()

            self.assertEqual(snapshot["groups"]["science"]["channels"][0]["latest_video"]["video_id"], "v1")
            self.assertEqual(snapshot["groups"]["science"]["channels"][0]["latest_video_id"], "v1")
            self.assertEqual(snapshot["groups"]["science"]["latest_video"]["video_id"], "v1")
            self.assertEqual(snapshot["channels"]["c1"]["latest_video"]["video_id"], "v1")
            self.assertEqual(snapshot["channels"]["c1"]["latest_video_id"], "v1")
            self.assertEqual(snapshot["groups"]["science"]["latest_video_count"], 1)

    def test_sync_snapshot_passes_normalized_section_channels_into_refresh(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            registry_payload = {
                "latest": {
                    "source_name": "PocketTube",
                    "imported_at": FIXED_NOW.isoformat(),
                    "fingerprint": "registry-refresh-input",
                    "section_count": 1,
                    "group_count": 1,
                    "channel_count": 1,
                    "sections": [
                        {
                            "section_name": "Science",
                            "section_key": "science",
                            "group_name": "Science",
                            "group_key": "science",
                            "tier": "best",
                            "channel_count": 1,
                            "channels": [
                                {
                                    "channel_name": "UCTDc1RLIHHNjN5WlHoZwXQg",
                                    "channel_id": "",
                                    "channel_key": "uctdc1rlihhnjn5wlhozwxqg",
                                    "section_name": "Science",
                                    "section_key": "science",
                                    "group_name": "Science",
                                    "group_key": "science",
                                    "tier": "best",
                                }
                            ],
                        }
                    ],
                    "channels": [],
                }
            }
            refresh_mock = Mock(return_value={"group_name": "Science", "section_name": "Science", "latest_items": [], "latest_videos_found": 0})
            service, _state = self._build_service(
                temp_dir,
                imported_sections=[],
                registry_payload=registry_payload,
                refresh_mock=refresh_mock,
            )

            snapshot = service.sync_snapshot()

            self.assertEqual(refresh_mock.call_count, 1)
            refresh_admin_data = refresh_mock.call_args.kwargs["admin_data"]
            refresh_latest = refresh_admin_data["youtube_pockettube_imports"]["latest"]
            self.assertEqual(len(refresh_latest["sections"]), 1)
            self.assertEqual(len(refresh_latest["sections"][0]["channels"]), 1)
            self.assertEqual(refresh_latest["sections"][0]["channels"][0]["channel_id"], "UCTDc1RLIHHNjN5WlHoZwXQg")
            self.assertEqual(snapshot["groups"]["science"]["channel_count"], 1)
            self.assertEqual(len(snapshot["channels"]), 1)

    def test_sync_snapshot_merges_same_latest_video_across_multiple_groups(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            imported_sections = [
                {
                    "section_name": "Science",
                    "section_key": "science",
                    "group_name": "Science",
                    "group_key": "science",
                    "channels": [
                        {"channel_id": "c1", "channel_name": "Channel One"},
                    ],
                },
                {
                    "section_name": "Knowledge",
                    "section_key": "knowledge",
                    "group_name": "Knowledge",
                    "group_key": "knowledge",
                    "channels": [
                        {"channel_id": "c1", "channel_name": "Channel One"},
                    ],
                },
            ]

            def refresh_side_effect(section_name, admin_data=None, max_channels=200):
                return {
                    "group_name": section_name,
                    "section_name": section_name,
                    "latest_videos_found": 1,
                    "latest_items": [
                        {
                            "video_id": "v1",
                            "title": "Shared Video",
                            "channel_id": "c1",
                            "channel_name": "Channel One",
                            "published_at": FIXED_NOW.isoformat(),
                            "url": "https://www.youtube.com/watch?v=v1",
                            "thumb": "https://img.youtube.com/vi/v1/hqdefault.jpg",
                        }
                    ],
                }

            service, _state = self._build_service(
                temp_dir,
                imported_sections=imported_sections,
                refresh_mock=Mock(side_effect=refresh_side_effect),
            )

            snapshot = service.sync_snapshot()

            self.assertEqual(list(snapshot["groups"].keys()), ["knowledge", "science"])
            self.assertEqual(snapshot["groups"]["science"]["channels"][0]["latest_video"]["video_id"], "v1")
            self.assertEqual(snapshot["groups"]["knowledge"]["channels"][0]["latest_video"]["video_id"], "v1")
            self.assertEqual(snapshot["channels"]["c1"]["latest_video"]["video_id"], "v1")
            self.assertEqual(snapshot["channels"]["c1"]["group_names"], ["Knowledge", "Science"])

    def test_empty_latest_result_keeps_channel_entry_without_overwrite(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            imported_sections = [
                {
                    "section_name": "Science",
                    "section_key": "science",
                    "group_name": "Science",
                    "group_key": "science",
                    "channels": [
                        {"channel_id": "c1", "channel_name": "Channel One"},
                    ],
                }
            ]
            latest_cache = {
                "c1": {
                    "latest_video": {
                        "video_id": "v-real",
                        "title": "Cached Real Video",
                        "channel_id": "c1",
                        "channel_name": "Channel One",
                        "published_at": FIXED_NOW.isoformat(),
                        "url": "https://www.youtube.com/watch?v=v-real",
                        "thumb": "https://img.youtube.com/vi/v-real/hqdefault.jpg",
                    }
                }
            }

            service, _state = self._build_service(
                temp_dir,
                imported_sections=imported_sections,
                latest_cache=latest_cache,
                refresh_mock=Mock(return_value={"group_name": "Science", "section_name": "Science", "latest_items": [], "latest_videos_found": 0}),
            )

            snapshot = service.sync_snapshot()

            self.assertEqual(snapshot["groups"]["science"]["channels"][0]["latest_video"]["video_id"], "v-real")
            self.assertEqual(snapshot["channels"]["c1"]["latest_video"]["video_id"], "v-real")

    def test_sync_snapshot_writes_latest_video_payloads_to_disk(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            imported_sections = [
                {
                    "section_name": "Science",
                    "section_key": "science",
                    "group_name": "Science",
                    "group_key": "science",
                    "channels": [
                        {"channel_id": "c1", "channel_name": "Channel One"},
                    ],
                }
            ]
            service, _state = self._build_service(
                temp_dir,
                imported_sections=imported_sections,
                refresh_mock=Mock(
                    return_value={
                        "group_name": "Science",
                        "section_name": "Science",
                        "latest_videos_found": 1,
                        "latest_items": [
                            {
                                "video_id": "v1",
                                "title": "Science One",
                                "channel_id": "c1",
                                "channel_name": "Channel One",
                                "published_at": FIXED_NOW.isoformat(),
                                "url": "https://www.youtube.com/watch?v=v1",
                                "thumb": "https://img.youtube.com/vi/v1/hqdefault.jpg",
                            }
                        ],
                    }
                ),
            )

            snapshot = service.sync_snapshot()
            saved_snapshot = load_json_file(service.snapshot_path, {})

            self.assertEqual(saved_snapshot["groups"]["science"]["latest_video"]["video_id"], "v1")
            self.assertEqual(saved_snapshot["groups"]["science"]["channels"][0]["latest_video"]["video_id"], "v1")
            self.assertEqual(saved_snapshot["channels"]["c1"]["latest_video"]["video_id"], "v1")
            self.assertEqual(snapshot["groups"]["science"]["latest_video_count"], 1)

    def test_sync_snapshot_caps_group_videos_at_200(self):
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
            latest_items = [
                self._group_video(
                    f"v{index:03d}",
                    channel_id="c1" if index % 2 == 0 else "c2",
                    channel_name="Channel One" if index % 2 == 0 else "Channel Two",
                    hours_ago=index,
                )
                for index in range(250)
            ]
            service, _state = self._build_service(
                temp_dir,
                imported_sections=imported_sections,
                refresh_mock=Mock(return_value={
                    "group_name": "Science",
                    "section_name": "Science",
                    "channels_scanned": 2,
                    "videos_collected": 250,
                    "videos_stored": 200,
                    "errors": [],
                    "fetched_at": FIXED_NOW.isoformat(),
                    "latest_items": latest_items,
                }),
            )

            snapshot = service.sync_snapshot()

            self.assertEqual(len(snapshot["groups"]["science"]["videos"]), 200)
            self.assertEqual(snapshot["groups"]["science"]["latest_video_count"], 200)
            self.assertEqual(snapshot["groups"]["science"]["videos"][0]["video_id"], "v000")
            self.assertEqual(snapshot["groups"]["science"]["videos"][-1]["video_id"], "v199")
            self.assertEqual(snapshot["groups"]["science"]["diagnostics"]["videos_collected"], 250)
            self.assertEqual(snapshot["groups"]["science"]["diagnostics"]["videos_stored"], 200)

    def test_sync_snapshot_dedupes_duplicate_group_videos(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            imported_sections = [
                {
                    "section_name": "News",
                    "section_key": "news",
                    "group_name": "News",
                    "group_key": "news",
                    "channels": [
                        {"channel_id": "c1", "channel_name": "Desk One"},
                        {"channel_id": "c2", "channel_name": "Desk Two"},
                    ],
                }
            ]
            service, _state = self._build_service(
                temp_dir,
                imported_sections=imported_sections,
                refresh_mock=Mock(return_value={
                    "group_name": "News",
                    "section_name": "News",
                    "channels_scanned": 2,
                    "videos_collected": 3,
                    "videos_stored": 2,
                    "latest_items": [
                        self._group_video("dup-1", channel_id="c1", channel_name="Desk One", hours_ago=3, title="Older copy"),
                        self._group_video("dup-1", channel_id="c2", channel_name="Desk Two", hours_ago=1, title="Newer copy"),
                        self._group_video("uniq-1", channel_id="c1", channel_name="Desk One", hours_ago=2),
                    ],
                }),
            )

            snapshot = service.sync_snapshot()

            self.assertEqual(len(snapshot["groups"]["news"]["videos"]), 2)
            self.assertEqual(snapshot["groups"]["news"]["videos"][0]["video_id"], "dup-1")
            self.assertEqual(snapshot["groups"]["news"]["videos"][0]["title"], "Newer copy")
            self.assertEqual(snapshot["groups"]["news"]["latest_video"]["video_id"], "dup-1")

    def test_sync_snapshot_keeps_group_videos_when_under_limit(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            imported_sections = [
                {
                    "section_name": "Philosophy",
                    "section_key": "philosophy",
                    "group_name": "Philosophy",
                    "group_key": "philosophy",
                    "channels": [
                        {"channel_id": "c1", "channel_name": "Thinker"},
                    ],
                }
            ]
            service, _state = self._build_service(
                temp_dir,
                imported_sections=imported_sections,
                refresh_mock=Mock(return_value={
                    "group_name": "Philosophy",
                    "section_name": "Philosophy",
                    "channels_scanned": 1,
                    "videos_collected": 3,
                    "videos_stored": 3,
                    "latest_items": [
                        self._group_video("p1", channel_id="c1", channel_name="Thinker", hours_ago=1),
                        self._group_video("p2", channel_id="c1", channel_name="Thinker", hours_ago=2),
                        self._group_video("p3", channel_id="c1", channel_name="Thinker", hours_ago=3),
                    ],
                }),
            )

            snapshot = service.sync_snapshot()

            self.assertEqual(len(snapshot["groups"]["philosophy"]["videos"]), 3)
            self.assertEqual(snapshot["groups"]["philosophy"]["latest_video_count"], 3)

    def test_build_page_context_uses_group_video_snapshot_shape(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            service, _state = self._build_service(temp_dir)
            save_json_file(service.snapshot_path, {
                "version": 2,
                "generated_at": FIXED_NOW.isoformat(),
                "synced_at": FIXED_NOW.isoformat(),
                "group_video_limit": 200,
                "all_feed_video_limit": 200,
                "groups": {
                    "news": {
                        "group_name": "News",
                        "group_key": "news",
                        "section_name": "News",
                        "section_key": "news",
                        "source_name": "PocketTube",
                        "imported_at": FIXED_NOW.isoformat(),
                        "channel_count": 2,
                        "latest_video_count": 2,
                        "latest_video": self._group_video("n1", channel_id="c1", channel_name="Desk One", hours_ago=1),
                        "channels": [],
                        "videos": [
                            self._group_video("n1", channel_id="c1", channel_name="Desk One", hours_ago=1),
                            self._group_video("n2", channel_id="c2", channel_name="Desk Two", hours_ago=2),
                        ],
                        "diagnostics": {
                            "group_key": "news",
                            "group_name": "News",
                            "channels_scanned": 2,
                            "videos_collected": 2,
                            "videos_stored": 2,
                            "errors": [],
                            "generated_at": FIXED_NOW.isoformat(),
                            "synced_at": FIXED_NOW.isoformat(),
                        },
                    }
                },
                "channels": {},
                "errors": [],
            })
            service.build_youtube_channel_video_summary = Mock(return_value={"title": "OVERRIDDEN"})

            context = service.build_page_context_for_filter("news")

            self.assertEqual(context["feed_video_count"], 2)
            self.assertEqual(context["feed_video_count_total"], 2)
            self.assertEqual(context["feed_videos"][0]["video_id"], "n1")
            self.assertEqual(context["feed_videos"][0]["title"], "Video n1")
            self.assertEqual(context["feed_filters"][0]["key"], "all")
            self.assertTrue(any(item["key"] == "news" and item["video_count"] == 2 for item in context["feed_filters"]))
            self.assertEqual(context["selected_filter_key"], "news")

    def test_build_page_context_for_filter_all_dedupes_sorts_and_caps_group_videos(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            service, _state = self._build_service(temp_dir)
            news_videos = [
                self._group_video("shared-1", channel_id="c-news-1", channel_name="News One", hours_ago=1, title="Shared News"),
                self._group_video("news-2", channel_id="c-news-2", channel_name="News Two", hours_ago=2, title="News Two"),
            ]
            tech_videos = [
                self._group_video("shared-1", channel_id="c-tech-1", channel_name="Tech One", hours_ago=3, title="Shared Tech"),
                self._group_video("tech-2", channel_id="c-tech-2", channel_name="Tech Two", hours_ago=4, title="Tech Two"),
            ]
            save_json_file(service.snapshot_path, {
                "version": 2,
                "generated_at": FIXED_NOW.isoformat(),
                "synced_at": FIXED_NOW.isoformat(),
                "group_video_limit": 200,
                "all_feed_video_limit": 200,
                "groups": {
                    "news": {
                        "group_name": "News",
                        "group_key": "news",
                        "section_name": "News",
                        "section_key": "news",
                        "source_name": "PocketTube",
                        "imported_at": FIXED_NOW.isoformat(),
                        "channel_count": 2,
                        "latest_video_count": 2,
                        "latest_video": news_videos[0],
                        "channels": [],
                        "videos": news_videos,
                        "diagnostics": {
                            "group_key": "news",
                            "group_name": "News",
                            "channels_scanned": 2,
                            "videos_collected": 2,
                            "videos_stored": 2,
                            "upload_playlist_ids": ["UU1"],
                            "errors": [],
                            "generated_at": FIXED_NOW.isoformat(),
                            "synced_at": FIXED_NOW.isoformat(),
                        },
                    },
                    "tech": {
                        "group_name": "Tech",
                        "group_key": "tech",
                        "section_name": "Tech",
                        "section_key": "tech",
                        "source_name": "PocketTube",
                        "imported_at": FIXED_NOW.isoformat(),
                        "channel_count": 2,
                        "latest_video_count": 2,
                        "latest_video": tech_videos[0],
                        "channels": [],
                        "videos": tech_videos,
                        "diagnostics": {
                            "group_key": "tech",
                            "group_name": "Tech",
                            "channels_scanned": 2,
                            "videos_collected": 2,
                            "videos_stored": 2,
                            "upload_playlist_ids": ["UU2"],
                            "errors": [],
                            "generated_at": FIXED_NOW.isoformat(),
                            "synced_at": FIXED_NOW.isoformat(),
                        },
                    },
                },
                "channels": {},
                "errors": [],
            })

            context = service.build_page_context_for_filter("all")

            self.assertEqual(context["selected_filter_key"], "all")
            self.assertEqual(context["feed_video_count_total"], 3)
            self.assertEqual(context["feed_video_count"], 3)
            self.assertEqual([video["video_id"] for video in context["feed_videos"]], ["shared-1", "news-2", "tech-2"])
            self.assertEqual(context["feed_filters"][0]["video_count"], 3)
            self.assertTrue(any(item["key"] == "news" and item["video_count"] == 2 for item in context["feed_filters"]))
            self.assertTrue(any(item["key"] == "tech" and item["video_count"] == 2 for item in context["feed_filters"]))

    def test_build_page_context_canonical_filters_use_group_video_counts(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            service, _state = self._build_service(temp_dir)
            news_videos = [self._group_video(f"news-{index:03d}", channel_id="c-news", channel_name="News Desk", hours_ago=index) for index in range(200)]
            tech_videos = [self._group_video(f"tech-{index:03d}", channel_id="c-tech", channel_name="Tech Desk", hours_ago=index + 300) for index in range(200)]
            save_json_file(service.snapshot_path, {
                "version": 2,
                "generated_at": FIXED_NOW.isoformat(),
                "synced_at": FIXED_NOW.isoformat(),
                "group_video_limit": 200,
                "all_feed_video_limit": 200,
                "groups": {
                    "news": {
                        "group_name": "News",
                        "group_key": "news",
                        "section_name": "News",
                        "section_key": "news",
                        "source_name": "PocketTube",
                        "imported_at": FIXED_NOW.isoformat(),
                        "channel_count": 1,
                        "latest_video_count": 200,
                        "latest_video": news_videos[0],
                        "channels": [],
                        "videos": news_videos,
                        "diagnostics": {"group_key": "news", "group_name": "News", "videos_collected": 200, "videos_stored": 200, "errors": []},
                    },
                    "tech": {
                        "group_name": "Tech",
                        "group_key": "tech",
                        "section_name": "Tech",
                        "section_key": "tech",
                        "source_name": "PocketTube",
                        "imported_at": FIXED_NOW.isoformat(),
                        "channel_count": 1,
                        "latest_video_count": 200,
                        "latest_video": tech_videos[0],
                        "channels": [],
                        "videos": tech_videos,
                        "diagnostics": {"group_key": "tech", "group_name": "Tech", "videos_collected": 200, "videos_stored": 200, "errors": []},
                    },
                },
                "channels": {},
                "errors": [],
            })

            context = service.build_page_context_for_filter("news")

            self.assertEqual(context["selected_filter_key"], "news")
            self.assertEqual(context["selected_filter_count"], 200)
            self.assertEqual(context["feed_video_count_total"], 400)
            self.assertEqual(context["feed_video_count"], 50)
            self.assertEqual(context["selected_filter_display_count"], 50)
            self.assertEqual(context["display_limit"], 50)
            self.assertTrue(any(item["key"] == "news" and item["video_count"] == 200 for item in context["feed_filters"]))
            self.assertTrue(any(item["key"] == "tech" and item["video_count"] == 200 for item in context["feed_filters"]))

    def test_build_page_context_canonical_favorites_maps_my_favorite_group(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            service, _state = self._build_service(temp_dir)
            favorite_videos = [self._group_video(f"fav-{index:03d}", channel_id="c-fav", channel_name="Favorite Desk", hours_ago=index) for index in range(3)]
            save_json_file(service.snapshot_path, {
                "version": 2,
                "generated_at": FIXED_NOW.isoformat(),
                "synced_at": FIXED_NOW.isoformat(),
                "groups": {
                    "myfavorite": {
                        "group_name": "My Favorite",
                        "group_key": "myfavorite",
                        "section_name": "My Favorite",
                        "section_key": "myfavorite",
                        "source_name": "PocketTube",
                        "imported_at": FIXED_NOW.isoformat(),
                        "channel_count": 1,
                        "latest_video_count": 3,
                        "latest_video": favorite_videos[0],
                        "channels": [],
                        "videos": favorite_videos,
                        "diagnostics": {"group_key": "myfavorite", "group_name": "My Favorite", "videos_collected": 3, "videos_stored": 3, "errors": []},
                    }
                },
                "channels": {},
                "errors": [],
            })

            context = service.build_page_context_for_filter("favorites", display_limit=100)

            self.assertEqual(context["selected_filter_key"], "favorites")
            self.assertEqual(context["selected_filter_count"], 3)
            self.assertEqual(context["feed_video_count"], 3)
            self.assertEqual([video["video_id"] for video in context["feed_videos"]], ["fav-000", "fav-001", "fav-002"])

    def test_build_page_context_canonical_cinema_maps_movise_group(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            service, _state = self._build_service(temp_dir)
            cinema_videos = [self._group_video(f"mov-{index:03d}", channel_id="c-mov", channel_name="Movise Desk", hours_ago=index) for index in range(4)]
            save_json_file(service.snapshot_path, {
                "version": 2,
                "generated_at": FIXED_NOW.isoformat(),
                "synced_at": FIXED_NOW.isoformat(),
                "groups": {
                    "movise": {
                        "group_name": "movise",
                        "group_key": "movise",
                        "section_name": "movise",
                        "section_key": "movise",
                        "source_name": "PocketTube",
                        "imported_at": FIXED_NOW.isoformat(),
                        "channel_count": 1,
                        "latest_video_count": 4,
                        "latest_video": cinema_videos[0],
                        "channels": [],
                        "videos": cinema_videos,
                        "diagnostics": {"group_key": "movise", "group_name": "movise", "videos_collected": 4, "videos_stored": 4, "errors": []},
                    }
                },
                "channels": {},
                "errors": [],
            })

            context = service.build_page_context_for_filter("cinema", display_limit=100)

            self.assertEqual(context["selected_filter_key"], "cinema")
            self.assertEqual(context["selected_filter_count"], 4)
            self.assertEqual(context["feed_video_count"], 4)
            self.assertTrue(any(item["key"] == "cinema" and item["video_count"] == 4 for item in context["feed_filters"]))

    def test_build_page_context_display_limit_options_control_feed_length(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            service, _state = self._build_service(temp_dir)
            news_videos = [self._group_video(f"news-{index:03d}", channel_id="c-news", channel_name="News Desk", hours_ago=index) for index in range(180)]
            save_json_file(service.snapshot_path, {
                "version": 2,
                "generated_at": FIXED_NOW.isoformat(),
                "synced_at": FIXED_NOW.isoformat(),
                "groups": {
                    "news": {
                        "group_name": "News",
                        "group_key": "news",
                        "section_name": "News",
                        "section_key": "news",
                        "source_name": "PocketTube",
                        "imported_at": FIXED_NOW.isoformat(),
                        "channel_count": 1,
                        "latest_video_count": 180,
                        "latest_video": news_videos[0],
                        "channels": [],
                        "videos": news_videos,
                        "diagnostics": {"group_key": "news", "group_name": "News", "videos_collected": 180, "videos_stored": 180, "errors": []},
                    }
                },
                "channels": {},
                "errors": [],
            })

            default_context = service.build_page_context_for_filter("news")
            limit_100_context = service.build_page_context_for_filter("news", display_limit=100)
            limit_150_context = service.build_page_context_for_filter("news", display_limit=150)
            limit_200_context = service.build_page_context_for_filter("news", display_limit=200)

            self.assertEqual(default_context["display_limit"], 50)
            self.assertEqual(default_context["feed_video_count"], 50)
            self.assertEqual(limit_100_context["feed_video_count"], 100)
            self.assertEqual(limit_150_context["feed_video_count"], 150)
            self.assertEqual(limit_200_context["feed_video_count"], 180)

    def test_build_pockettube_coverage_report_marks_group_without_channels(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            service, _state = self._build_service(temp_dir)
            save_json_file(service.snapshot_path, {
                "version": 1,
                "generated_at": FIXED_NOW.isoformat(),
                "synced_at": FIXED_NOW.isoformat(),
                "group_video_limit": 200,
                "all_feed_video_limit": 200,
                "groups": {
                    "archive": {
                        "group_name": "Archive",
                        "group_key": "archive",
                        "section_name": "Archive",
                        "section_key": "archive",
                        "source_name": "PocketTube",
                        "imported_at": FIXED_NOW.isoformat(),
                        "channel_count": 0,
                        "latest_video_count": 0,
                        "latest_video": {},
                        "channels": [],
                        "videos": [],
                        "diagnostics": {
                            "group_key": "archive",
                            "group_name": "Archive",
                            "channels_scanned": 0,
                            "videos_collected": 0,
                            "videos_stored": 0,
                            "errors": [],
                            "generated_at": FIXED_NOW.isoformat(),
                            "synced_at": FIXED_NOW.isoformat(),
                        },
                    }
                },
                "channels": {},
                "errors": [],
            })

            report = service.build_pockettube_coverage_report()

            self.assertEqual(report["summary"]["group_count"], 1)
            self.assertEqual(report["groups"][0]["group_key"], "archive")
            self.assertEqual(report["groups"][0]["reason"], "no_channels_mapped")

    def test_build_pockettube_coverage_report_explains_news_snapshot_gap(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            latest_cache = {
                "c1": {
                    "uploads_playlist_id": "UUc1",
                    "latest_source": "playlist_items",
                    "latest_video": {
                        "video_id": "v1",
                        "title": "News One",
                        "channel_id": "c1",
                        "channel_name": "News Channel",
                        "published_at": FIXED_NOW.isoformat(),
                        "url": "https://www.youtube.com/watch?v=v1",
                    },
                },
                "c2": {
                    "uploads_playlist_id": "UUc2",
                    "latest_source": "playlist_items",
                    "latest_video": {
                        "video_id": "v2",
                        "title": "News Two",
                        "channel_id": "c2",
                        "channel_name": "News Desk",
                        "published_at": FIXED_NOW.isoformat(),
                        "url": "https://www.youtube.com/watch?v=v2",
                    },
                },
            }
            imported_sections = [
                {
                    "section_name": "News",
                    "section_key": "news",
                    "group_name": "News",
                    "group_key": "news",
                    "channels": [
                        {"channel_id": "c1", "channel_name": "News Channel"},
                        {"channel_id": "c2", "channel_name": "News Desk"},
                    ],
                }
            ]
            service, _state = self._build_service(
                temp_dir,
                imported_sections=imported_sections,
                latest_cache=latest_cache,
            )
            save_json_file(service.snapshot_path, {
                "version": 1,
                "generated_at": FIXED_NOW.isoformat(),
                "synced_at": FIXED_NOW.isoformat(),
                "group_video_limit": 200,
                "all_feed_video_limit": 200,
                "groups": {
                    "news": {
                        "group_name": "News",
                        "group_key": "news",
                        "section_name": "News",
                        "section_key": "news",
                        "source_name": "PocketTube",
                        "imported_at": FIXED_NOW.isoformat(),
                        "channel_count": 2,
                        "latest_video_count": 24,
                        "latest_video": {
                            "video_id": "v1",
                            "title": "News One",
                            "channel_id": "c1",
                            "channel_name": "News Channel",
                            "published_at": FIXED_NOW.isoformat(),
                            "url": "https://www.youtube.com/watch?v=v1",
                        },
                        "channels": [
                            {
                                "channel_id": "c1",
                                "channel_title": "News Channel",
                                "group_names": ["News"],
                                "latest_video": {
                                    "video_id": "v1",
                                    "title": "News One",
                                    "channel_id": "c1",
                                    "channel_name": "News Channel",
                                    "published_at": FIXED_NOW.isoformat(),
                                    "url": "https://www.youtube.com/watch?v=v1",
                                },
                                "latest_video_id": "v1",
                                "published_at": FIXED_NOW.isoformat(),
                                "published_display": "2026-06-02 12:00",
                                "thumbnail": "https://img.youtube.com/vi/v1/hqdefault.jpg",
                                "url": "https://www.youtube.com/watch?v=v1",
                                "reason_tags": ["latest-cached"],
                            },
                            {
                                "channel_id": "c2",
                                "channel_title": "News Desk",
                                "group_names": ["News"],
                                "latest_video": {
                                    "video_id": "v2",
                                    "title": "News Two",
                                    "channel_id": "c2",
                                    "channel_name": "News Desk",
                                    "published_at": FIXED_NOW.isoformat(),
                                    "url": "https://www.youtube.com/watch?v=v2",
                                },
                                "latest_video_id": "v2",
                                "published_at": FIXED_NOW.isoformat(),
                                "published_display": "2026-06-02 12:00",
                                "thumbnail": "https://img.youtube.com/vi/v2/hqdefault.jpg",
                                "url": "https://www.youtube.com/watch?v=v2",
                                "reason_tags": ["latest-cached"],
                            },
                        ],
                        "videos": [],
                        "diagnostics": {
                            "group_key": "news",
                            "group_name": "News",
                            "channels_scanned": 2,
                            "videos_collected": 0,
                            "videos_stored": 0,
                            "errors": [],
                            "generated_at": FIXED_NOW.isoformat(),
                            "synced_at": FIXED_NOW.isoformat(),
                        },
                    }
                },
                "channels": {},
                "errors": [],
            })

            report = service.build_pockettube_coverage_report()

            news = report["groups"][0]
            self.assertEqual(news["group_key"], "news")
            self.assertEqual(news["channels_assigned"], 2)
            self.assertEqual(news["upload_playlist_ids"], ["UUc1", "UUc2"])
            self.assertEqual(news["videos_fetched_before_dedupe"], 0)
            self.assertEqual(news["videos_stored_after_dedupe"], 0)
            self.assertEqual(news["reason"], "empty_uploads")
            self.assertTrue(any("group video list" in note for note in news["notes"]))

    def test_build_pockettube_coverage_report_prefers_fetch_error_over_missing_upload_playlist(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            service, _state = self._build_service(temp_dir)
            save_json_file(service.snapshot_path, {
                "version": 1,
                "generated_at": FIXED_NOW.isoformat(),
                "synced_at": FIXED_NOW.isoformat(),
                "group_video_limit": 200,
                "all_feed_video_limit": 200,
                "groups": {
                    "news": {
                        "group_name": "News",
                        "group_key": "news",
                        "section_name": "News",
                        "section_key": "news",
                        "source_name": "PocketTube",
                        "imported_at": FIXED_NOW.isoformat(),
                        "channel_count": 1,
                        "latest_video_count": 0,
                        "latest_video": {},
                        "channels": [
                            {
                                "channel_id": "UC1",
                                "channel_title": "Channel One",
                                "group_names": ["News"],
                                "latest_video": {},
                                "latest_video_id": "",
                                "published_at": "",
                                "published_display": "",
                                "thumbnail": "",
                                "url": "",
                                "reason_tags": [],
                            }
                        ],
                        "videos": [],
                        "diagnostics": {
                            "group_key": "news",
                            "group_name": "News",
                            "channels_scanned": 1,
                            "channels_with_upload_playlist": 0,
                            "channels_missing_upload_playlist": 1,
                            "videos_collected": 0,
                            "videos_stored": 0,
                            "upload_playlist_ids": [],
                            "errors": ["UC1: fetch_error - youtube_service_unavailable"],
                            "generated_at": FIXED_NOW.isoformat(),
                            "synced_at": FIXED_NOW.isoformat(),
                        },
                    }
                },
                "channels": {},
                "errors": [],
            })

            report = service.build_pockettube_coverage_report()

            self.assertEqual(report["groups"][0]["reason"], "fetch_error")

    def test_finalize_snapshot_populates_top_level_channels_from_groups(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            service, _state = self._build_service(temp_dir)
            raw_snapshot = {
                "version": 1,
                "generated_at": FIXED_NOW.isoformat(),
                "synced_at": FIXED_NOW.isoformat(),
                "groups": {
                    "science": {
                        "group_name": "Science",
                        "group_key": "science",
                        "section_name": "Science",
                        "section_key": "science",
                        "source_name": "PocketTube",
                        "imported_at": FIXED_NOW.isoformat(),
                        "channel_count": 1,
                        "latest_video_count": 0,
                        "latest_video": {},
                        "channels": [
                            {
                                "channel_id": "c1",
                                "channel_title": "Channel One",
                                "group_names": ["Science"],
                                "group_key": "science",
                                "latest_video": {},
                                "latest_video_id": "",
                                "published_at": "",
                                "thumbnail": "",
                                "url": "",
                                "reason_tags": ["source-diverse"],
                            }
                        ],
                    }
                },
                "channels": {},
                "errors": [],
            }

            finalized = service.finalize_snapshot(raw_snapshot)

            self.assertEqual(list(finalized["groups"].keys()), ["science"])
            self.assertEqual(list(finalized["channels"].keys()), ["c1"])
            self.assertEqual(finalized["channels"]["c1"]["group_names"], ["Science"])
            self.assertEqual(finalized["channels"]["c1"]["latest_video"], {})

    def test_finalize_snapshot_merges_duplicate_group_names(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            service, _state = self._build_service(temp_dir)
            raw_snapshot = {
                "version": 1,
                "generated_at": FIXED_NOW.isoformat(),
                "synced_at": FIXED_NOW.isoformat(),
                "groups": {
                    "science": {
                        "group_name": "Science",
                        "group_key": "science",
                        "section_name": "Science",
                        "section_key": "science",
                        "source_name": "PocketTube",
                        "imported_at": FIXED_NOW.isoformat(),
                        "channel_count": 1,
                        "latest_video_count": 0,
                        "latest_video": {},
                        "channels": [
                            {
                                "channel_id": "c1",
                                "channel_title": "Channel One",
                                "group_names": ["Science"],
                                "group_key": "science",
                                "latest_video": {},
                                "latest_video_id": "",
                                "published_at": "",
                                "thumbnail": "",
                                "url": "",
                                "reason_tags": ["source-diverse"],
                            }
                        ],
                    },
                    "knowledge": {
                        "group_name": "Knowledge",
                        "group_key": "knowledge",
                        "section_name": "Knowledge",
                        "section_key": "knowledge",
                        "source_name": "PocketTube",
                        "imported_at": FIXED_NOW.isoformat(),
                        "channel_count": 1,
                        "latest_video_count": 0,
                        "latest_video": {},
                        "channels": [
                            {
                                "channel_id": "c1",
                                "channel_title": "Channel One",
                                "group_names": ["Knowledge"],
                                "group_key": "knowledge",
                                "latest_video": {},
                                "latest_video_id": "",
                                "published_at": "",
                                "thumbnail": "",
                                "url": "",
                                "reason_tags": ["source-diverse"],
                            }
                        ],
                    },
                },
                "channels": {},
                "errors": [],
            }

            finalized = service.finalize_snapshot(raw_snapshot)

            self.assertEqual(list(finalized["channels"].keys()), ["c1"])
            self.assertEqual(finalized["channels"]["c1"]["group_names"], ["Knowledge", "Science"])

    def test_finalize_snapshot_keeps_empty_latest_video_channel_entry(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            service, _state = self._build_service(temp_dir)
            raw_snapshot = {
                "version": 1,
                "generated_at": FIXED_NOW.isoformat(),
                "synced_at": FIXED_NOW.isoformat(),
                "groups": {
                    "science": {
                        "group_name": "Science",
                        "group_key": "science",
                        "section_name": "Science",
                        "section_key": "science",
                        "source_name": "PocketTube",
                        "imported_at": FIXED_NOW.isoformat(),
                        "channel_count": 1,
                        "latest_video_count": 0,
                        "latest_video": {},
                        "channels": [
                            {
                                "channel_id": "c1",
                                "channel_title": "Channel One",
                                "group_names": ["Science"],
                                "group_key": "science",
                                "latest_video": {},
                                "latest_video_id": "",
                                "published_at": "",
                                "thumbnail": "",
                                "url": "",
                                "reason_tags": ["source-diverse"],
                            }
                        ],
                    }
                },
                "channels": {},
                "errors": [],
            }

            finalized = service.finalize_snapshot(raw_snapshot)

            self.assertEqual(list(finalized["channels"].keys()), ["c1"])
            self.assertEqual(finalized["channels"]["c1"]["latest_video"], {})

    def test_finalize_snapshot_preserves_real_latest_video(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            service, _state = self._build_service(temp_dir)
            raw_snapshot = {
                "version": 1,
                "generated_at": FIXED_NOW.isoformat(),
                "synced_at": FIXED_NOW.isoformat(),
                "groups": {
                    "science": {
                        "group_name": "Science",
                        "group_key": "science",
                        "section_name": "Science",
                        "section_key": "science",
                        "source_name": "PocketTube",
                        "imported_at": FIXED_NOW.isoformat(),
                        "channel_count": 1,
                        "latest_video_count": 1,
                        "latest_video": {
                            "video_id": "v1",
                            "title": "Science One",
                            "channel_id": "c1",
                            "channel_name": "Channel One",
                            "published_at": FIXED_NOW.isoformat(),
                            "url": "https://www.youtube.com/watch?v=v1",
                            "thumb": "https://img.youtube.com/vi/v1/hqdefault.jpg",
                        },
                        "channels": [
                            {
                                "channel_id": "c1",
                                "channel_title": "Channel One",
                                "group_names": ["Science"],
                                "group_key": "science",
                                "latest_video": {
                                    "video_id": "v1",
                                    "title": "Science One",
                                    "channel_id": "c1",
                                    "channel_name": "Channel One",
                                    "published_at": FIXED_NOW.isoformat(),
                                    "url": "https://www.youtube.com/watch?v=v1",
                                    "thumb": "https://img.youtube.com/vi/v1/hqdefault.jpg",
                                },
                                "latest_video_id": "v1",
                                "published_at": FIXED_NOW.isoformat(),
                                "thumbnail": "https://img.youtube.com/vi/v1/hqdefault.jpg",
                                "url": "https://www.youtube.com/watch?v=v1",
                                "reason_tags": ["latest-cached"],
                            }
                        ],
                    }
                },
                "channels": {},
                "errors": [],
            }

            finalized = service.finalize_snapshot(raw_snapshot)

            self.assertEqual(finalized["channels"]["c1"]["latest_video"]["video_id"], "v1")
            self.assertEqual(finalized["channels"]["c1"]["latest_video_id"], "v1")

    def test_finalize_snapshot_does_not_overwrite_real_latest_with_empty(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            service, _state = self._build_service(temp_dir)
            raw_snapshot = {
                "version": 1,
                "generated_at": FIXED_NOW.isoformat(),
                "synced_at": FIXED_NOW.isoformat(),
                "groups": {
                    "science": {
                        "group_name": "Science",
                        "group_key": "science",
                        "section_name": "Science",
                        "section_key": "science",
                        "source_name": "PocketTube",
                        "imported_at": FIXED_NOW.isoformat(),
                        "channel_count": 1,
                        "latest_video_count": 0,
                        "latest_video": {},
                        "channels": [
                            {
                                "channel_id": "c1",
                                "channel_title": "Channel One",
                                "group_names": ["Science"],
                                "group_key": "science",
                                "latest_video": {},
                                "latest_video_id": "",
                                "published_at": "",
                                "thumbnail": "",
                                "url": "",
                                "reason_tags": ["source-diverse"],
                            }
                        ],
                    }
                },
                "channels": {
                    "c1": {
                        "channel_title": "Channel One",
                        "latest_video": {
                            "video_id": "v-real",
                            "title": "Real Video",
                            "channel_id": "c1",
                            "channel_name": "Channel One",
                            "published_at": FIXED_NOW.isoformat(),
                            "url": "https://www.youtube.com/watch?v=v-real",
                            "thumb": "https://img.youtube.com/vi/v-real/hqdefault.jpg",
                        },
                        "group_names": ["Science"],
                        "latest_video_id": "v-real",
                        "published_at": FIXED_NOW.isoformat(),
                        "published_display": "2026-06-02 12:00",
                        "thumbnail": "https://img.youtube.com/vi/v-real/hqdefault.jpg",
                        "url": "https://www.youtube.com/watch?v=v-real",
                        "reason_tags": ["latest-cached"],
                    }
                },
                "errors": [],
            }

            finalized = service.finalize_snapshot(raw_snapshot)

            self.assertEqual(finalized["channels"]["c1"]["latest_video"]["video_id"], "v-real")
            self.assertEqual(finalized["channels"]["c1"]["latest_video_id"], "v-real")

    def test_registry_fallback_builds_non_empty_snapshot(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            registry_payload = {
                "latest": {
                    "source_name": "PocketTube",
                    "imported_at": FIXED_NOW.isoformat(),
                    "fingerprint": "registry-fallback",
                    "source_structure": {
                        "top_level_groups": ["Science"],
                        "main_collection_page": "",
                        "meta_keys": [],
                    },
                    "section_count": 1,
                    "group_count": 1,
                    "channel_count": 1,
                    "sections": [
                        {
                            "section_name": "Science",
                            "section_key": "science",
                            "group_name": "Science",
                            "group_key": "science",
                            "tier": "best",
                            "channel_count": 1,
                            "channels": [
                                {
                                    "channel_name": "UCTDc1RLIHHNjN5WlHoZwXQg",
                                    "channel_id": "",
                                    "channel_key": "uctdc1rlihhnjn5wlhozwxqg",
                                    "section_name": "Science",
                                    "section_key": "science",
                                    "group_name": "Science",
                                    "group_key": "science",
                                    "tier": "best",
                                }
                            ],
                        }
                    ],
                    "channels": [
                        {
                            "channel_name": "UCTDc1RLIHHNjN5WlHoZwXQg",
                            "channel_id": "",
                            "channel_key": "uctdc1rlihhnjn5wlhozwxqg",
                            "section_name": "Science",
                            "section_key": "science",
                            "group_name": "Science",
                            "group_key": "science",
                            "tier": "best",
                        }
                    ],
                }
            }
            latest_cache = {
                "UCTDc1RLIHHNjN5WlHoZwXQg": {
                    "latest_video": {
                        "video_id": "v-science",
                        "title": "Science Today",
                        "channel_id": "UCTDc1RLIHHNjN5WlHoZwXQg",
                        "channel_name": "Science Channel",
                        "published_at": FIXED_NOW.isoformat(),
                        "url": "https://www.youtube.com/watch?v=v-science",
                        "thumb": "https://img.youtube.com/vi/v-science/hqdefault.jpg",
                    }
                }
            }
            service, _state = self._build_service(
                temp_dir,
                imported_sections=[],
                latest_cache=latest_cache,
                registry_payload=registry_payload,
                refresh_mock=Mock(),
            )

            snapshot = service.sync_snapshot()

            self.assertEqual(list(snapshot["groups"].keys()), ["science"])
            self.assertEqual(snapshot["groups"]["science"]["channel_count"], 1)
            self.assertEqual(len(snapshot["channels"]), 1)
            self.assertEqual(snapshot["channels"]["UCTDc1RLIHHNjN5WlHoZwXQg"]["group_names"], ["Science"])
            self.assertEqual(snapshot["groups"]["science"]["latest_video"]["video_id"], "v-science")
            self.assertEqual(snapshot["errors"], [])

    def test_registry_multi_group_channel_is_deduped_with_multiple_group_names(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            registry_payload = {
                "latest": {
                    "source_name": "PocketTube",
                    "imported_at": FIXED_NOW.isoformat(),
                    "fingerprint": "registry-multi-group",
                    "section_count": 2,
                    "group_count": 2,
                    "channel_count": 2,
                    "sections": [
                        {
                            "section_name": "Science",
                            "section_key": "science",
                            "group_name": "Science",
                            "group_key": "science",
                            "tier": "best",
                            "channel_count": 1,
                            "channels": [
                                {
                                    "channel_name": "UCTDc1RLIHHNjN5WlHoZwXQg",
                                    "channel_id": "",
                                    "channel_key": "uctdc1rlihhnjn5wlhozwxqg",
                                    "section_name": "Science",
                                    "section_key": "science",
                                    "group_name": "Science",
                                    "group_key": "science",
                                    "tier": "best",
                                }
                            ],
                        },
                        {
                            "section_name": "Knowledge",
                            "section_key": "knowledge",
                            "group_name": "Knowledge",
                            "group_key": "knowledge",
                            "tier": "best",
                            "channel_count": 1,
                            "channels": [
                                {
                                    "channel_name": "UCTDc1RLIHHNjN5WlHoZwXQg",
                                    "channel_id": "",
                                    "channel_key": "uctdc1rlihhnjn5wlhozwxqg",
                                    "section_name": "Knowledge",
                                    "section_key": "knowledge",
                                    "group_name": "Knowledge",
                                    "group_key": "knowledge",
                                    "tier": "best",
                                }
                            ],
                        },
                    ],
                    "channels": [],
                }
            }
            service, _state = self._build_service(
                temp_dir,
                imported_sections=[],
                registry_payload=registry_payload,
                refresh_mock=Mock(),
            )

            snapshot = service.sync_snapshot()
            channel_entry = snapshot["channels"]["UCTDc1RLIHHNjN5WlHoZwXQg"]

            self.assertEqual(list(snapshot["groups"].keys()), ["knowledge", "science"])
            self.assertEqual(len(snapshot["channels"]), 1)
            self.assertEqual(channel_entry["group_names"], ["Knowledge", "Science"])
            self.assertEqual(snapshot["groups"]["science"]["channels"][0]["channel_id"], "UCTDc1RLIHHNjN5WlHoZwXQg")
            self.assertEqual(snapshot["groups"]["knowledge"]["channels"][0]["channel_id"], "UCTDc1RLIHHNjN5WlHoZwXQg")

    def test_failed_latest_video_fetch_records_errors_but_writes_snapshot(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            registry_payload = {
                "latest": {
                    "source_name": "PocketTube",
                    "imported_at": FIXED_NOW.isoformat(),
                    "fingerprint": "registry-fallback",
                    "section_count": 1,
                    "group_count": 1,
                    "channel_count": 1,
                    "sections": [
                        {
                            "section_name": "Science",
                            "section_key": "science",
                            "group_name": "Science",
                            "group_key": "science",
                            "tier": "best",
                            "channel_count": 1,
                            "channels": [
                                {
                                    "channel_name": "UCTDc1RLIHHNjN5WlHoZwXQg",
                                    "channel_id": "",
                                    "channel_key": "uctdc1rlihhnjn5wlhozwxqg",
                                    "section_name": "Science",
                                    "section_key": "science",
                                    "group_name": "Science",
                                    "group_key": "science",
                                    "tier": "best",
                                }
                            ],
                        }
                    ],
                    "channels": [],
                }
            }
            service, _state = self._build_service(
                temp_dir,
                imported_sections=[],
                registry_payload=registry_payload,
                refresh_mock=Mock(side_effect=RuntimeError("boom")),
            )

            snapshot = service.sync_snapshot()

            self.assertEqual(list(snapshot["groups"].keys()), ["science"])
            self.assertEqual(snapshot["groups"]["science"]["channel_count"], 1)
            self.assertEqual(len(snapshot["channels"]), 1)
            self.assertEqual(snapshot["groups"]["science"]["latest_video"], {})
            self.assertTrue(snapshot["errors"])
            self.assertIn("boom", snapshot["errors"][0])
            self.assertTrue(service.snapshot_path.exists())

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

    def test_post_sync_returns_quickly_and_persists_requested_status(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            trigger_mock = Mock(return_value=({"status": "started", "ok": True}, 200))
            service, _state = self._build_service(temp_dir, trigger_mock=trigger_mock)

            payload, status_code = service.request_sync(scope="science")
            saved_status = service.load_sync_status()

            self.assertEqual(status_code, 200)
            self.assertTrue(payload["ok"])
            self.assertEqual(saved_status["status"], "queued")
            self.assertEqual(saved_status["scope"], "science")
            trigger_mock.assert_called_once_with(scope="science")

    def test_existing_snapshot_still_renders_after_failed_or_queued_sync(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            trigger_mock = Mock(return_value=({"ok": False, "error": "boom"}, 502))
            service, _state = self._build_service(temp_dir, trigger_mock=trigger_mock)
            save_json_file(
                service.snapshot_path,
                {
                    "version": 1,
                    "generated_at": FIXED_NOW.isoformat(),
                    "synced_at": FIXED_NOW.isoformat(),
                    "groups": {
                        "science": {
                            "group_name": "Science",
                            "group_key": "science",
                            "section_name": "Science",
                            "section_key": "science",
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
                },
            )

            payload, status_code = service.request_sync(scope="")
            context = service.build_page_context()

            self.assertEqual(status_code, 502)
            self.assertFalse(payload["ok"])
            self.assertFalse(context["empty_state"])
            self.assertEqual(context["groups"][0]["group_name"], "Science")
            self.assertEqual(context["sync_status"]["status"], "failed")

    def test_failed_payload_does_not_download_and_marks_failed(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            github_refresh_mock = Mock()
            service, _state = self._build_service(temp_dir, github_refresh_mock=github_refresh_mock)

            result = service.ingest_github_snapshot_update({
                "status": "failed",
                "error": "workflow failed",
                "run_id": "run-1",
                "run_url": "https://example.test/run-1",
            })

            self.assertTrue(result["ok"])
            self.assertEqual(result["status"], "failed")
            self.assertEqual(result["sync_status"]["status"], "failed")
            self.assertIn("workflow failed", result["sync_status"]["last_error"])
            github_refresh_mock.assert_not_called()

    def test_completed_payload_404_keeps_local_snapshot_and_marks_failed(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            github_refresh_mock = Mock(side_effect=RuntimeError("GitHub snapshot file missing: cache/youtube_latest_snapshot.json"))
            service, _state = self._build_service(temp_dir, github_refresh_mock=github_refresh_mock)
            original_snapshot = {
                "version": 1,
                "generated_at": FIXED_NOW.isoformat(),
                "synced_at": FIXED_NOW.isoformat(),
                "groups": {
                    "science": {
                        "group_name": "Science",
                        "group_key": "science",
                        "section_name": "Science",
                        "section_key": "science",
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
            save_json_file(service.snapshot_path, original_snapshot)

            result = service.ingest_github_snapshot_update({
                "status": "completed",
                "run_id": "run-2",
                "run_url": "https://example.test/run-2",
            })

            self.assertFalse(result["ok"])
            self.assertEqual(result["status"], "failed")
            self.assertEqual(result["sync_status"]["status"], "failed")
            self.assertEqual(result["sync_status"]["run_id"], "run-2")
            self.assertEqual(result["sync_status"]["run_url"], "https://example.test/run-2")
            self.assertEqual(
                result["sync_status"]["last_error"],
                "GitHub snapshot file missing: cache/youtube_latest_snapshot.json",
            )
            self.assertEqual(load_json_file(service.snapshot_path, {}), original_snapshot)
            github_refresh_mock.assert_called_once()

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

    def test_main_pockettube_route_uses_snapshot_only(self):
        dragon_app.app.config["TESTING"] = True
        client = dragon_app.app.test_client()
        mock_service = Mock()
        mock_service.build_page_context_for_filter.return_value = {
            "title": "PocketTube Freshness",
            "snapshot": {"version": 1, "groups": {}, "channels": {}, "errors": []},
            "sync_status": {"status": "idle"},
            "groups": [],
            "group_count": 0,
            "channel_count": 0,
            "feed_videos": [
                {
                    "video_id": "v1",
                    "title": "PocketTube Test Video",
                    "channel_title": "Channel One",
                    "published_display": "2026-06-02 12:00",
                    "published_at": FIXED_NOW.isoformat(),
                    "thumbnail": "https://img.youtube.com/vi/v1/hqdefault.jpg",
                    "detail_url": "/video/yt-v1",
                    "url": "https://www.youtube.com/watch?v=v1",
                    "group_names": ["Science"],
                    "group_keys": ["science"],
                    "reason_tags": ["cached-latest"],
                }
            ],
            "feed_groups": [{"group_key": "science", "group_name": "Science", "video_count": 1, "channel_count": 1, "empty_channel_count": 0}],
            "feed_video_count": 1,
            "feed_video_count_total": 1,
            "feed_empty_channels": [],
            "feed_empty_channel_count": 0,
            "feed_empty_group_count": 0,
            "feed_filters": [
                {"key": "all", "label": "All", "video_count": 1},
                {"key": "science", "label": "Science", "video_count": 1},
            ],
            "selected_filter_key": "all",
            "selected_filter_label": "All",
            "selected_filter_count": 1,
            "has_latest": True,
            "generated_at": "",
            "synced_at": "",
            "errors": [],
            "empty_state": False,
            "empty_reason": "no_cached_latest",
            "sync_notice": "",
            "snapshot_status": {
                "state": "ok",
                "message": "Feed is using the latest cached PocketTube snapshot.",
                "is_stale": False,
                "has_snapshot": True,
            },
        }

        with patch.object(dragon_app, "YOUTUBE_FRESHNESS_SERVICE", mock_service):
            response = client.get("/pockettube")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("Section", body)
        self.assertIn("Limit", body)
        self.assertIn("Open on YouTube", body)
        self.assertIn("/video/yt-v1", body)
        self.assertIn("Snapshot status", body)
        mock_service.build_page_context_for_filter.assert_called_once_with(
            "all",
            display_limit="50",
        )
        mock_service.request_sync.assert_not_called()

    def test_pockettube_groups_route_still_works(self):
        dragon_app.app.config["TESTING"] = True
        client = dragon_app.app.test_client()
        mock_playlist_service = Mock()
        mock_playlist_service.render_pockettube_groups.return_value = "ok"

        with patch.object(dragon_app, "YOUTUBE_PLAYLIST_SERVICE", mock_playlist_service):
            response = client.get("/pockettube/groups")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_data(as_text=True), "ok")
        mock_playlist_service.render_pockettube_groups.assert_called_once()

    def test_video_detail_route_falls_back_to_snapshot_without_remote_lookup(self):
        dragon_app.app.config["TESTING"] = True
        client = dragon_app.app.test_client()
        with tempfile.TemporaryDirectory() as temp_dir:
            service, _state = self._build_service(temp_dir)
            save_json_file(service.snapshot_path, self._build_related_snapshot_payload())
            collect_mock = Mock(return_value=[])

            with patch.object(dragon_app, "YOUTUBE_FRESHNESS_SERVICE", service), \
                 patch.object(dragon_app, "collect_all_youtube_entries", collect_mock), \
                 patch.object(dragon_app, "_youtube_perf_log") as perf_log_mock, \
                 patch.object(dragon_app, "get_youtube_duration", Mock()) as mocked_get_duration:
                response = client.get("/video/yt-current")

            self.assertEqual(response.status_code, 200)
            body = response.get_data(as_text=True)
            self.assertNotIn("No related entries available yet.", body)
            self.assertIn("Current News Story", body)
            self.assertIn("Breaking News Update", body)
            self.assertIn("/video/yt-news-2", body)
            self.assertNotIn("could not find the requested entry", body)
            fast_path_log_calls = [
                call for call in perf_log_mock.call_args_list
                if call.args and call.args[0] == "pockettube_video_detail_fast_path"
            ]
            self.assertTrue(fast_path_log_calls)
            self.assertGreater(int(fast_path_log_calls[0].kwargs.get("playlist_entries_count", 0)), 1)
            context_log_calls = [
                call for call in perf_log_mock.call_args_list
                if call.args and call.args[0] == "video_detail_context"
            ]
            self.assertTrue(context_log_calls)
            self.assertEqual(context_log_calls[0].kwargs.get("source"), "pockettube_snapshot_fast_path")
            collect_mock.assert_not_called()
            mocked_get_duration.assert_not_called()

    def test_video_detail_route_snapshot_miss_still_uses_existing_collect_path(self):
        dragon_app.app.config["TESTING"] = True
        client = dragon_app.app.test_client()
        with tempfile.TemporaryDirectory() as temp_dir:
            service, _state = self._build_service(temp_dir)
            save_json_file(service.snapshot_path, {
                "version": 1,
                "generated_at": FIXED_NOW.isoformat(),
                "synced_at": FIXED_NOW.isoformat(),
                "groups": {},
                "channels": {},
                "errors": [],
            })
            collect_mock = Mock(return_value=[{
                "entry_id": "yt-watch-1",
                "video_id": "watch-1",
                "watch_key": "watch-1",
                "title": "Watch Later Sample",
                "name": "Watch Later Sample",
                "channel_name": "Sample Channel",
                "channel_id": "c-watch-1",
                "published_at": self._timestamp(1),
                "published_display": "2026-06-02 11:00",
                "url": "https://www.youtube.com/watch?v=watch-1",
                "detail_url": "/video/yt-watch-1",
                "thumbnail": "https://img.youtube.com/vi/watch-1/hqdefault.jpg",
                "thumbnail_url": "https://img.youtube.com/vi/watch-1/hqdefault.jpg",
                "image_url": "https://img.youtube.com/vi/watch-1/hqdefault.jpg",
                "duration": "3:21",
                "duration_seconds": 201,
                "playlist_name": "Watch Later",
                "playlist_url": "/watch-later",
                "section": "Watch Later",
                "source_type": "youtube",
                "entry_type": "youtube",
            }])

            with patch.object(dragon_app, "YOUTUBE_FRESHNESS_SERVICE", service), \
                 patch.object(dragon_app, "collect_all_youtube_entries", collect_mock), \
                 patch.object(service, "build_snapshot_video_detail_context", wraps=service.build_snapshot_video_detail_context) as snapshot_mock:
                response = client.get("/video/yt-watch-1")

            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.get_data(as_text=True).count("Watch Later Sample") > 0, True)
            snapshot_mock.assert_called_once_with("yt-watch-1")
            collect_mock.assert_called_once()

    def test_refresh_local_snapshot_from_github_replaces_local_files_when_valid(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            requests_module = _FakeRequestsModule({
                "https://example.com/youtube_latest_snapshot.json": _FakeResponse(text="""{
                    "version": 1,
                    "generated_at": "2026-06-02T12:00:00+00:00",
                    "synced_at": "2026-06-02T12:00:00+00:00",
                    "groups": {
                        "science": {
                            "group_name": "Science",
                            "group_key": "science",
                            "section_name": "Science",
                            "section_key": "science",
                            "source_name": "PocketTube",
                            "imported_at": "2026-06-02T12:00:00+00:00",
                            "channel_count": 1,
                            "latest_video_count": 1,
                            "latest_video": {},
                            "channels": [
                                {
                                    "channel_id": "c1",
                                    "channel_title": "Science One",
                                    "group_names": ["Science"],
                                    "group_keys": ["science"],
                                    "latest_video": {
                                        "entry_id": "yt-v1",
                                        "video_id": "v1",
                                        "watch_key": "v1",
                                        "title": "Fresh Cache Video",
                                        "channel_id": "c1",
                                        "channel_name": "Science One",
                                        "published_at": "2026-06-02T11:00:00+00:00",
                                        "url": "https://www.youtube.com/watch?v=v1"
                                    }
                                }
                            ]
                        }
                    },
                    "channels": {},
                    "errors": []
                }"""),
                "https://example.com/youtube_latest_sync_status.json": _FakeResponse(text="""{
                    "status": "completed",
                    "requested_at": "2026-06-02T11:59:00+00:00",
                    "started_at": "2026-06-02T11:59:10+00:00",
                    "completed_at": "2026-06-02T12:00:00+00:00",
                    "last_error": "",
                    "scope": "",
                    "run_id": "123",
                    "run_url": "https://github.com/example/actions/runs/123",
                    "source": "github_actions",
                    "updated_at": "2026-06-02T12:00:00+00:00"
                }"""),
            })
            service, _state = self._build_service(temp_dir, requests_module=requests_module)
            save_json_file(service.snapshot_path, {"version": 1, "generated_at": "", "synced_at": "", "groups": {}, "channels": {}, "errors": []})
            save_json_file(service.sync_status_path, service.empty_sync_status())

            result = service.refresh_local_snapshot_from_github()

            self.assertTrue(result["ok"])
            saved_snapshot = load_json_file(service.snapshot_path, {})
            saved_sync_status = load_json_file(service.sync_status_path, {})
            self.assertIn("science", saved_snapshot.get("groups", {}))
            self.assertEqual(saved_snapshot["groups"]["science"]["channels"][0]["latest_video"]["video_id"], "v1")
            self.assertEqual(saved_sync_status.get("status"), "completed")
            self.assertEqual(saved_sync_status.get("run_id"), "123")

    def test_refresh_local_snapshot_from_github_does_not_replace_local_files_when_invalid(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            requests_module = _FakeRequestsModule({
                "https://example.com/youtube_latest_snapshot.json": _FakeResponse(text="[]"),
                "https://example.com/youtube_latest_sync_status.json": _FakeResponse(text="""{
                    "status": "completed",
                    "requested_at": "",
                    "started_at": "",
                    "completed_at": "",
                    "last_error": "",
                    "scope": "",
                    "run_id": "",
                    "run_url": "",
                    "source": "github_actions",
                    "updated_at": ""
                }"""),
            })
            service, _state = self._build_service(temp_dir, requests_module=requests_module)
            original_snapshot = {
                "version": 1,
                "generated_at": "2026-06-01T12:00:00+00:00",
                "synced_at": "2026-06-01T12:00:00+00:00",
                "groups": {},
                "channels": {},
                "errors": [],
            }
            original_sync_status = {
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
            save_json_file(service.snapshot_path, original_snapshot)
            save_json_file(service.sync_status_path, original_sync_status)

            with self.assertRaises(RuntimeError):
                service.refresh_local_snapshot_from_github()

            self.assertEqual(load_json_file(service.snapshot_path, {}), original_snapshot)
            self.assertEqual(load_json_file(service.sync_status_path, {}), original_sync_status)

    def test_freshness_route_redirects_to_main_pockettube(self):
        dragon_app.app.config["TESTING"] = True
        client = dragon_app.app.test_client()

        response = client.get("/pockettube/freshness?sync_requested=1")

        self.assertEqual(response.status_code, 302)
        self.assertIn("/pockettube", response.location)
        self.assertIn("sync_requested=1", response.location)

    def test_post_sync_route_returns_quickly(self):
        dragon_app.app.config["TESTING"] = True
        client = dragon_app.app.test_client()
        mock_service = Mock()
        mock_service.request_sync.return_value = ({"ok": True, "trigger": {"status": "started"}, "sync_status": {"status": "queued"}}, 200)

        with patch.object(dragon_app, "YOUTUBE_FRESHNESS_SERVICE", mock_service):
            response = client.post("/pockettube/freshness/sync", data={"scope": ""})

        self.assertEqual(response.status_code, 302)
        self.assertIn("/pockettube", response.location)
        mock_service.request_sync.assert_called_once()

    def test_refresh_snapshot_route_redirects_with_success_message(self):
        dragon_app.app.config["TESTING"] = True
        client = dragon_app.app.test_client()
        mock_service = Mock()
        mock_service.refresh_local_snapshot_from_github.return_value = {
            "ok": True,
            "status": "updated",
            "group_count": 3,
            "channel_count": 12,
        }

        with patch.object(dragon_app, "YOUTUBE_FRESHNESS_SERVICE", mock_service):
            response = client.post("/pockettube/refresh-snapshot", follow_redirects=False)

        self.assertEqual(response.status_code, 302)
        self.assertIn("/pockettube", response.location)
        mock_service.refresh_local_snapshot_from_github.assert_called_once_with()

    def test_refresh_snapshot_route_redirects_safely_on_failure(self):
        dragon_app.app.config["TESTING"] = True
        client = dragon_app.app.test_client()
        mock_service = Mock()
        mock_service.refresh_local_snapshot_from_github.side_effect = RuntimeError("bad payload")

        with patch.object(dragon_app, "YOUTUBE_FRESHNESS_SERVICE", mock_service):
            response = client.post("/pockettube/refresh-snapshot", follow_redirects=False)

        self.assertEqual(response.status_code, 302)
        self.assertIn("/pockettube", response.location)
        mock_service.refresh_local_snapshot_from_github.assert_called_once_with()

    def test_refresh_pockettube_section_latest_uploads_keeps_more_than_first_50(self):
        videos = [
            {
                "video_id": f"v{index:03d}",
                "title": f"Video {index:03d}",
                "channel_id": "c1",
                "channel_name": "Channel One",
                "published_at": (FIXED_NOW - timedelta(minutes=index)).isoformat(),
                "url": f"https://www.youtube.com/watch?v=v{index:03d}",
                "thumb": f"https://img.youtube.com/vi/v{index:03d}/hqdefault.jpg",
            }
            for index in range(120)
        ]
        pockettube_context = {
            "group_name": "News",
            "group_key": "news",
            "channels": [{"channel_id": "c1", "channel_name": "Channel One"}],
        }

        with patch.object(dragon_app, "_pockettube_section_membership_context", return_value=pockettube_context), \
             patch.object(dragon_app, "fetch_youtube_channel_group_feed_videos", return_value=videos), \
             patch.object(dragon_app, "clear_persisted_youtube_section_feed_cache"), \
             patch.object(dragon_app, "_youtube_trace"), \
             patch.object(dragon_app, "build_youtube_channel_video_summary", side_effect=lambda video: dict(video)):
            result = dragon_app.refresh_pockettube_section_latest_uploads("News", admin_data={})

        self.assertEqual(result["videos_stored"], 120)
        self.assertEqual(len(result["latest_items"]), 120)
        self.assertEqual(result["latest_items"][0]["video_id"], "v000")
        self.assertEqual(result["latest_items"][-1]["video_id"], "v119")

    def test_refresh_pockettube_section_latest_uploads_handles_channel_error_without_killing_group(self):
        pockettube_context = {
            "group_name": "Tech",
            "group_key": "tech",
            "channels": [
                {"channel_id": "c1", "channel_name": "Channel One"},
                {"channel_id": "c2", "channel_name": "Channel Two"},
            ],
        }

        def fetch_side_effect(channel_id, channel_name="", limit=4, uploads_playlist_id="", **_kwargs):
            if channel_id == "c2":
                raise RuntimeError("boom")
            return [
                {
                    "video_id": "ok-1",
                    "title": "Okay One",
                    "channel_id": "c1",
                    "channel_name": "Channel One",
                    "published_at": FIXED_NOW.isoformat(),
                    "url": "https://www.youtube.com/watch?v=ok-1",
                    "thumb": "https://img.youtube.com/vi/ok-1/hqdefault.jpg",
                }
            ]

        with patch.object(dragon_app, "_pockettube_section_membership_context", return_value=pockettube_context), \
             patch.object(dragon_app, "_resolve_youtube_channel_upload_playlist_ids", return_value={
                 "resolved": {"c1": "UU1", "c2": "UU2"},
                 "diagnostics": [],
                 "missing": [],
                 "channel_count": 2,
                 "resolved_count": 2,
                 "missing_count": 0,
             }), \
             patch.object(dragon_app, "fetch_youtube_channel_group_feed_videos", side_effect=fetch_side_effect), \
             patch.object(dragon_app, "clear_persisted_youtube_section_feed_cache"), \
             patch.object(dragon_app, "_youtube_trace"), \
             patch.object(dragon_app, "build_youtube_channel_video_summary", side_effect=lambda video: dict(video)):
            result = dragon_app.refresh_pockettube_section_latest_uploads("Tech", admin_data={})

        self.assertEqual(result["videos_stored"], 1)
        self.assertEqual(len(result["latest_items"]), 1)
        self.assertEqual(result["latest_items"][0]["video_id"], "ok-1")
        self.assertEqual(result["channels_scanned"], 2)
        self.assertEqual(len(result["errors"]), 1)
        self.assertIn("c2", result["errors"][0])

    def test_batch_upload_playlist_resolution_resolves_and_persists_cached_ids(self):
        responses = {
            ("UC1", "UC2"): {
                "items": [
                    {
                        "id": "UC1",
                        "snippet": {"title": "Channel One"},
                        "contentDetails": {"relatedPlaylists": {"uploads": "UU1"}},
                    },
                    {
                        "id": "UC2",
                        "snippet": {"title": "Channel Two"},
                        "contentDetails": {"relatedPlaylists": {"uploads": "UU2"}},
                    },
                ]
            }
        }
        cache_data = {}

        def fake_get(url, params=None, timeout=15):
            if "channels" not in url:
                raise AssertionError(f"unexpected url: {url}")
            self.assertEqual(params.get("part"), "contentDetails,snippet")
            self.assertEqual(params.get("key"), "test-key")
            channel_ids = tuple(sorted((params.get("id") or "").split(",")))
            payload = responses.get(channel_ids)
            if payload is None:
                raise AssertionError(f"unexpected channel batch: {channel_ids}")
            return Mock(status_code=200, json=Mock(return_value=payload), text=json.dumps(payload))

        with patch.object(dragon_app, "YOUTUBE_API_KEY", "test-key"), \
             patch.object(dragon_app.requests, "get", side_effect=fake_get):
            result = dragon_app._resolve_youtube_channel_upload_playlist_ids(["UC1", "UC2"], cache_data=cache_data, force_refresh=True)

        self.assertEqual(result["resolved"], {"UC1": "UU1", "UC2": "UU2"})
        self.assertEqual(result["resolved_count"], 2)
        self.assertEqual(result["missing_count"], 0)
        self.assertEqual(cache_data["youtube_channel_latest_uploads"]["uc1"]["data"]["uploads_playlist_id"], "UU1")
        self.assertEqual(cache_data["youtube_channel_latest_uploads"]["uc2"]["data"]["uploads_playlist_id"], "UU2")

    def test_batch_upload_playlist_resolution_reuses_cached_ids_without_api(self):
        with dragon_app.RUNTIME_CACHE_LOCK:
            dragon_app.YOUTUBE_RUNTIME.latest_uploads_index.pop(dragon_app._youtube_channel_latest_video_cache_key("UC1"), None)
        with patch.object(dragon_app, "get_persisted_youtube_channel_latest_entry", return_value=(
            {
                "channel_id": "UC1",
                "uploads_playlist_id": "UUcached1",
                "latest_video": {},
            },
            False,
        )), patch.object(dragon_app.requests, "get") as mock_get:
            result = dragon_app._resolve_youtube_channel_upload_playlist_ids(["UC1"], cache_data={})

        self.assertEqual(result["resolved"], {"UC1": "UUcached1"})
        mock_get.assert_not_called()

    def test_batch_upload_playlist_resolution_records_missing_playlist(self):
        def fake_get(url, params=None, timeout=15):
            payload = {
                "items": [
                    {
                        "id": "UC1",
                        "snippet": {"title": "Channel One"},
                        "contentDetails": {"relatedPlaylists": {}},
                    }
                ]
            }
            return Mock(status_code=200, json=Mock(return_value=payload), text=json.dumps(payload))

        with patch.object(dragon_app, "YOUTUBE_API_KEY", "test-key"), \
             patch.object(dragon_app.requests, "get", side_effect=fake_get):
            result = dragon_app._resolve_youtube_channel_upload_playlist_ids(["UC1"], cache_data={}, force_refresh=True)

        self.assertEqual(result["resolved_count"], 0)
        self.assertEqual(result["missing_count"], 1)
        self.assertEqual(result["diagnostics"][0]["error"], "no_upload_playlist")

    def test_batch_upload_playlist_resolution_survives_batch_failure(self):
        call_count = {"value": 0}

        def fake_get(url, params=None, timeout=15):
            call_count["value"] += 1
            if call_count["value"] == 1:
                raise RuntimeError("boom")
            payload = {
                "items": [
                    {
                        "id": "UC51",
                        "snippet": {"title": "Channel Fifty One"},
                        "contentDetails": {"relatedPlaylists": {"uploads": "UU51"}},
                    }
                ]
            }
            return Mock(status_code=200, json=Mock(return_value=payload), text=json.dumps(payload))

        channel_ids = [f"UC{i}" for i in range(1, 52)]
        with patch.object(dragon_app, "YOUTUBE_API_KEY", "test-key"), \
             patch.object(dragon_app.requests, "get", side_effect=fake_get):
            result = dragon_app._resolve_youtube_channel_upload_playlist_ids(channel_ids, cache_data={}, force_refresh=True)

        self.assertEqual(result["resolved"].get("UC51"), "UU51")
        self.assertGreaterEqual(result["missing_count"], 50)
        self.assertGreaterEqual(len(result["diagnostics"]), 50)

    def test_refresh_pockettube_section_latest_uploads_uses_resolved_upload_playlists(self):
        pockettube_context = {
            "group_name": "News",
            "group_key": "news",
            "channels": [
                {"channel_id": "UC1", "channel_name": "Channel One"},
            ],
        }

        def fake_get(url, params=None, timeout=15):
            if "channels" in url:
                payload = {
                    "items": [
                        {
                            "id": "UC1",
                            "snippet": {"title": "Channel One"},
                            "contentDetails": {"relatedPlaylists": {"uploads": "UU1"}},
                        }
                    ]
                }
                return Mock(status_code=200, json=Mock(return_value=payload), text=json.dumps(payload))
            if "playlistItems" in url:
                payload = {
                    "items": [
                        {
                            "id": "PLI1",
                            "snippet": {
                                "title": "News Item 1",
                                "publishedAt": FIXED_NOW.isoformat(),
                                "resourceId": {"videoId": "v1"},
                                "videoOwnerChannelTitle": "Channel One",
                                "videoOwnerChannelId": "UC1",
                                "channelTitle": "Channel One",
                                "channelId": "UC1",
                            },
                        }
                    ]
                }
                return Mock(status_code=200, json=Mock(return_value=payload), text=json.dumps(payload))
            raise AssertionError(f"unexpected url: {url}")

        with patch.object(dragon_app, "_pockettube_section_membership_context", return_value=pockettube_context), \
             patch.object(dragon_app, "_resolve_youtube_channel_upload_playlist_ids", return_value={
                 "resolved": {"UC1": "UU1"},
                 "diagnostics": [],
                 "missing": [],
                 "channel_count": 1,
                 "resolved_count": 1,
                 "missing_count": 0,
             }), \
             patch.object(dragon_app, "YOUTUBE_API_KEY", "test-key"), \
             patch.object(dragon_app.requests, "get", side_effect=fake_get), \
             patch.object(dragon_app, "clear_persisted_youtube_section_feed_cache"), \
             patch.object(dragon_app, "_youtube_trace"), \
             patch.object(dragon_app, "get_youtube_duration", return_value={"display": "0:00"}), \
             patch.object(dragon_app, "build_youtube_channel_video_summary", side_effect=lambda video: dict(video)):
            result = dragon_app.refresh_pockettube_section_latest_uploads("News", admin_data={})

        self.assertEqual(result["channels_with_upload_playlist"], 1)
        self.assertEqual(result["channels_missing_upload_playlist"], 0)
        self.assertEqual(result["upload_playlist_ids"], ["UU1"])
        self.assertEqual(result["videos_stored"], 1)
        self.assertEqual(result["latest_items"][0]["video_id"], "v1")

    def test_coverage_report_changes_reason_when_upload_playlists_exist(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            service, _state = self._build_service(temp_dir)
            save_json_file(service.snapshot_path, {
                "version": 1,
                "generated_at": FIXED_NOW.isoformat(),
                "synced_at": FIXED_NOW.isoformat(),
                "group_video_limit": 200,
                "all_feed_video_limit": 200,
                "groups": {
                    "news": {
                        "group_name": "News",
                        "group_key": "news",
                        "section_name": "News",
                        "section_key": "news",
                        "source_name": "PocketTube",
                        "imported_at": FIXED_NOW.isoformat(),
                        "channel_count": 1,
                        "latest_video_count": 0,
                        "latest_video": {},
                        "channels": [
                            {
                                "channel_id": "UC1",
                                "channel_title": "Channel One",
                                "group_names": ["News"],
                                "latest_video": {},
                                "latest_video_id": "",
                                "published_at": "",
                                "published_display": "",
                                "thumbnail": "",
                                "url": "",
                                "reason_tags": [],
                            }
                        ],
                        "videos": [],
                        "diagnostics": {
                            "group_key": "news",
                            "group_name": "News",
                            "channels_scanned": 1,
                            "channels_with_upload_playlist": 1,
                            "channels_missing_upload_playlist": 0,
                            "videos_collected": 0,
                            "videos_stored": 0,
                            "upload_playlist_ids": ["UU1"],
                            "errors": [],
                            "generated_at": FIXED_NOW.isoformat(),
                            "synced_at": FIXED_NOW.isoformat(),
                        },
                    }
                },
                "channels": {},
                "errors": [],
            })
            cache_data = {
                "youtube_channel_latest_uploads": {
                    "uc1": {
                        "updated_at": FIXED_NOW.isoformat(),
                        "data": {
                            "channel_id": "UC1",
                            "uploads_playlist_id": "UU1",
                            "latest_video": {},
                        },
                    }
                }
            }

            report = service.build_pockettube_coverage_report(cache_data=cache_data)

            self.assertEqual(report["groups"][0]["upload_playlist_ids"], ["UU1"])
            self.assertNotEqual(report["groups"][0]["reason"], "no_upload_playlist")

    def test_github_snapshot_callback_updates_status_without_blocking(self):
        dragon_app.app.config["TESTING"] = True
        client = dragon_app.app.test_client()
        mock_service = Mock()
        mock_service.ingest_github_snapshot_update.return_value = {"ok": True, "status": "completed"}

        with patch.object(dragon_app, "YOUTUBE_FRESHNESS_SERVICE", mock_service):
            response = client.post(
                "/pockettube/freshness/github-snapshot-updated",
                json={"status": "completed", "run_id": "123"},
                headers={"X-Dragon-GitHub-Secret": "test"},
            )

            self.assertEqual(response.status_code, 403)
            mock_service.ingest_github_snapshot_update.assert_not_called()

    def test_empty_sync_snapshot_writes_valid_empty_snapshot(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            service, _state = self._build_service(temp_dir, imported_sections=[], latest_cache={})

            snapshot = service.sync_snapshot(scope="")

            self.assertEqual(snapshot["version"], 2)
            self.assertTrue(snapshot["generated_at"])
            self.assertTrue(snapshot["synced_at"])
            self.assertEqual(snapshot["groups"], {})
            self.assertEqual(snapshot["channels"], {})
            self.assertTrue(service.snapshot_path.exists())
            self.assertEqual(load_json_file(service.snapshot_path, {}), snapshot)

    def test_sync_snapshot_writes_group_videos_and_diagnostics(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            imported_sections = [
                {
                    "section_name": "News",
                    "section_key": "news",
                    "group_name": "News",
                    "group_key": "news",
                    "channels": [
                        {"channel_id": "UC1", "channel_name": "Channel One"},
                    ],
                }
            ]
            latest_result = {
                "group_name": "News",
                "section_name": "News",
                "group_key": "news",
                "channels_scanned": 1,
                "channels_with_upload_playlist": 1,
                "channels_missing_upload_playlist": 0,
                "videos_collected": 1,
                "videos_stored": 1,
                "upload_playlist_ids": ["UU1"],
                "latest_videos_found": 1,
                "latest_items": [
                    self._group_video("v1", channel_id="UC1", channel_name="Channel One", hours_ago=1)
                ],
                "diagnostics": {
                    "group_key": "news",
                    "group_name": "News",
                    "channels_scanned": 1,
                    "channels_with_upload_playlist": 1,
                    "channels_missing_upload_playlist": 0,
                    "videos_collected": 1,
                    "videos_stored": 1,
                    "upload_playlist_ids": ["UU1"],
                    "errors": [],
                    "generated_at": FIXED_NOW.isoformat(),
                    "synced_at": FIXED_NOW.isoformat(),
                },
                "errors": [],
                "generated_at": FIXED_NOW.isoformat(),
                "synced_at": FIXED_NOW.isoformat(),
            }
            service, _state = self._build_service(
                temp_dir,
                imported_sections=imported_sections,
                refresh_mock=Mock(return_value=latest_result),
            )

            saved_payloads = {}

            def capture_save(path, payload):
                key = str(path)
                if key.endswith("youtube_latest_snapshot.json"):
                    saved_payloads["snapshot"] = payload
                elif key.endswith("youtube_latest_sync_status.json"):
                    saved_payloads["status"] = payload

            service.save_json_file = Mock(side_effect=capture_save)
            snapshot = service.sync_snapshot(scope="news")

            news = snapshot["groups"]["news"]
            self.assertEqual(snapshot["version"], 2)
            self.assertEqual(news["videos"][0]["video_id"], "v1")
            self.assertEqual(news["diagnostics"]["videos_stored"], 1)
            self.assertEqual(news["latest_video"]["video_id"], "v1")
            self.assertEqual(news["channels"][0]["latest_video"]["video_id"], "v1")
            self.assertIn("snapshot", saved_payloads)
            self.assertIn("videos", saved_payloads["snapshot"]["groups"]["news"])
            self.assertIn("diagnostics", saved_payloads["snapshot"]["groups"]["news"])
            self.assertEqual(saved_payloads["snapshot"]["groups"]["news"]["videos"][0]["video_id"], "v1")

    def test_sync_snapshot_records_warnings_for_empty_group_videos(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            imported_sections = [
                {
                    "section_name": "News",
                    "section_key": "news",
                    "group_name": "News",
                    "group_key": "news",
                    "channels": [
                        {"channel_id": "UC1", "channel_name": "Channel One"},
                    ],
                }
            ]
            latest_result = {
                "group_name": "News",
                "section_name": "News",
                "group_key": "news",
                "channels_scanned": 1,
                "channels_with_upload_playlist": 1,
                "channels_missing_upload_playlist": 0,
                "videos_collected": 0,
                "videos_stored": 0,
                "upload_playlist_ids": ["UU1"],
                "latest_videos_found": 0,
                "latest_items": [],
                "diagnostics": {
                    "group_key": "news",
                    "group_name": "News",
                    "channels_scanned": 1,
                    "channels_with_upload_playlist": 1,
                    "channels_missing_upload_playlist": 0,
                    "videos_collected": 0,
                    "videos_stored": 0,
                    "upload_playlist_ids": ["UU1"],
                    "errors": [],
                    "generated_at": FIXED_NOW.isoformat(),
                    "synced_at": FIXED_NOW.isoformat(),
                },
                "errors": [],
                "generated_at": FIXED_NOW.isoformat(),
                "synced_at": FIXED_NOW.isoformat(),
            }
            service, _state = self._build_service(
                temp_dir,
                imported_sections=imported_sections,
                refresh_mock=Mock(return_value=latest_result),
            )

            saved_payloads = {}

            def capture_save(path, payload):
                key = str(path)
                if key.endswith("youtube_latest_snapshot.json"):
                    saved_payloads["snapshot"] = payload
                elif key.endswith("youtube_latest_sync_status.json"):
                    saved_payloads["status"] = payload

            service.save_json_file = Mock(side_effect=capture_save)
            snapshot = service.sync_snapshot(scope="news")

            self.assertEqual(snapshot["groups"]["news"]["videos"], [])
            self.assertTrue(snapshot["warnings"])
            self.assertTrue(saved_payloads["status"]["warnings"])
            self.assertIn("news", saved_payloads["status"]["warnings"][0].lower())

    def test_sync_snapshot_finalizes_before_save(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            registry_payload = {
                "latest": {
                    "source_name": "PocketTube",
                    "imported_at": FIXED_NOW.isoformat(),
                    "fingerprint": "registry-save",
                    "section_count": 1,
                    "group_count": 1,
                    "channel_count": 1,
                    "sections": [
                        {
                            "section_name": "Science",
                            "section_key": "science",
                            "group_name": "Science",
                            "group_key": "science",
                            "tier": "best",
                            "channel_count": 1,
                            "channels": [
                                {
                                    "channel_name": "UCTDc1RLIHHNjN5WlHoZwXQg",
                                    "channel_id": "",
                                    "channel_key": "uctdc1rlihhnjn5wlhozwxqg",
                                    "section_name": "Science",
                                    "section_key": "science",
                                    "group_name": "Science",
                                    "group_key": "science",
                                    "tier": "best",
                                }
                            ],
                        }
                    ],
                    "channels": [],
                }
            }
            service, _state = self._build_service(
                temp_dir,
                imported_sections=[],
                registry_payload=registry_payload,
                refresh_mock=Mock(),
            )
            saved_payloads = {}

            def capture_save(path, payload):
                key = str(path)
                if key.endswith("youtube_latest_snapshot.json"):
                    saved_payloads["snapshot"] = payload
                elif key.endswith("youtube_latest_sync_status.json"):
                    saved_payloads["status"] = payload

            service.save_json_file = Mock(side_effect=capture_save)

            with patch.object(service, "finalize_snapshot", wraps=service.finalize_snapshot) as finalize_mock:
                snapshot = service.sync_snapshot()

            self.assertGreaterEqual(finalize_mock.call_count, 1)
            self.assertIn("snapshot", saved_payloads)
            self.assertGreater(len(saved_payloads["snapshot"].get("channels", {})), 0)
            self.assertGreater(len(snapshot.get("channels", {})), 0)


if __name__ == "__main__":
    unittest.main()
