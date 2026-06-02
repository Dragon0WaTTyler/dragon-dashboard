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
            registry_path=registry_path,
            app_logger=Mock(),
        ), state

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

    def test_get_route_uses_snapshot_only(self):
        dragon_app.app.config["TESTING"] = True
        client = dragon_app.app.test_client()
        mock_service = Mock()
        mock_service.build_page_context.return_value = {
            "title": "PocketTube Freshness",
            "snapshot": {"version": 1, "groups": {}, "channels": {}, "errors": []},
            "sync_status": {"status": "idle"},
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
        mock_service.request_sync.assert_not_called()

    def test_post_sync_route_returns_quickly(self):
        dragon_app.app.config["TESTING"] = True
        client = dragon_app.app.test_client()
        mock_service = Mock()
        mock_service.request_sync.return_value = ({"ok": True, "trigger": {"status": "started"}, "sync_status": {"status": "queued"}}, 200)

        with patch.object(dragon_app, "YOUTUBE_FRESHNESS_SERVICE", mock_service):
            response = client.post("/pockettube/freshness/sync", data={"scope": ""})

        self.assertEqual(response.status_code, 302)
        self.assertIn("/pockettube/freshness", response.location)
        mock_service.request_sync.assert_called_once()

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

            self.assertEqual(snapshot["version"], 1)
            self.assertTrue(snapshot["generated_at"])
            self.assertTrue(snapshot["synced_at"])
            self.assertEqual(snapshot["groups"], {})
            self.assertEqual(snapshot["channels"], {})
            self.assertTrue(service.snapshot_path.exists())
            self.assertEqual(load_json_file(service.snapshot_path, {}), snapshot)

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
