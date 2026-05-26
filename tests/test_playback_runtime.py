import tempfile
import unittest
from pathlib import Path

from domains.magnets.playback import prepare_playback_runtime, select_playback_candidates
from domains.magnets.playback.capability_matrix import (
    estimate_bandwidth_class,
    estimate_startup_risk,
    evaluate_browser_capability,
    evaluate_capability_matrix,
    evaluate_mobile_capability,
)
from domains.magnets.playback.readiness_snapshot import build_playback_readiness_snapshot
from domains.magnets.playback.runtime_diagnostics import build_runtime_diagnostics
from domains.magnets.playback.runtime_policy import (
    browser_hard_fail_codec,
    browser_safe_size_limit,
    is_browser_rejected_source_type,
    mobile_safe_size_limit,
    startup_confidence_threshold,
)
from domains.magnets.services.session_store import StreamSessionStore
from domains.magnets.sessions import StreamSession


def make_source(
    *,
    title,
    magnet,
    resolution="1080p",
    codec="x264",
    source_type="WebDL",
    size_gb=4.2,
    seeders=120,
    release_group="ntb",
    likely_streamable=True,
    estimated_quality_score=78,
    hdr=False,
    dolby_vision=False,
):
    return {
        "title": title,
        "magnet": magnet,
        "resolution": resolution,
        "codec": codec,
        "source_type": source_type,
        "size_gb": size_gb,
        "seeders": seeders,
        "release_group": release_group,
        "likely_streamable": likely_streamable,
        "estimated_quality_score": estimated_quality_score,
        "provider": "torrentio",
        "source": "torrentio",
        "hdr": hdr,
        "dolby_vision": dolby_vision,
    }


class PlaybackRuntimeTests(unittest.TestCase):
    def test_select_playback_candidates_prefers_browser_safe_webdl(self):
        candidates = [
            make_source(
                title="Film.2026.1080p.WEB-DL.x264-NTb",
                magnet="magnet:?xt=urn:btih:1234567890ABCDEF1234567890ABCDEF12345678",
                release_group="ntb",
                size_gb=4.1,
            ),
            make_source(
                title="Film.2026.2160p.REMUX.x265-FraMeSToR",
                magnet="magnet:?xt=urn:btih:ABCDEF1234567890ABCDEF1234567890ABCDEF12",
                resolution="2160p",
                codec="x265",
                source_type="REMUX",
                size_gb=58.0,
                seeders=35,
                release_group="framestor",
                estimated_quality_score=92,
                hdr=True,
            ),
        ]

        result = select_playback_candidates(candidates, movie={"title": "Film"})
        selected = result["selected_source"]

        self.assertTrue(selected["auto_selected"])
        self.assertEqual(selected["source_type"], "WebDL")
        self.assertTrue(selected["browser_playable_candidate"])
        self.assertEqual(selected["runtime_recommended"], "browser_runtime")

    def test_prepare_playback_runtime_returns_browser_runtime_for_safe_source(self):
        source = make_source(
            title="Film.2026.1080p.WEB-DL.x264-NTb",
            magnet="magnet:?xt=urn:btih:1234567890ABCDEF1234567890ABCDEF12345678",
        )

        result = prepare_playback_runtime(
            movie={"title": "Film"},
            selected_source=source,
            sources=[source],
            player_sources=[{"key": "vidsrc", "label": "VidSrc", "url": "https://example.com/embed"}],
            fallback_urls=["https://example.com/embed"],
        )

        self.assertEqual(result["playback_runtime"], "browser_runtime")
        self.assertEqual(result["playback_readiness"], "browser_ready")
        self.assertIn(result["startup_confidence"], {"high", "medium"})
        self.assertEqual(result["browser_runtime"]["launch_strategy"], "browser_embed")
        self.assertIn("runtime_diagnostics", result)
        self.assertGreaterEqual(result["readiness_meter"], 70)
        self.assertIn(result["execution_state"], {"runtime_completed", "runtime_active"})
        self.assertIn("stability_score", result["execution_metrics"])
        self.assertIn("estimated_startup_ms", result["execution_timeline"])
        self.assertTrue(result["recovery_path"])

    def test_prepare_playback_runtime_falls_back_to_external_for_remux(self):
        source = make_source(
            title="Film.2026.2160p.REMUX.x265-FraMeSToR",
            magnet="magnet:?xt=urn:btih:ABCDEF1234567890ABCDEF1234567890ABCDEF12",
            resolution="2160p",
            codec="x265",
            source_type="REMUX",
            size_gb=52.0,
            seeders=40,
            release_group="framestor",
            estimated_quality_score=95,
            hdr=True,
        )

        result = prepare_playback_runtime(
            movie={"title": "Film"},
            selected_source=source,
            sources=[source],
            player_sources=[],
            fallback_urls=[],
        )

        self.assertEqual(result["playback_runtime"], "external_runtime")
        self.assertEqual(result["runtime_profile"], "external_player_only")
        self.assertIn("browser_blocked", result["runtime_warnings"])
        self.assertTrue(result["runtime_diagnostics"]["fallback_reason"])

    def test_malformed_magnet_degrades_without_crashing(self):
        source = make_source(
            title="Film.2026.1080p.WEB-DL.x264-NTb",
            magnet="not-a-magnet",
            likely_streamable=False,
        )

        result = prepare_playback_runtime(movie={"title": "Film"}, selected_source=source, sources=[source])

        self.assertEqual(result["playback_runtime"], "external_runtime")
        self.assertEqual(result["startup_confidence"], "low")
        self.assertIn("browser_blocked", result["runtime_warnings"])

    def test_capability_matrix_marks_browser_safe_source(self):
        capability = evaluate_capability_matrix(
            make_source(
                title="Film.2026.1080p.WEB-DL.x264-NTb.mkv",
                magnet="magnet:?xt=urn:btih:1234567890ABCDEF1234567890ABCDEF12345678",
            )
        )

        self.assertTrue(capability["browser_friendly"])
        self.assertTrue(capability["mobile_friendly"])
        self.assertEqual(capability["bandwidth_class"], "low")
        self.assertEqual(capability["startup_risk"], "low")

    def test_browser_rejection_for_hevc_codec(self):
        source = make_source(
            title="Film.2026.1080p.WEB-DL.HEVC-Group",
            magnet="magnet:?xt=urn:btih:1234567890ABCDEF1234567890ABCDEF12345678",
            codec="hevc",
        )

        browser_capability = evaluate_browser_capability(source)
        result = prepare_playback_runtime(movie={"title": "Film"}, selected_source=source, sources=[source])

        self.assertFalse(browser_capability["browser_friendly"])
        self.assertTrue(browser_hard_fail_codec("hevc"))
        self.assertEqual(result["playback_runtime"], "external_runtime")
        self.assertIn("browser_hard_fail_codec", result["runtime_warnings"])

    def test_malformed_codec_handling_degrades_gracefully(self):
        source = make_source(
            title="Film.2026.1080p.WEB-DL.UNKNOWN-Group",
            magnet="magnet:?xt=urn:btih:1234567890ABCDEF1234567890ABCDEF12345678",
            codec=None,
        )

        capability = evaluate_capability_matrix(source)
        self.assertFalse(capability["browser_friendly"])
        self.assertIn("codec_unverified", capability["notes"])

    def test_runtime_diagnostics_generation(self):
        source = make_source(
            title="Film.2026.2160p.REMUX.x265-FraMeSToR",
            magnet="magnet:?xt=urn:btih:ABCDEF1234567890ABCDEF1234567890ABCDEF12",
            resolution="2160p",
            codec="x265",
            source_type="REMUX",
            size_gb=52.0,
            hdr=True,
        )
        capability = evaluate_capability_matrix(source)
        diagnostics = build_runtime_diagnostics(
            source=source,
            capability=capability,
            playback_runtime="external_runtime",
            runtime_profile={"warnings": ["size_heavy"]},
            penalties=["hdr_penalty"],
            browser_runtime={"warnings": ["browser_blocked"]},
            external_runtime={"warnings": ["mobile_handoff_risk"], "readiness": "external_ready"},
        )

        self.assertTrue(diagnostics["selected_reasoning"])
        self.assertTrue(diagnostics["compatibility_notes"])
        self.assertTrue(diagnostics["fallback_reason"])
        self.assertIn("hdr_penalty", diagnostics["penalties_applied"])

    def test_runtime_snapshot_serialization_is_json_safe(self):
        result = prepare_playback_runtime(
            movie={"title": "Film"},
            selected_source=make_source(
                title="Film.2026.1080p.WEB-DL.x264-NTb",
                magnet="magnet:?xt=urn:btih:1234567890ABCDEF1234567890ABCDEF12345678",
            ),
            sources=[],
        )

        snapshot = build_playback_readiness_snapshot(result, timestamp="2026-05-26T00:00:00Z")

        self.assertEqual(snapshot["timestamp"], "2026-05-26T00:00:00Z")
        self.assertIsInstance(snapshot["warnings"], list)
        self.assertIsInstance(snapshot["external_fallback_available"], bool)
        self.assertIsInstance(snapshot["execution_metrics"], dict)
        self.assertIsInstance(snapshot["execution_timeline"], dict)
        self.assertIsInstance(snapshot["execution_events"], list)

    def test_policy_thresholds_and_rejections(self):
        self.assertEqual(browser_safe_size_limit("browser_light"), 4.5)
        self.assertEqual(mobile_safe_size_limit(), 6.5)
        self.assertEqual(startup_confidence_threshold("high"), 80)
        self.assertTrue(is_browser_rejected_source_type("REMUX"))

    def test_mobile_capability_degrades_on_hdr_and_heavy_bandwidth(self):
        source = make_source(
            title="Film.2026.2160p.WEB-DL.x264-Group",
            magnet="magnet:?xt=urn:btih:1234567890ABCDEF1234567890ABCDEF12345678",
            resolution="2160p",
            size_gb=22.0,
            hdr=True,
        )
        mobile_capability = evaluate_mobile_capability(source)
        self.assertFalse(mobile_capability["mobile_friendly"])
        self.assertEqual(estimate_bandwidth_class(source), "high")
        self.assertEqual(estimate_startup_risk(source), "medium")

    def test_confidence_degradation_edge_case_for_browser_without_surface(self):
        source = make_source(
            title="Film.2026.1080p.WEB-DL.x264-NTb",
            magnet="magnet:?xt=urn:btih:1234567890ABCDEF1234567890ABCDEF12345678",
            size_gb=9.4,
        )
        result = prepare_playback_runtime(
            movie={"title": "Film"},
            selected_source=source,
            sources=[source],
            player_sources=[],
            fallback_urls=[],
        )

        self.assertEqual(result["browser_runtime"]["launch_strategy"], "browser_deferred")
        self.assertIn("no_browser_surface", result["runtime_warnings"])
        self.assertEqual(result["startup_confidence"], "low")
        self.assertEqual(result["playback_readiness"], "browser_deferred")

    def test_session_store_persists_playback_fields(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = StreamSessionStore(path=Path(temp_dir) / "sessions.json")
            session = StreamSession(
                session_id="abc123",
                movie_id="film-1",
                source_fingerprint="src123",
                handoff_mode="browser_handoff",
                preferred_runtime="browser_stream",
                session_state="prepared",
                playback_runtime="browser_runtime",
                runtime_profile="browser_balanced",
                selected_source={"source_fingerprint": "src123", "quality_label": "1080p"},
                playback_readiness="browser_ready",
                startup_confidence="high",
                runtime_warnings=["no_browser_surface"],
                execution_state="runtime_completed",
                execution_metrics={"stability_score": 82},
                execution_timeline={"estimated_startup_ms": 6200},
                simulated_runtime_health="stable",
                recovery_path={"path": "switch_runtime"},
                execution_events=[{"event_type": "runtime_completed"}],
            )

            store.save_session(session)
            saved = store.get_session("abc123")

            self.assertEqual(saved["playback_runtime"], "browser_runtime")
            self.assertEqual(saved["runtime_profile"], "browser_balanced")
            self.assertEqual(saved["selected_source"]["quality_label"], "1080p")
            self.assertEqual(saved["startup_confidence"], "high")
            self.assertEqual(saved["execution_state"], "runtime_completed")
            self.assertEqual(saved["execution_metrics"]["stability_score"], 82)


if __name__ == "__main__":
    unittest.main()
