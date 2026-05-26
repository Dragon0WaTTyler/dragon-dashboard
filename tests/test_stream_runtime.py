import unittest

from domains.magnets.playback import prepare_playback_runtime
from domains.magnets.playback.capability_matrix import evaluate_capability_matrix
from domains.magnets.stream_runtime import (
    InMemoryRuntimeRegistry,
    build_runtime_failure,
    build_runtime_manifest,
    build_runtime_preflight,
    determine_runtime_transport,
    evaluate_runtime_guardrails,
    evolve_runtime_state,
)


def make_source(
    *,
    title="Film.2026.1080p.WEB-DL.x264-NTb",
    magnet="magnet:?xt=urn:btih:1234567890ABCDEF1234567890ABCDEF12345678",
    resolution="1080p",
    codec="x264",
    source_type="WebDL",
    size_gb=4.2,
    seeders=120,
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
        "likely_streamable": likely_streamable,
        "estimated_quality_score": estimated_quality_score,
        "provider": "torrentio",
        "source": "torrentio",
        "hdr": hdr,
        "dolby_vision": dolby_vision,
        "source_fingerprint": "src123",
    }


class StreamRuntimeTests(unittest.TestCase):
    def test_runtime_state_transitions_are_deterministic(self):
        state = evolve_runtime_state("idle", "preflight")
        state = evolve_runtime_state(state, "runtime_ready")
        state = evolve_runtime_state(state, "handoff_ready")

        self.assertEqual(state, "handoff_ready")
        with self.assertRaises(ValueError):
            evolve_runtime_state("handoff_ready", "preflight")

    def test_guardrails_reject_browser_remux(self):
        source = make_source(
            title="Film.2026.2160p.REMUX.x265-FraMeSToR",
            resolution="2160p",
            codec="x265",
            source_type="REMUX",
            size_gb=52.0,
        )
        capability = evaluate_capability_matrix(source)

        result = evaluate_runtime_guardrails(
            source=source,
            capability_snapshot=capability,
            runtime_mode="browser_runtime",
            startup_confidence="low",
            runtime_profile="browser_cinematic",
        )

        self.assertFalse(result["allowed"])
        self.assertIn("browser_policy_block", result["blocking_reasons"])
        self.assertIn("bandwidth_insufficient", result["blocking_reasons"])

    def test_preflight_passes_for_browser_safe_source(self):
        source = make_source()
        capability = evaluate_capability_matrix(source)
        preflight = build_runtime_preflight(
            source=source,
            capability_snapshot=capability,
            runtime_mode="browser_runtime",
            runtime_profile="browser_balanced",
            startup_confidence="high",
            player_sources=[{"key": "vidsrc", "url": "https://example.com"}],
            fallback_urls=["https://example.com"],
            fallbacks=[{"runtime": "external_runtime", "available": True}],
        )

        self.assertTrue(preflight["runtime_allowed"])
        self.assertEqual(preflight["runtime_mode"], "browser_runtime")
        self.assertEqual(preflight["fallback_strategy"], "external_player_fallback")

    def test_preflight_blocks_invalid_magnet_without_fallback(self):
        source = make_source(magnet="not-a-magnet", likely_streamable=False)
        capability = evaluate_capability_matrix(source)
        preflight = build_runtime_preflight(
            source=source,
            capability_snapshot=capability,
            runtime_mode="browser_runtime",
            runtime_profile="browser_balanced",
            startup_confidence="low",
            player_sources=[],
            fallback_urls=[],
            fallbacks=[],
        )

        self.assertFalse(preflight["runtime_allowed"])
        self.assertEqual(preflight["runtime_mode"], "blocked")
        self.assertIn("invalid_magnet", preflight["blocking_reasons"])

    def test_runtime_manifest_generation_is_json_safe(self):
        manifest = build_runtime_manifest(
            runtime_id="rt1",
            session_id="sess1",
            selected_source=make_source(),
            runtime_mode="browser_runtime",
            runtime_state="runtime_ready",
            startup_confidence="high",
            capability_snapshot={"browser_friendly": True},
            diagnostics={"fallback_reason": ""},
            fallbacks=[{"id": "copy_magnet", "available": True}],
            preflight={"runtime_allowed": True},
            transport=determine_runtime_transport(runtime_mode="browser_runtime", browser_runtime={"launch_strategy": "browser_embed"}),
            created_at="2026-05-26T00:00:00Z",
        )

        self.assertEqual(manifest["runtime_id"], "rt1")
        self.assertEqual(manifest["transport"]["strategy"], "browser_embed")
        self.assertIsInstance(manifest["fallbacks"], list)

    def test_runtime_failure_shaping(self):
        failure = build_runtime_failure("unsupported_codec", diagnostics={"codec": "hevc"})

        self.assertEqual(failure["code"], "unsupported_codec")
        self.assertEqual(failure["category"], "compatibility")
        self.assertTrue(failure["user_safe_message"])
        self.assertEqual(failure["diagnostics"]["codec"], "hevc")

    def test_runtime_registry_lifecycle(self):
        registry = InMemoryRuntimeRegistry()
        created = registry.create(
            {
                "runtime_id": "rt1",
                "session_id": "sess1",
                "selected_source": {"source_fingerprint": "src123"},
                "runtime_profile": "browser_balanced",
                "runtime_state": "idle",
                "startup_confidence": "high",
            }
        )
        updated = registry.update("rt1", {"runtime_state": "preflight"})
        expired = registry.expire("rt1")

        self.assertEqual(created["runtime_id"], "rt1")
        self.assertEqual(updated["runtime_state"], "preflight")
        self.assertEqual(expired["runtime_state"], "expired")
        self.assertEqual(len(registry.list()), 1)

    def test_malformed_source_degrades_in_runtime_foundation(self):
        source = make_source(magnet="not-a-magnet", likely_streamable=False)
        result = prepare_playback_runtime(movie={"title": "Film"}, selected_source=source, sources=[source])

        self.assertEqual(result["runtime_state"], "runtime_blocked")
        self.assertEqual(result["runtime_mode"], "blocked")
        self.assertFalse(result["runtime_preflight"]["runtime_allowed"])
        self.assertIn("invalid_magnet", result["runtime_preflight"]["blocking_reasons"])


if __name__ == "__main__":
    unittest.main()
