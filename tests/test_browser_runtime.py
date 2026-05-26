import unittest

from domains.magnets.browser_runtime import (
    InMemoryBrowserRuntimeSessionRegistry,
    build_browser_runtime_bridge,
    build_browser_runtime_session,
    build_capability_snapshot,
    build_runtime_bootstrap,
    build_runtime_limits,
    estimate_browser_risk,
    evaluate_runtime_sandbox,
    normalize_runtime_source,
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
    hdr=False,
    dolby_vision=False,
    likely_streamable=True,
):
    return {
        "title": title,
        "magnet": magnet,
        "resolution": resolution,
        "quality_label": resolution,
        "codec": codec,
        "source_type": source_type,
        "size_gb": size_gb,
        "seeders": seeders,
        "provider": "torrentio",
        "source": "torrentio",
        "hdr": hdr,
        "dolby_vision": dolby_vision,
        "likely_streamable": likely_streamable,
        "source_fingerprint": "src123",
    }


def make_manifest(*, runtime_mode="browser_runtime", runtime_state="runtime_ready", startup_confidence="high"):
    return {
        "runtime_id": "rt1",
        "session_id": "sess1",
        "runtime_mode": runtime_mode,
        "runtime_state": runtime_state,
        "runtime_profile": "browser_balanced",
        "startup_confidence": startup_confidence,
        "selected_source": {"source_fingerprint": "src123", "title": "Film"},
        "preflight": {"fallback_strategy": "external_player_fallback"},
        "transport": {"strategy": "browser_embed"},
        "fallbacks": [{"runtime": "external_runtime", "available": True}],
    }


class BrowserRuntimeTests(unittest.TestCase):
    def test_capability_snapshot_is_deterministic(self):
        source = normalize_runtime_source(make_source())
        snapshot = build_capability_snapshot(
            source,
            runtime_manifest=make_manifest(),
            readiness_snapshot={"startup_confidence": "high"},
        )

        self.assertEqual(snapshot["browser_codec_support_assumption"], "supported")
        self.assertEqual(snapshot["browser_safety_class"], "safe")
        self.assertEqual(snapshot["startup_viability"], "viable")

    def test_bootstrap_planning_for_browser_runtime(self):
        source = normalize_runtime_source(make_source())
        capability = build_capability_snapshot(source, runtime_manifest=make_manifest(), readiness_snapshot={"startup_confidence": "high"})
        sandbox = evaluate_runtime_sandbox(
            runtime_manifest=make_manifest(),
            source_descriptor=source,
            capability_snapshot=capability,
        )
        bootstrap = build_runtime_bootstrap(
            runtime_manifest=make_manifest(),
            playback_plan={"runtime_mode": "browser_runtime"},
            readiness_snapshot={"fallback_strategy": "external_player_fallback"},
            source_metadata=source,
            capability_snapshot=capability,
            sandbox=sandbox,
        )

        self.assertTrue(bootstrap["bootstrap_allowed"])
        self.assertEqual(bootstrap["bootstrap_mode"], "browser_sandbox")
        self.assertEqual(bootstrap["fallback_runtime"], "external_runtime")

    def test_runtime_bridge_shapes_safe_handoff(self):
        bridge = build_browser_runtime_bridge(
            runtime_manifest=make_manifest(),
            playback_plan={"runtime_mode": "browser_runtime", "fallbacks": [{"runtime": "external_runtime", "available": True}]},
            readiness_snapshot={"startup_confidence": "high", "fallback_strategy": "external_player_fallback"},
            source_metadata=make_source(),
        )

        self.assertTrue(bridge["experimental"])
        self.assertTrue(bridge["bridge_allowed"])
        self.assertEqual(bridge["player_descriptor"]["player_mode"], "browser_sandbox")
        self.assertEqual(bridge["runtime_payload"]["runtime_target"], "experimental_browser_runtime")

    def test_sandbox_rejects_malformed_manifest(self):
        source = normalize_runtime_source(make_source())
        capability = build_capability_snapshot(source, runtime_manifest=make_manifest(runtime_mode="blocked"), readiness_snapshot={"startup_confidence": "high"})
        sandbox = evaluate_runtime_sandbox(
            runtime_manifest=make_manifest(runtime_mode="blocked", runtime_state="runtime_blocked"),
            source_descriptor=source,
            capability_snapshot=capability,
        )

        self.assertFalse(sandbox["sandbox_allowed"])
        self.assertIn("unsupported_runtime_mode", sandbox["blocking_reasons"])
        self.assertIn("blocked_runtime_manifest", sandbox["blocking_reasons"])

    def test_malformed_runtime_manifest_degrades_bridge(self):
        bridge = build_browser_runtime_bridge(
            runtime_manifest=make_manifest(runtime_mode="blocked", runtime_state="runtime_blocked", startup_confidence="low"),
            playback_plan={"runtime_mode": "blocked", "fallbacks": []},
            readiness_snapshot={"startup_confidence": "low", "fallback_strategy": "none"},
            source_metadata=make_source(magnet="not-a-magnet", likely_streamable=False),
        )

        self.assertFalse(bridge["bridge_allowed"])
        self.assertEqual(bridge["bridge_mode"], "blocked")
        self.assertEqual(bridge["sandbox"]["sandbox_status"], "sandbox_blocked")

    def test_runtime_session_lifecycle(self):
        registry = InMemoryBrowserRuntimeSessionRegistry()
        session = build_browser_runtime_session(
            linked_stream_runtime_id="rt1",
            playback_session_id="sess1",
            runtime_state="runtime_ready",
            capability_snapshot={"browser_safety_class": "safe"},
            bootstrap_summary={"bootstrap_mode": "browser_sandbox"},
        )
        created = registry.create(session)
        updated = registry.update(created["runtime_session_id"], {"runtime_state": "handoff_ready"})
        expired = registry.expire(created["runtime_session_id"])

        self.assertEqual(updated["runtime_state"], "handoff_ready")
        self.assertEqual(expired["runtime_state"], "expired")
        self.assertEqual(len(registry.list()), 1)

    def test_runtime_degradation_behavior(self):
        source = normalize_runtime_source(make_source(size_gb=14.0, hdr=True))
        limits = build_runtime_limits(source=source)
        risk = estimate_browser_risk(
            source=source,
            limits=limits,
            runtime_manifest=make_manifest(startup_confidence="low"),
            readiness_snapshot={"startup_confidence": "low"},
        )
        snapshot = build_capability_snapshot(
            source,
            runtime_manifest=make_manifest(startup_confidence="low"),
            readiness_snapshot={"startup_confidence": "low"},
        )

        self.assertEqual(risk, "high")
        self.assertIn("memory_pressure_risk", limits["degradation_rules"])
        self.assertIn("browser_size_limit_exceeded", snapshot["degradation_warnings"])


if __name__ == "__main__":
    unittest.main()
