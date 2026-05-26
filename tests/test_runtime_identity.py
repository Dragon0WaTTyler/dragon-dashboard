import json
import tempfile
import unittest
from pathlib import Path

from domains.magnets.playback import prepare_playback_runtime
from domains.magnets.runtime_identity import build_runtime_identity, load_identity_memory


def make_identity_context(
    *,
    playback_runtime="browser_runtime",
    runtime_profile="browser_cinematic",
    startup_confidence="high",
    degradation_risk=24,
    stability_score=82,
    fallback_probability=0.18,
    runtime_resilience=78,
    adaptation_pressure=18,
    forecast_risk="low",
    mobile_friendly=True,
    high_bandwidth_required=False,
    quality_label="1080p",
    fallback_strategy="external_player_fallback",
    confidence_delta=6,
    confidence_stability="stable",
):
    return {
        "playback_runtime": playback_runtime,
        "runtime_mode": playback_runtime,
        "runtime_profile": runtime_profile,
        "startup_confidence": startup_confidence,
        "selected_source": {
            "source_fingerprint": "src-identity",
            "title": "Identity Source",
            "quality_label": quality_label,
            "resolution": quality_label,
            "mobile_friendly": mobile_friendly,
            "high_bandwidth_required": high_bandwidth_required,
        },
        "runtime_preflight": {
            "fallback_strategy": fallback_strategy,
        },
        "execution_metrics": {
            "degradation_risk": degradation_risk,
            "stability_score": stability_score,
        },
        "execution_timeline": {
            "fallback_probability": fallback_probability,
        },
        "coordination_metrics": {
            "runtime_resilience": runtime_resilience,
            "adaptation_pressure": adaptation_pressure,
        },
        "confidence_evolution": {
            "confidence_delta": confidence_delta,
            "confidence_stability": confidence_stability,
        },
        "orchestration_forecast": {
            "forecast_risk": forecast_risk,
        },
        "runtime_warnings": [],
    }


class RuntimeIdentityTests(unittest.TestCase):
    def test_identity_persistence_accumulates_observations(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            memory_path = Path(temp_dir) / "runtime_identity_memory.json"
            build_runtime_identity(make_identity_context(), memory_path=memory_path)
            result = build_runtime_identity(
                make_identity_context(playback_runtime="external_runtime", runtime_profile="external_player_only"),
                memory_path=memory_path,
            )
            stored = load_identity_memory(path=memory_path)

            self.assertTrue(memory_path.exists())
            self.assertEqual(result["identity_memory_summary"]["total_observations"], 2)
            self.assertEqual(stored["aggregates"]["total_runs"], 2)

    def test_corruption_recovery_rebuilds_json_memory(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            memory_path = Path(temp_dir) / "runtime_identity_memory.json"
            memory_path.write_text("{broken json", encoding="utf-8")

            result = build_runtime_identity(make_identity_context(), memory_path=memory_path)
            recovered = json.loads(memory_path.read_text(encoding="utf-8"))

            self.assertEqual(result["identity_memory_summary"]["memory_status"], "recovered")
            self.assertEqual(recovered["corrupted_recoveries"], 1)

    def test_behavioral_drift_hardens_under_repeated_fallback_pressure(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            memory_path = Path(temp_dir) / "runtime_identity_memory.json"
            unstable = make_identity_context(
                playback_runtime="external_runtime",
                runtime_profile="external_player_only",
                startup_confidence="low",
                degradation_risk=82,
                stability_score=42,
                fallback_probability=0.84,
                runtime_resilience=58,
                adaptation_pressure=66,
                forecast_risk="high",
                high_bandwidth_required=True,
            )
            for _ in range(3):
                build_runtime_identity(unstable, memory_path=memory_path)
            result = build_runtime_identity(unstable, memory_path=memory_path)

            self.assertEqual(result["behavioral_drift"]["drift_state"], "stronger_fallback_dependency")
            self.assertGreaterEqual(result["behavioral_drift"]["drift_score"], 50)

    def test_archetype_generation_prefers_cinematic_orchestrator_for_stable_cinematic_runtime(self):
        result = build_runtime_identity(
            make_identity_context(runtime_profile="browser_cinematic", quality_label="2160p"),
            persist_memory=False,
        )

        self.assertEqual(result["orchestration_archetype"], "cinematic_orchestrator")

    def test_temperament_shifts_to_defensive_under_high_risk(self):
        result = build_runtime_identity(
            make_identity_context(
                playback_runtime="external_runtime",
                startup_confidence="low",
                degradation_risk=86,
                stability_score=34,
                fallback_probability=0.88,
            ),
            persist_memory=False,
        )

        self.assertEqual(result["runtime_temperament"], "defensive")

    def test_continuity_tracker_strengthens_across_stable_runs(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            memory_path = Path(temp_dir) / "runtime_identity_memory.json"
            stable = make_identity_context()
            for _ in range(4):
                build_runtime_identity(stable, memory_path=memory_path)
            result = build_runtime_identity(stable, memory_path=memory_path)

            self.assertIn(result["continuity_state"]["continuity_state"], {"stable", "developing"})
            self.assertGreaterEqual(result["continuity_state"]["continuity_confidence"], 40)

    def test_adaptation_profile_evolves_for_degraded_environment(self):
        result = build_runtime_identity(
            make_identity_context(
                playback_runtime="external_runtime",
                runtime_profile="external_player_only",
                degradation_risk=76,
                fallback_probability=0.74,
                adaptation_pressure=58,
            ),
            persist_memory=False,
        )

        self.assertEqual(result["adaptation_profile"], "degraded_environment_specialist")

    def test_preference_evolution_tracks_emergent_runtime_bias(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            memory_path = Path(temp_dir) / "runtime_identity_memory.json"
            context = make_identity_context(
                playback_runtime="external_runtime",
                runtime_profile="external_player_only",
                high_bandwidth_required=True,
                quality_label="2160p",
                fallback_strategy="external_player",
            )
            for _ in range(3):
                build_runtime_identity(context, memory_path=memory_path)
            result = build_runtime_identity(context, memory_path=memory_path)

            self.assertEqual(result["preference_evolution"]["browser_preference"], "external_runtime")
            self.assertEqual(result["preference_evolution"]["bandwidth_adaptation_preference"], "constrained")

    def test_identity_engine_is_deterministic_without_persistence(self):
        context = make_identity_context()
        first = build_runtime_identity(context, persist_memory=False)
        second = build_runtime_identity(context, persist_memory=False)

        self.assertEqual(first, second)

    def test_prepare_playback_runtime_exposes_identity_payloads(self):
        source = {
            "title": "Film.2026.1080p.WEB-DL.x264-NTb",
            "magnet": "magnet:?xt=urn:btih:1234567890ABCDEF1234567890ABCDEF12345678",
            "quality_label": "1080p",
            "resolution": "1080p",
            "codec": "x264",
            "source_type": "WebDL",
            "browser_playable_candidate": True,
            "mobile_friendly": True,
            "high_bandwidth_required": False,
            "runtime_profile": "browser_cinematic",
            "runtime_recommended": "browser_runtime",
            "source_fingerprint": "src-identity-plan",
        }

        plan = prepare_playback_runtime(movie={"title": "Film"}, selected_source=source, sources=[source])

        self.assertIn("runtime_identity", plan)
        self.assertIn("orchestration_archetype", plan["session_payload"])
        self.assertIn("runtime_temperament", plan["session_payload"])
        self.assertIn("identity_metrics", plan["readiness_snapshot"])


if __name__ == "__main__":
    unittest.main()
