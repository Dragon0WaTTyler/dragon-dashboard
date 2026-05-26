import json
import tempfile
import unittest
from pathlib import Path

from domains.magnets.playback import prepare_playback_runtime
from domains.magnets.runtime_cinema import build_runtime_cinema, load_cinematic_memory


def make_cinema_context(
    *,
    playback_runtime="browser_runtime",
    runtime_profile="browser_cinematic",
    startup_confidence="high",
    degradation_risk=22,
    stability_score=84,
    fallback_probability=0.14,
    runtime_resilience=80,
    adaptation_pressure=18,
    pressure_direction="steady",
    pressure_score=34,
    climate="calm_climate",
    equilibrium_state="equilibrium_stable",
    continuity_state="stable",
    continuity_confidence=76,
    drift_score=12,
    switch_frequency=0,
    authority_state="approved",
    archetype="cinematic_orchestrator",
    forecast_risk="low",
    quality_label="1080p",
):
    return {
        "playback_runtime": playback_runtime,
        "runtime_mode": playback_runtime,
        "runtime_profile": runtime_profile,
        "startup_confidence": startup_confidence,
        "selected_source": {
            "source_fingerprint": "src-cinema",
            "title": "Cinema Source",
            "quality_label": quality_label,
            "resolution": quality_label,
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
        "orchestration_pressure": {
            "pressure_direction": pressure_direction,
            "pressure_score": pressure_score,
        },
        "ecosystem_climate": {
            "climate": climate,
        },
        "resilience_topology": {
            "topology": "distributed_resilience" if runtime_resilience >= 72 else "fallback_resilience",
        },
        "adaptive_equilibrium": {
            "equilibrium_state": equilibrium_state,
        },
        "continuity_state": {
            "continuity_state": continuity_state,
            "continuity_confidence": continuity_confidence,
        },
        "behavioral_drift": {
            "drift_score": drift_score,
        },
        "adaptation_history": {
            "switch_frequency": switch_frequency,
        },
        "authority_state": authority_state,
        "orchestration_archetype": archetype,
        "orchestration_forecast": {
            "forecast_risk": forecast_risk,
        },
    }


class RuntimeCinemaTests(unittest.TestCase):
    def test_cinematic_direction_shapes_stable_runtime(self):
        result = build_runtime_cinema(make_cinema_context(), persist_memory=False)

        self.assertEqual(result["cinematic_direction"]["style"], "cinematic_stable")

    def test_pacing_evolves_under_adaptation_pressure(self):
        result = build_runtime_cinema(
            make_cinema_context(adaptation_pressure=76, fallback_probability=0.62, stability_score=46),
            persist_memory=False,
        )

        self.assertEqual(result["runtime_pacing"]["pacing"], "adaptive_pacing")

    def test_immersion_degrades_under_high_risk(self):
        result = build_runtime_cinema(
            make_cinema_context(
                playback_runtime="external_runtime",
                runtime_profile="external_player_only",
                startup_confidence="low",
                degradation_risk=86,
                runtime_resilience=42,
                continuity_confidence=34,
                forecast_risk="high",
            ),
            persist_memory=False,
        )

        self.assertEqual(result["immersion_state"]["state"], "degraded_immersion")

    def test_atmosphere_stability_prefers_resilient_atmosphere(self):
        result = build_runtime_cinema(
            make_cinema_context(runtime_resilience=88, degradation_risk=18, pressure_direction="steady"),
            persist_memory=False,
        )

        self.assertEqual(result["runtime_atmosphere"]["atmosphere"], "resilient_atmosphere")

    def test_dramatic_tension_tracks_escalation(self):
        result = build_runtime_cinema(
            make_cinema_context(pressure_score=78, adaptation_pressure=74, degradation_risk=68, fallback_probability=0.58),
            persist_memory=False,
        )

        self.assertEqual(result["dramatic_tension"]["tension"], "escalating_tension")

    def test_continuity_shaping_fragments_when_switches_accumulate(self):
        result = build_runtime_cinema(
            make_cinema_context(continuity_state="developing", continuity_confidence=58, switch_frequency=4, drift_score=54),
            persist_memory=False,
        )

        self.assertEqual(result["continuity_cinema"]["continuity"], "fragmented_continuity")

    def test_cinematic_governance_contains_degradation(self):
        result = build_runtime_cinema(
            make_cinema_context(
                playback_runtime="external_runtime",
                runtime_profile="external_player_only",
                startup_confidence="low",
                degradation_risk=84,
                adaptation_pressure=72,
                continuity_state="fragmented",
                continuity_confidence=36,
                switch_frequency=3,
                forecast_risk="high",
            ),
            persist_memory=False,
        )

        self.assertIn("contain_degradation", result["cinematic_governance"]["governance_actions"])
        self.assertIn("preserve_cinematic_continuity", result["cinematic_governance"]["governance_actions"])

    def test_forecasting_biases_to_recovery_under_degraded_balance(self):
        result = build_runtime_cinema(
            make_cinema_context(
                degradation_risk=82,
                stability_score=38,
                adaptation_pressure=66,
                continuity_confidence=40,
                equilibrium_state="equilibrium_fragmented",
            ),
            persist_memory=False,
        )

        self.assertEqual(result["cinematic_forecast"]["forecast"], "cinematic_recovery")

    def test_corruption_recovery_rebuilds_memory(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            memory_path = Path(temp_dir) / "runtime_cinema_memory.json"
            memory_path.write_text("{bad json", encoding="utf-8")

            result = build_runtime_cinema(make_cinema_context(), memory_path=memory_path)
            recovered = json.loads(memory_path.read_text(encoding="utf-8"))

            self.assertEqual(result["cinematic_memory"]["memory_status"], "recovered")
            self.assertEqual(recovered["corrupted_recoveries"], 1)

    def test_deterministic_outputs_without_persistence(self):
        context = make_cinema_context()

        first = build_runtime_cinema(context, persist_memory=False)
        second = build_runtime_cinema(context, persist_memory=False)

        self.assertEqual(first, second)

    def test_memory_persistence_accumulates_runs(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            memory_path = Path(temp_dir) / "runtime_cinema_memory.json"
            build_runtime_cinema(make_cinema_context(), memory_path=memory_path)
            result = build_runtime_cinema(
                make_cinema_context(playback_runtime="external_runtime", runtime_profile="external_player_only"),
                memory_path=memory_path,
            )
            stored = load_cinematic_memory(path=memory_path)

            self.assertEqual(result["cinematic_memory"]["total_observations"], 2)
            self.assertEqual(stored["aggregates"]["total_runs"], 2)

    def test_prepare_playback_runtime_exposes_cinema_payloads(self):
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
            "source_fingerprint": "src-cinema-plan",
        }

        plan = prepare_playback_runtime(movie={"title": "Film"}, selected_source=source, sources=[source])

        self.assertIn("runtime_cinema", plan)
        self.assertIn("cinematic_direction", plan["session_payload"])
        self.assertIn("cinematic_metrics", plan["readiness_snapshot"])
        self.assertIn("runtime_atmosphere", plan["session_payload"])


if __name__ == "__main__":
    unittest.main()
