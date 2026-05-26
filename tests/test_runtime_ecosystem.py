import json
import tempfile
import unittest
from pathlib import Path

from domains.magnets.playback import prepare_playback_runtime
from domains.magnets.runtime_ecosystem import build_runtime_ecosystem, load_ecosystem_memory


def make_context(
    *,
    playback_runtime="browser_runtime",
    runtime_profile="browser_cinematic",
    startup_confidence="high",
    degradation_risk=24,
    stability_score=82,
    fallback_probability=0.18,
    runtime_resilience=78,
    adaptation_pressure=20,
    coordination_confidence=80,
    prediction_confidence=78,
    authority_state="approved",
    balance_hint="stable",
    quality_label="1080p",
    high_bandwidth_required=False,
):
    return {
        "playback_runtime": playback_runtime,
        "runtime_mode": playback_runtime,
        "runtime_profile": runtime_profile,
        "startup_confidence": startup_confidence,
        "selected_source": {
            "source_fingerprint": "src-eco",
            "title": "Ecosystem Source",
            "quality_label": quality_label,
            "resolution": quality_label,
            "mobile_friendly": True,
            "high_bandwidth_required": high_bandwidth_required,
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
            "coordination_confidence": coordination_confidence,
        },
        "runtime_predictions": {
            "prediction_confidence": prediction_confidence,
        },
        "authority_state": authority_state,
        "runtime_identity": {
            "primary_trait": "adaptive_balanced" if balance_hint == "stable" else "resilience_first",
        },
        "continuity_state": {
            "continuity_state": "stable" if balance_hint == "stable" else "fragmented",
        },
    }


class RuntimeEcosystemTests(unittest.TestCase):
    def test_ecosystem_balance_shifts_to_resilience_stable(self):
        result = build_runtime_ecosystem(make_context(), persist_memory=False)

        self.assertEqual(result["ecosystem_balance"]["balance_state"], "resilience_stable")
        self.assertEqual(result["stability_zone"]["zone"], "cinematic_zone")

    def test_pressure_escalation_detected_under_stress(self):
        result = build_runtime_ecosystem(
            make_context(
                playback_runtime="external_runtime",
                runtime_profile="external_player_only",
                startup_confidence="low",
                degradation_risk=84,
                stability_score=34,
                fallback_probability=0.82,
                runtime_resilience=46,
                adaptation_pressure=72,
                coordination_confidence=42,
                prediction_confidence=38,
                authority_state="guarded",
                high_bandwidth_required=True,
            ),
            persist_memory=False,
        )

        self.assertEqual(result["orchestration_pressure"]["pressure_direction"], "escalating")
        self.assertIn("pressure_escalation_detected", {item["event"] for item in result["ecosystem_events"]})

    def test_degradation_current_propagates_deterministically(self):
        result = build_runtime_ecosystem(
            make_context(
                degradation_risk=88,
                fallback_probability=0.78,
                runtime_resilience=44,
                adaptation_pressure=70,
            ),
            persist_memory=False,
        )

        self.assertEqual(result["degradation_currents"]["current"], "cascading_degradation")
        self.assertEqual(result["ecosystem_forecast"]["forecast"], "degradation_spread")

    def test_resilience_topology_maps_distributed_resilience(self):
        result = build_runtime_ecosystem(
            make_context(runtime_resilience=84, fallback_probability=0.16, degradation_risk=22),
            persist_memory=False,
        )

        self.assertEqual(result["resilience_topology"]["topology"], "distributed_resilience")

    def test_equilibrium_evolution_fragments_under_adaptation_pressure(self):
        result = build_runtime_ecosystem(
            make_context(adaptation_pressure=74, runtime_resilience=58, degradation_risk=54, balance_hint="fragmented"),
            persist_memory=False,
        )

        self.assertEqual(result["adaptive_equilibrium"]["equilibrium_state"], "equilibrium_fragmented")

    def test_ecosystem_forecasting_prefers_resilience_convergence(self):
        result = build_runtime_ecosystem(
            make_context(runtime_resilience=84, fallback_probability=0.16, degradation_risk=24, quality_label="2160p"),
            persist_memory=False,
        )

        self.assertEqual(result["ecosystem_forecast"]["forecast"], "resilience_convergence")

    def test_corruption_recovery_rebuilds_memory(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            memory_path = Path(temp_dir) / "runtime_ecosystem_memory.json"
            memory_path.write_text("{bad json", encoding="utf-8")

            result = build_runtime_ecosystem(make_context(), memory_path=memory_path)
            recovered = json.loads(memory_path.read_text(encoding="utf-8"))

            self.assertEqual(result["ecosystem_memory"]["memory_status"], "recovered")
            self.assertEqual(recovered["corrupted_recoveries"], 1)

    def test_deterministic_outputs_without_persistence(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            context = make_context()
            memory_path = Path(temp_dir) / "runtime_ecosystem_memory.json"
            first = build_runtime_ecosystem(context, persist_memory=False, memory_path=memory_path)
            second = build_runtime_ecosystem(context, persist_memory=False, memory_path=memory_path)

            self.assertEqual(first, second)

    def test_governance_shaping_biases_suppression_under_cascading_degradation(self):
        result = build_runtime_ecosystem(
            make_context(degradation_risk=86, fallback_probability=0.74, runtime_resilience=48, adaptation_pressure=68),
            persist_memory=False,
        )

        self.assertIn("degradation_suppression", result["ecosystem_governance"]["governance_actions"])
        self.assertIn("fallback_containment", result["ecosystem_governance"]["governance_actions"])

    def test_memory_persistence_accumulates_runs(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            memory_path = Path(temp_dir) / "runtime_ecosystem_memory.json"
            build_runtime_ecosystem(make_context(), memory_path=memory_path)
            result = build_runtime_ecosystem(make_context(playback_runtime="external_runtime"), memory_path=memory_path)
            stored = load_ecosystem_memory(path=memory_path)

            self.assertEqual(result["ecosystem_memory"]["total_observations"], 2)
            self.assertEqual(stored["aggregates"]["total_runs"], 2)

    def test_prepare_playback_runtime_exposes_ecosystem_payloads(self):
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
            "source_fingerprint": "src-eco-plan",
        }

        plan = prepare_playback_runtime(movie={"title": "Film"}, selected_source=source, sources=[source])

        self.assertIn("runtime_ecosystem", plan)
        self.assertIn("ecosystem_balance", plan["session_payload"])
        self.assertIn("ecosystem_metrics", plan["readiness_snapshot"])
        self.assertIn("stability_zone", plan["session_payload"])


if __name__ == "__main__":
    unittest.main()
