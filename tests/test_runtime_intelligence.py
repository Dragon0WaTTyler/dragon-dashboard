import json
import tempfile
import unittest
from pathlib import Path

from domains.magnets.runtime_intelligence import build_runtime_intelligence, load_runtime_memory


def make_orchestration(
    *,
    runtime_profile="browser_balanced",
    playback_runtime="browser_runtime",
    startup_confidence="high",
    stability_score=84,
    degradation_risk=22,
    execution_outcome="runtime_active",
    simulated_runtime_health="stable",
    browser_rejected=False,
    mobile_friendly=True,
    codec="x264",
    resolution="1080p",
    source_type="WebDL",
):
    return {
        "runtime_profile": runtime_profile,
        "playback_runtime": playback_runtime,
        "runtime_mode": playback_runtime,
        "startup_confidence": startup_confidence,
        "selected_source": {
            "source_fingerprint": "src-1",
            "mobile_friendly": mobile_friendly,
            "codec": codec,
            "quality_label": resolution,
            "source_type": source_type,
        },
        "readiness_snapshot": {
            "runtime_profile": runtime_profile,
            "playback_runtime": playback_runtime,
            "runtime_mode": playback_runtime,
            "selected_source": {
                "mobile_friendly": mobile_friendly,
            },
        },
        "execution_metrics": {
            "startup_score": 82 if startup_confidence == "high" else 48,
            "stability_score": stability_score,
            "degradation_risk": degradation_risk,
            "runtime_confidence": stability_score,
        },
        "execution_timeline": {
            "estimated_startup_ms": 6200,
            "fallback_probability": 0.18 if execution_outcome == "runtime_active" else 0.72,
        },
        "recovery_path": {
            "path": "external_handoff" if execution_outcome == "fallback" else "degrade_quality",
        },
        "coordination_metrics": {
            "runtime_resilience": 78 if execution_outcome != "fallback" else 58,
            "coordination_confidence": 80 if execution_outcome != "fallback" else 52,
            "adaptation_pressure": 18 if execution_outcome != "fallback" else 64,
        },
        "runtime_negotiation": {
            "selected_runtime": playback_runtime,
            "fallback_runtime": "external_runtime",
        },
        "adaptive_strategy": {
            "adaptation_rule": "retain_runtime" if execution_outcome != "fallback" else "switch_browser_to_external",
            "target_runtime": playback_runtime if execution_outcome != "fallback" else "external_runtime",
        },
        "runtime_switch_history": [
            {
                "current_runtime": "browser_runtime",
                "target_runtime": playback_runtime if execution_outcome != "fallback" else "external_runtime",
                "switch_strategy": "retain_runtime" if execution_outcome != "fallback" else "browser_to_external_handoff",
            }
        ],
        "fallback_negotiation": {
            "fallback_urgency": "low" if execution_outcome != "fallback" else "high",
        },
        "transport_descriptor": {
            "transport_class": "browser_progressive" if playback_runtime == "browser_runtime" else "external_handoff",
        },
        "guardrails": {
            "rejected": browser_rejected,
            "blocking_reasons": ["unsupported_codec_rejection"] if browser_rejected else [],
        },
        "execution_outcome": execution_outcome,
        "simulated_runtime_health": simulated_runtime_health,
    }


class RuntimeIntelligenceTests(unittest.TestCase):
    def test_runtime_memory_persistence_and_summary(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            memory_path = Path(temp_dir) / "runtime_memory.json"
            result = build_runtime_intelligence(make_orchestration(), memory_path=memory_path)
            summary = result["runtime_memory_summary"]

            self.assertEqual(summary["total_observations"], 1)
            self.assertEqual(summary["memory_status"], "healthy")
            self.assertTrue(memory_path.exists())

    def test_corruption_recovery_rebuilds_memory_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            memory_path = Path(temp_dir) / "runtime_memory.json"
            memory_path.parent.mkdir(parents=True, exist_ok=True)
            memory_path.write_text("{not-json", encoding="utf-8")

            result = build_runtime_intelligence(make_orchestration(), memory_path=memory_path)

            self.assertEqual(result["runtime_memory_summary"]["memory_status"], "recovered")
            recovered = load_runtime_memory(path=memory_path)
            self.assertGreaterEqual(recovered["corrupted_recoveries"], 1)

    def test_prediction_shaping_prefers_external_fallback_when_history_is_bad(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            memory_path = Path(temp_dir) / "runtime_memory.json"
            for _ in range(3):
                build_runtime_intelligence(
                    make_orchestration(
                        runtime_profile="browser_cinematic",
                        playback_runtime="external_runtime",
                        startup_confidence="low",
                        stability_score=42,
                        degradation_risk=76,
                        execution_outcome="fallback",
                        simulated_runtime_health="degraded",
                        browser_rejected=True,
                        mobile_friendly=False,
                        codec="x265",
                        resolution="2160p",
                        source_type="REMUX",
                    ),
                    memory_path=memory_path,
                )
            result = build_runtime_intelligence(
                make_orchestration(
                    runtime_profile="browser_cinematic",
                    playback_runtime="external_runtime",
                    startup_confidence="low",
                    stability_score=48,
                    degradation_risk=72,
                    execution_outcome="fallback",
                    simulated_runtime_health="degraded",
                    browser_rejected=True,
                    mobile_friendly=False,
                    codec="x265",
                    resolution="2160p",
                    source_type="REMUX",
                ),
                memory_path=memory_path,
            )

            self.assertEqual(result["runtime_predictions"]["predicted_outcome"], "likely_external_fallback")
            self.assertGreaterEqual(result["orchestration_forecast"]["forecast_confidence"], 1)

    def test_confidence_evolution_tracks_direction_and_delta(self):
        result = build_runtime_intelligence(
            make_orchestration(startup_confidence="low", stability_score=80, degradation_risk=18),
            persist_memory=False,
        )
        evolution = result["confidence_evolution"]

        self.assertIn(evolution["confidence_direction"], {"up", "steady"})
        self.assertIsInstance(evolution["confidence_delta"], int)
        self.assertEqual(len(evolution["stages"]), 5)

    def test_adaptation_history_tracks_switch_sequences(self):
        result = build_runtime_intelligence(
            make_orchestration(execution_outcome="fallback", simulated_runtime_health="degraded"),
            persist_memory=False,
        )
        history = result["adaptation_history"]

        self.assertGreaterEqual(history["switch_frequency"], 1)
        self.assertTrue(history["runtime_evolution_trace"])

    def test_reputation_scoring_is_explainable(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            memory_path = Path(temp_dir) / "runtime_memory.json"
            build_runtime_intelligence(make_orchestration(), memory_path=memory_path)
            build_runtime_intelligence(make_orchestration(execution_outcome="fallback", simulated_runtime_health="degraded"), memory_path=memory_path)
            result = build_runtime_intelligence(make_orchestration(), memory_path=memory_path)
            reputation = result["runtime_reputation"]["runtime_profiles"]["browser_balanced"]

            self.assertIn("stability_reputation", reputation)
            self.assertIn("orchestration_trust", reputation)
            self.assertGreaterEqual(reputation["evidence_count"], 1)

    def test_forecasting_logic_is_explainable_and_json_safe(self):
        result = build_runtime_intelligence(
            make_orchestration(
                runtime_profile="browser_cinematic",
                playback_runtime="external_runtime",
                startup_confidence="low",
                stability_score=40,
                degradation_risk=80,
                execution_outcome="fallback",
                simulated_runtime_health="degraded",
            ),
            persist_memory=False,
        )
        encoded = json.dumps(result["orchestration_forecast"])

        self.assertIn(result["orchestration_forecast"]["forecast"], {
            "high_probability_of_external_fallback",
            "cinematic_runtime_risk_elevated",
            "mobile_runtime_likely_unstable",
        })
        self.assertIsInstance(encoded, str)

    def test_learning_rules_penalize_degradation_behavior(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            memory_path = Path(temp_dir) / "runtime_memory.json"
            for _ in range(4):
                build_runtime_intelligence(
                    make_orchestration(
                        startup_confidence="low",
                        stability_score=44,
                        degradation_risk=74,
                        execution_outcome="fallback",
                        simulated_runtime_health="degraded",
                        browser_rejected=True,
                    ),
                    memory_path=memory_path,
                )
            result = build_runtime_intelligence(
                make_orchestration(
                    startup_confidence="low",
                    stability_score=44,
                    degradation_risk=74,
                    execution_outcome="fallback",
                    simulated_runtime_health="degraded",
                    browser_rejected=True,
                ),
                memory_path=memory_path,
            )
            learning = result["runtime_learning"]

            self.assertLessEqual(learning["runtime_confidence_adjustment"], 0)
            self.assertTrue(any(rule["rule"] == "repeated_instability_lowers_runtime_confidence" for rule in learning["learning_rules"]))


if __name__ == "__main__":
    unittest.main()
