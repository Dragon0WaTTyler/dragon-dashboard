import tempfile
import unittest
from pathlib import Path

from domains.magnets.playback import prepare_playback_runtime
from domains.magnets.runtime_authority import build_runtime_authority, load_authority_memory


def make_context(
    *,
    playback_runtime="browser_runtime",
    runtime_profile="browser_cinematic",
    startup_confidence="high",
    degradation_risk=22,
    runtime_confidence=78,
    coordination_confidence=80,
    runtime_resilience=76,
    fallback_probability=0.18,
    mobile_runtime_risk="low",
    browser_risk="low",
    browser_safety_class="safe",
    memory_risk="low",
    predicted_outcome="likely_stable_browser_runtime",
    prediction_confidence=76,
    confidence_stability="stable",
    confidence_delta=4,
    fallback_urgency="low",
    mobile_friendly=True,
    high_bandwidth_required=False,
    switch_history=None,
    runtime_instability=0.12,
    forecast_risk="low",
):
    return {
        "playback_runtime": playback_runtime,
        "runtime_mode": playback_runtime,
        "runtime_profile": runtime_profile,
        "startup_confidence": startup_confidence,
        "selected_source": {
            "source_fingerprint": "src-1",
            "title": "Example Source",
            "quality_label": "1080p",
            "mobile_friendly": mobile_friendly,
            "high_bandwidth_required": high_bandwidth_required,
        },
        "capability_snapshot": {
            "mobile_runtime_risk": mobile_runtime_risk,
            "browser_risk": browser_risk,
            "browser_safety_class": browser_safety_class,
            "memory_risk": memory_risk,
            "startup_viability": "fragile" if degradation_risk >= 55 else "viable",
        },
        "execution_metrics": {
            "degradation_risk": degradation_risk,
            "runtime_confidence": runtime_confidence,
            "stability_score": runtime_confidence,
        },
        "execution_timeline": {
            "estimated_startup_ms": 6200,
            "fallback_probability": fallback_probability,
        },
        "coordination_metrics": {
            "coordination_confidence": coordination_confidence,
            "runtime_resilience": runtime_resilience,
            "adaptation_pressure": 18 if degradation_risk < 45 else 58,
        },
        "runtime_negotiation": {
            "selected_runtime": playback_runtime,
            "fallback_runtime": "external_runtime",
        },
        "runtime_predictions": {
            "predicted_outcome": predicted_outcome,
            "prediction_confidence": prediction_confidence,
        },
        "confidence_evolution": {
            "stages": [
                {"stage": "before_negotiation", "confidence": 82 if startup_confidence == "high" else 38},
                {"stage": "after_fallback", "confidence": runtime_confidence},
            ],
            "confidence_stability": confidence_stability,
            "confidence_delta": confidence_delta,
            "confidence_direction": "down" if confidence_delta < 0 else ("up" if confidence_delta > 0 else "steady"),
        },
        "runtime_learning": {
            "fallback_trust_adjustment": 4 if fallback_probability >= 0.5 else 0,
        },
        "fallback_negotiation": {
            "fallback_urgency": fallback_urgency,
        },
        "runtime_switch_history": switch_history
        if switch_history is not None
        else [{"current_runtime": playback_runtime, "target_runtime": playback_runtime, "switch_strategy": "retain_runtime"}],
        "runtime_memory_summary": {
            "runtime_instability": runtime_instability,
        },
        "orchestration_forecast": {
            "forecast": "high_probability_of_external_fallback" if forecast_risk == "high" else "stable_browser_runtime_expected",
            "forecast_risk": forecast_risk,
        },
    }


class RuntimeAuthorityTests(unittest.TestCase):
    def test_arbitration_conflict_overrides_browser_runtime(self):
        result = build_runtime_authority(
            make_context(
                degradation_risk=74,
                runtime_confidence=48,
                coordination_confidence=52,
                fallback_probability=0.72,
                predicted_outcome="likely_external_fallback",
                forecast_risk="high",
                confidence_stability="volatile",
                confidence_delta=-18,
            ),
            persist_memory=False,
        )

        self.assertEqual(result["approved_runtime"], "external_runtime")
        self.assertEqual(result["arbitration_result"]["arbitration_result"], "runtime_overridden")

    def test_stability_guard_intervenes_on_oscillation(self):
        result = build_runtime_authority(
            make_context(
                degradation_risk=68,
                fallback_probability=0.76,
                confidence_stability="volatile",
                confidence_delta=-16,
                switch_history=[
                    {"current_runtime": "browser_runtime", "target_runtime": "external_runtime", "switch_strategy": "browser_to_external_handoff"},
                    {"current_runtime": "external_runtime", "target_runtime": "browser_runtime", "switch_strategy": "external_to_browser_recovery"},
                ],
                forecast_risk="high",
            ),
            persist_memory=False,
        )

        self.assertEqual(result["stability_state"]["guard_intervention"], "freeze_to_safe_fallback")
        self.assertIn("oscillation_prevented", result["governance_actions"])

    def test_fallback_suppression_terminates_loops_from_memory(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            memory_path = Path(temp_dir) / "runtime_authority_memory.json"
            unstable = make_context(
                degradation_risk=78,
                fallback_probability=0.82,
                predicted_outcome="likely_external_fallback",
                forecast_risk="high",
                confidence_stability="volatile",
                confidence_delta=-20,
            )
            for _ in range(3):
                build_runtime_authority(unstable, memory_path=memory_path)
            result = build_runtime_authority(unstable, memory_path=memory_path)

            self.assertTrue(result["fallback_authority"]["terminate_loops"])
            self.assertFalse(result["fallback_authority"]["fallback_allowed"])

    def test_constraint_enforcement_blocks_unstable_browser_path(self):
        result = build_runtime_authority(
            make_context(
                browser_risk="high",
                browser_safety_class="unsafe",
                memory_risk="high",
                mobile_runtime_risk="high",
                mobile_friendly=False,
            ),
            persist_memory=False,
        )

        constraint_names = {item["constraint"] for item in result["forced_constraints"]}
        self.assertIn("browser_capability_cap", constraint_names)
        self.assertIn("low_memory_runtime_restrictions", constraint_names)

    def test_confidence_governance_suppresses_overconfidence_under_risk(self):
        result = build_runtime_authority(
            make_context(
                degradation_risk=70,
                fallback_probability=0.64,
                prediction_confidence=92,
                predicted_outcome="likely_external_fallback",
                confidence_stability="volatile",
                confidence_delta=-12,
                forecast_risk="high",
            ),
            persist_memory=False,
        )

        governance = result["confidence_governance"]
        self.assertTrue(governance["suppressed"])
        self.assertIn("overconfidence_prevention", governance["governance_actions"])

    def test_risk_engine_escalates_to_hazard(self):
        result = build_runtime_authority(
            make_context(
                degradation_risk=88,
                runtime_confidence=42,
                coordination_confidence=44,
                fallback_probability=0.84,
                predicted_outcome="likely_external_fallback",
                confidence_stability="volatile",
                confidence_delta=-18,
                forecast_risk="high",
            ),
            persist_memory=False,
        )

        self.assertEqual(result["risk_state"], "orchestration_hazard")

    def test_authority_memory_persists(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            memory_path = Path(temp_dir) / "runtime_authority_memory.json"
            build_runtime_authority(make_context(), memory_path=memory_path)
            result = build_runtime_authority(make_context(playback_runtime="external_runtime"), memory_path=memory_path)
            stored = load_authority_memory(path=memory_path)

            self.assertTrue(memory_path.exists())
            self.assertEqual(result["authority_memory_summary"]["total_observations"], 2)
            self.assertEqual(stored["aggregates"]["total_runs"], 2)

    def test_prepare_playback_runtime_exposes_authority_in_session_payload(self):
        source = {
            "title": "Film 1080p",
            "magnet": "magnet:?xt=urn:btih:1234567890abcdef1234567890abcdef12345678",
            "quality_label": "1080p",
            "resolution": "1080p",
            "codec": "x264",
            "source_type": "WebDL",
            "browser_playable_candidate": True,
            "mobile_friendly": True,
            "high_bandwidth_required": False,
            "runtime_profile": "browser_balanced",
            "runtime_recommended": "browser_runtime",
            "source_fingerprint": "src-1",
        }

        plan = prepare_playback_runtime(movie={"title": "Film"}, selected_source=source, sources=[source])
        session_payload = dict(plan["session_payload"])

        self.assertIn("authority_state", session_payload)
        self.assertIn("runtime_risk", session_payload)
        self.assertIn("arbitration_trace", session_payload)
        self.assertIn("execution_policy", session_payload)


if __name__ == "__main__":
    unittest.main()
