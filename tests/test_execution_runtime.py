import unittest

from domains.magnets.execution_runtime import (
    build_runtime_grade,
    can_transition_execution_state,
    classify_execution_transport,
    evolve_execution_state,
    failure_safe_downgrade_state,
    simulate_execution_runtime,
    summarize_execution_metrics,
    validate_execution_transition,
)


def make_capability(
    *,
    startup_viability="viable",
    browser_safety_class="safe",
    browser_codec_support_assumption="supported",
    memory_risk="low",
    mobile_runtime_risk="low",
    startup_timeout_estimate_seconds=12,
):
    return {
        "startup_viability": startup_viability,
        "browser_safety_class": browser_safety_class,
        "browser_codec_support_assumption": browser_codec_support_assumption,
        "memory_risk": memory_risk,
        "mobile_runtime_risk": mobile_runtime_risk,
        "startup_timeout_estimate_seconds": startup_timeout_estimate_seconds,
    }


def make_manifest(*, runtime_mode="browser_runtime", startup_confidence="high"):
    return {
        "runtime_mode": runtime_mode,
        "runtime_state": "runtime_ready",
        "startup_confidence": startup_confidence,
    }


def make_bootstrap(*, bootstrap_mode="browser_sandbox"):
    return {
        "bootstrap_mode": bootstrap_mode,
    }


class ExecutionRuntimeTests(unittest.TestCase):
    def test_state_transitions_allow_forward_progression(self):
        self.assertTrue(can_transition_execution_state("idle", "bootstrapping"))
        self.assertEqual(evolve_execution_state("startup_pending", "runtime_active"), "runtime_active")

    def test_invalid_transition_resolves_to_failure_safe_downgrade(self):
        validation = validate_execution_transition("runtime_active", "bootstrapping")
        self.assertFalse(validation["allowed"])
        self.assertEqual(validation["fallback_state"], "runtime_unstable")
        self.assertEqual(failure_safe_downgrade_state("runtime_active"), "runtime_unstable")
        self.assertEqual(evolve_execution_state("runtime_active", "bootstrapping"), "runtime_unstable")

    def test_transport_classification_prefers_browser_progressive_for_safe_runtime(self):
        descriptor = classify_execution_transport(
            runtime_manifest=make_manifest(),
            capability_snapshot=make_capability(),
            bootstrap_plan=make_bootstrap(),
            source_metadata={"mobile_friendly": True},
        )
        self.assertEqual(descriptor["transport_class"], "browser_progressive")
        self.assertEqual(descriptor["startup_behavior"], "progressive")

    def test_transport_classification_marks_mobile_limited(self):
        descriptor = classify_execution_transport(
            runtime_manifest=make_manifest(),
            capability_snapshot=make_capability(mobile_runtime_risk="high"),
            bootstrap_plan=make_bootstrap(),
            source_metadata={"mobile_friendly": False},
        )
        self.assertEqual(descriptor["transport_class"], "mobile_limited")
        self.assertEqual(descriptor["degradation_likelihood"], "high")

    def test_metrics_and_grade_are_explainable(self):
        metrics = summarize_execution_metrics(
            capability_snapshot=make_capability(startup_viability="fragile", browser_safety_class="limited"),
            playback_readiness="browser_deferred",
            transport_descriptor={"startup_behavior": "buffer_sensitive", "runtime_pressure": "high", "degradation_likelihood": "medium", "transport_class": "browser_heavy"},
            guardrails={"rejected": False},
        )
        grade = build_runtime_grade(metrics)
        self.assertLess(metrics["startup_score"], 60)
        self.assertLess(metrics["stability_score"], 70)
        self.assertIn(grade["grade"], {"C", "D", "F"})

    def test_simulator_produces_stable_completion_for_safe_runtime(self):
        result = simulate_execution_runtime(
            capability_snapshot=make_capability(),
            playback_readiness="browser_ready",
            source_metadata={"mobile_friendly": True},
            runtime_manifest=make_manifest(),
            bootstrap_plan=make_bootstrap(),
            readiness_snapshot={"startup_confidence": "high"},
        )
        self.assertEqual(result["simulated_runtime_health"], "stable")
        self.assertEqual(result["execution_state"], "runtime_completed")
        self.assertEqual(result["runtime_grade"]["grade"], "A")
        self.assertEqual(result["execution_events"][-1]["event_type"], "runtime_completed")

    def test_simulator_selects_recovery_for_degraded_runtime(self):
        result = simulate_execution_runtime(
            capability_snapshot=make_capability(startup_viability="fragile", memory_risk="high"),
            playback_readiness="browser_deferred",
            source_metadata={"mobile_friendly": True},
            runtime_manifest=make_manifest(startup_confidence="low"),
            bootstrap_plan=make_bootstrap(),
            readiness_snapshot={"startup_confidence": "low"},
        )
        self.assertEqual(result["simulated_runtime_health"], "degraded")
        self.assertEqual(result["recovery_path"]["path"], "degrade_quality")
        self.assertTrue(result["execution_failures"])
        self.assertIn(result["execution_events"][2]["event_type"], {"startup_degraded", "runtime_fallback_selected"})

    def test_browser_rejection_path_is_reported(self):
        result = simulate_execution_runtime(
            capability_snapshot=make_capability(browser_safety_class="unsafe", browser_codec_support_assumption="unsupported"),
            playback_readiness="external_recommended",
            source_metadata={"mobile_friendly": False},
            runtime_manifest=make_manifest(),
            bootstrap_plan=make_bootstrap(),
            readiness_snapshot={"startup_confidence": "high"},
        )
        self.assertTrue(result["guardrails"]["rejected"])
        self.assertIn("unsupported_codec_rejection", result["guardrails"]["blocking_reasons"])
        self.assertEqual(result["recovery_path"]["path"], "external_handoff")

    def test_execution_timeline_shapes_with_heavier_runtime(self):
        stable = simulate_execution_runtime(
            capability_snapshot=make_capability(),
            playback_readiness="browser_ready",
            source_metadata={"mobile_friendly": True},
            runtime_manifest=make_manifest(),
            bootstrap_plan=make_bootstrap(),
            readiness_snapshot={"startup_confidence": "high"},
        )
        heavy = simulate_execution_runtime(
            capability_snapshot=make_capability(startup_viability="fragile", memory_risk="high"),
            playback_readiness="browser_deferred",
            source_metadata={"mobile_friendly": True},
            runtime_manifest=make_manifest(startup_confidence="low"),
            bootstrap_plan=make_bootstrap(),
            readiness_snapshot={"startup_confidence": "low"},
        )
        self.assertGreater(heavy["execution_timeline"]["estimated_startup_ms"], stable["execution_timeline"]["estimated_startup_ms"])
        self.assertGreater(heavy["execution_timeline"]["fallback_probability"], stable["execution_timeline"]["fallback_probability"])


if __name__ == "__main__":
    unittest.main()
