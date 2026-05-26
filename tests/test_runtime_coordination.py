import unittest

from domains.magnets.runtime_coordination import (
    assess_runtime_degradation,
    build_adaptive_runtime_strategy,
    build_coordination_metrics,
    build_orchestration_graph,
    coordinate_runtime,
    negotiate_runtime,
    plan_runtime_switch,
    validate_coordination_transition,
)


def make_capability(
    *,
    browser_safety_class="safe",
    startup_viability="viable",
    memory_risk="low",
    mobile_runtime_risk="low",
    browser_risk="low",
):
    return {
        "browser_safety_class": browser_safety_class,
        "startup_viability": startup_viability,
        "memory_risk": memory_risk,
        "mobile_runtime_risk": mobile_runtime_risk,
        "browser_risk": browser_risk,
    }


def make_metrics(*, startup_score=82, fallback_pressure=18, degradation_risk=24):
    return {
        "startup_score": startup_score,
        "stability_score": startup_score,
        "fallback_pressure": fallback_pressure,
        "degradation_risk": degradation_risk,
        "runtime_confidence": startup_score,
    }


def make_readiness(*, runtime_profile="browser_balanced", playback_runtime="browser_runtime", fallback_strategy="external_player_fallback"):
    return {
        "runtime_profile": runtime_profile,
        "playback_runtime": playback_runtime,
        "fallback_strategy": fallback_strategy,
        "runtime_mode": playback_runtime,
    }


class RuntimeCoordinationTests(unittest.TestCase):
    def test_negotiation_logic_prefers_browser_when_safe(self):
        negotiation = negotiate_runtime(
            capability_snapshot=make_capability(),
            execution_metrics=make_metrics(),
            readiness_snapshot=make_readiness(),
            runtime_pressure="low",
            degradation_risk=18,
        )
        self.assertEqual(negotiation["selected_runtime"], "browser_runtime")
        self.assertEqual(negotiation["fallback_runtime"], "external_runtime")

    def test_degraded_runtime_selection_when_browser_and_mobile_are_unstable(self):
        negotiation = negotiate_runtime(
            capability_snapshot=make_capability(browser_safety_class="unsafe", memory_risk="high", mobile_runtime_risk="high", browser_risk="high"),
            execution_metrics=make_metrics(startup_score=28, fallback_pressure=82, degradation_risk=88),
            readiness_snapshot=make_readiness(runtime_profile="browser_cinematic"),
            runtime_pressure="high",
            degradation_risk=88,
        )
        self.assertEqual(negotiation["selected_runtime"], "external_runtime")
        self.assertTrue(any(item["runtime"] == "browser_runtime" for item in negotiation["rejected_runtimes"]))

    def test_downgrade_selection_prefers_balancing_for_cinematic_pressure(self):
        degradation = assess_runtime_degradation(
            capability_snapshot=make_capability(startup_viability="fragile"),
            execution_metrics=make_metrics(degradation_risk=48),
            readiness_snapshot=make_readiness(runtime_profile="browser_cinematic"),
            runtime_pressure="medium",
        )
        strategy = build_adaptive_runtime_strategy(
            selected_runtime="cinematic_runtime",
            fallback_runtime="external_runtime",
            degradation_report=degradation,
            readiness_snapshot=make_readiness(runtime_profile="browser_cinematic"),
            execution_metrics=make_metrics(degradation_risk=48),
            runtime_pressure="medium",
        )
        self.assertEqual(strategy["adaptation_rule"], "downgrade_cinematic_to_balanced")
        self.assertEqual(strategy["target_runtime"], "browser_runtime")

    def test_invalid_coordination_transition_returns_rollback(self):
        validation = validate_coordination_transition("coordination_pending", "runtime_rebalanced")
        self.assertFalse(validation["allowed"])
        self.assertIn("coordination_pending", validation["rollback_path"])

    def test_runtime_switching_plans_browser_to_external(self):
        switch_plan = plan_runtime_switch(
            current_runtime="browser_runtime",
            target_runtime="external_runtime",
            switch_reason="browser_instability",
            degradation_report={"degradation_severity": 84},
            execution_metrics=make_metrics(fallback_pressure=76),
        )
        self.assertEqual(switch_plan["switch_strategy"], "browser_to_external_handoff")
        self.assertGreater(switch_plan["estimated_recovery"]["eta_ms"], 1600)

    def test_orchestration_graph_shapes_expected_nodes_and_paths(self):
        graph = build_orchestration_graph(
            selected_runtime="browser_runtime",
            fallback_runtime="external_runtime",
            adaptive_strategy={"target_runtime": "mobile_safe_runtime"},
            degradation_report={"degradation_severity": 58},
        )
        self.assertEqual(graph["fallback_runtime"], "external_runtime")
        self.assertTrue(any(node["id"] == "balanced_runtime" for node in graph["nodes"]))
        self.assertTrue(any(path["reason"] == "browser_instability" for path in graph["downgrade_paths"]))

    def test_fallback_escalation_and_resilience_scoring_are_explainable(self):
        coordination = coordinate_runtime(
            capability_snapshot=make_capability(memory_risk="high", browser_risk="high"),
            execution_metrics=make_metrics(startup_score=34, fallback_pressure=78, degradation_risk=74),
            readiness_snapshot=make_readiness(runtime_profile="browser_balanced"),
            runtime_pressure="high",
        )
        metrics = coordination["coordination_metrics"]
        self.assertIn(coordination["coordination_state"], {"fallback_negotiated", "runtime_rebalanced"})
        self.assertGreaterEqual(metrics["switching_cost"], 0)
        self.assertLessEqual(metrics["coordination_confidence"], 100)
        self.assertTrue(coordination["runtime_switch_history"])

    def test_metrics_grade_tracks_runtime_resilience(self):
        metrics = build_coordination_metrics(
            runtime_negotiation={"fallback_runtime": "external_runtime"},
            adaptive_strategy={"adaptation_pressure": 18},
            degradation_report={"degradation_severity": 20},
            switch_plan={"estimated_recovery": {"eta_ms": 1800}},
        )
        self.assertGreater(metrics["runtime_resilience"], 60)
        self.assertIn(metrics["runtime_orchestration_grade"], {"A", "B"})


if __name__ == "__main__":
    unittest.main()
