import json
import tempfile
import unittest
from pathlib import Path

from domains.magnets.playback import prepare_playback_runtime
from domains.magnets.runtime_instinct import build_runtime_instinct, load_instinct_memory


def make_instinct_context(
    *,
    playback_runtime="browser_runtime",
    runtime_profile="browser_cinematic",
    startup_confidence="high",
    degradation_risk=24,
    stability_score=84,
    fallback_probability=0.12,
    runtime_resilience=82,
    adaptation_pressure=22,
    pressure_direction="steady",
    pressure_score=34,
    continuity_state="stable",
    continuity_confidence=78,
    continuity_awareness="resilient_awareness",
    continuity_cinema="adaptive_continuity",
    drift_score=14,
    switch_frequency=0,
    authority_state="approved",
    identity_trait="adaptive_balanced",
    awareness_state="cinematic_awareness",
    awareness_integrity=82,
    cinematic_quality=84,
    cinematic_direction="cinematic_stable",
    immersion_state="fully_immersive",
    ecosystem_balance="balanced_ecosystem",
    equilibrium_state="equilibrium_stable",
    resilience_topology="distributed_resilience",
    fallback_strategy="external_player_fallback",
):
    return {
        "playback_runtime": playback_runtime,
        "runtime_profile": runtime_profile,
        "startup_confidence": startup_confidence,
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
        "continuity_state": {
            "continuity_state": continuity_state,
            "continuity_confidence": continuity_confidence,
        },
        "continuity_awareness": {
            "state": continuity_awareness,
        },
        "continuity_cinema": {
            "continuity": continuity_cinema,
        },
        "behavioral_drift": {
            "drift_score": drift_score,
        },
        "adaptation_history": {
            "switch_frequency": switch_frequency,
        },
        "authority_state": authority_state,
        "runtime_identity": {
            "primary_trait": identity_trait,
        },
        "awareness_state": {
            "state": awareness_state,
        },
        "consciousness_metrics": {
            "awareness_integrity": awareness_integrity,
        },
        "cinematic_direction": {
            "style": cinematic_direction,
        },
        "cinematic_metrics": {
            "cinematic_quality": cinematic_quality,
        },
        "immersion_state": {
            "state": immersion_state,
        },
        "ecosystem_balance": {
            "balance_state": ecosystem_balance,
        },
        "adaptive_equilibrium": {
            "equilibrium_state": equilibrium_state,
        },
        "resilience_topology": {
            "topology": resilience_topology,
        },
        "fallback_strategy": fallback_strategy,
        "runtime_preflight": {
            "fallback_strategy": fallback_strategy,
        },
    }


class RuntimeInstinctTests(unittest.TestCase):
    def test_stabilization_shaping_prefers_strong_stabilization(self):
        result = build_runtime_instinct(make_instinct_context(), persist_memory=False)

        self.assertEqual(result["stabilization_instinct"]["state"], "strong_stabilization")

    def test_fallback_instinct_evolves_under_recovery_pressure(self):
        result = build_runtime_instinct(
            make_instinct_context(
                startup_confidence="low",
                degradation_risk=82,
                fallback_probability=0.76,
                fallback_strategy="recovery_fallback",
            ),
            persist_memory=False,
        )

        self.assertEqual(result["fallback_instinct"]["state"], "fallback_recovery")

    def test_continuity_instinct_fragments_with_switch_drift(self):
        result = build_runtime_instinct(
            make_instinct_context(
                continuity_state="fragmented",
                continuity_confidence=42,
                switch_frequency=4,
                drift_score=68,
                continuity_awareness="adaptive_awareness",
            ),
            persist_memory=False,
        )

        self.assertEqual(result["continuity_instinct"]["state"], "continuity_fragmented")

    def test_cinematic_preservation_remains_high_for_stable_cinematic_runtime(self):
        result = build_runtime_instinct(make_instinct_context(), persist_memory=False)

        self.assertEqual(result["cinematic_instinct"]["state"], "cinematic_preserving")

    def test_orchestration_reflexes_map_recovery_actions(self):
        result = build_runtime_instinct(
            make_instinct_context(
                degradation_risk=84,
                stability_score=34,
                fallback_probability=0.82,
                continuity_state="fragmented",
                continuity_confidence=38,
                switch_frequency=5,
                drift_score=72,
            ),
            persist_memory=False,
        )

        self.assertEqual(result["orchestration_reflexes"]["stabilization_reflex"], "stabilize_orchestration")
        self.assertEqual(result["orchestration_reflexes"]["fallback_reflex"], "escalate_fallback")
        self.assertEqual(result["orchestration_reflexes"]["continuity_reflex"], "restore_continuity")

    def test_survival_shaping_tracks_fragility(self):
        result = build_runtime_instinct(
            make_instinct_context(
                degradation_risk=86,
                stability_score=32,
                runtime_resilience=38,
                continuity_confidence=36,
                fallback_probability=0.8,
            ),
            persist_memory=False,
        )

        self.assertEqual(result["runtime_survival"]["state"], "survival_fragile")

    def test_corruption_recovery_rebuilds_memory(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            memory_path = Path(temp_dir) / "runtime_instinct_memory.json"
            memory_path.write_text("{bad json", encoding="utf-8")

            result = build_runtime_instinct(make_instinct_context(), memory_path=memory_path)
            recovered = json.loads(memory_path.read_text(encoding="utf-8"))

            self.assertEqual(result["instinct_memory"]["memory_status"], "recovered")
            self.assertEqual(recovered["corrupted_recoveries"], 1)

    def test_deterministic_outputs_without_persistence(self):
        context = make_instinct_context()

        first = build_runtime_instinct(context, persist_memory=False)
        second = build_runtime_instinct(context, persist_memory=False)

        self.assertEqual(first, second)

    def test_governance_shaping_targets_stabilization_and_continuity(self):
        result = build_runtime_instinct(
            make_instinct_context(
                degradation_risk=80,
                stability_score=36,
                fallback_probability=0.72,
                continuity_state="fragmented",
                continuity_confidence=34,
                runtime_resilience=42,
            ),
            persist_memory=False,
        )

        self.assertIn("preserve_continuity_instinct", result["instinct_governance"]["governance_actions"])
        self.assertIn("stabilize_orchestration_reflexes", result["instinct_governance"]["governance_actions"])
        self.assertIn("suppress_degradation_reflexes", result["instinct_governance"]["governance_actions"])

    def test_memory_persistence_accumulates_runs(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            memory_path = Path(temp_dir) / "runtime_instinct_memory.json"
            build_runtime_instinct(make_instinct_context(), memory_path=memory_path)
            result = build_runtime_instinct(
                make_instinct_context(
                    playback_runtime="external_runtime",
                    runtime_profile="external_player_only",
                    startup_confidence="low",
                    degradation_risk=74,
                    stability_score=44,
                    fallback_probability=0.66,
                    runtime_resilience=48,
                ),
                memory_path=memory_path,
            )
            stored = load_instinct_memory(path=memory_path)

            self.assertEqual(result["instinct_memory"]["total_observations"], 2)
            self.assertEqual(stored["aggregates"]["total_runs"], 2)

    def test_prepare_playback_runtime_exposes_instinct_payloads(self):
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
            "source_fingerprint": "src-instinct-plan",
        }

        plan = prepare_playback_runtime(movie={"title": "Film"}, selected_source=source, sources=[source])

        self.assertIn("runtime_instinct", plan)
        self.assertIn("stabilization_instinct", plan["session_payload"])
        self.assertIn("instinct_metrics", plan["readiness_snapshot"])
        self.assertIn("runtime_survival", plan["session_payload"])


if __name__ == "__main__":
    unittest.main()
