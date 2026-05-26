import json
import tempfile
import unittest
from pathlib import Path

from domains.magnets.playback import prepare_playback_runtime
from domains.magnets.runtime_consciousness import build_runtime_consciousness, load_consciousness_memory


def make_consciousness_context(
    *,
    playback_runtime="browser_runtime",
    runtime_profile="browser_cinematic",
    degradation_risk=22,
    stability_score=84,
    runtime_resilience=82,
    adaptation_pressure=18,
    continuity_state="stable",
    continuity_confidence=78,
    drift_score=12,
    switch_frequency=0,
    pressure_direction="steady",
    pressure_score=34,
    identity_confidence=76,
    cinematic_quality=84,
    cinematic_direction="cinematic_stable",
    ecosystem_balance_state="balanced_ecosystem",
):
    return {
        "playback_runtime": playback_runtime,
        "runtime_profile": runtime_profile,
        "execution_metrics": {
            "degradation_risk": degradation_risk,
            "stability_score": stability_score,
        },
        "coordination_metrics": {
            "runtime_resilience": runtime_resilience,
            "adaptation_pressure": adaptation_pressure,
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
        "orchestration_pressure": {
            "pressure_direction": pressure_direction,
            "pressure_score": pressure_score,
        },
        "identity_confidence": identity_confidence,
        "cinematic_direction": {
            "style": cinematic_direction,
        },
        "cinematic_metrics": {
            "cinematic_quality": cinematic_quality,
        },
        "ecosystem_balance": {
            "balance_state": ecosystem_balance_state,
        },
    }


class RuntimeConsciousnessTests(unittest.TestCase):
    def test_awareness_shaping_prefers_cinematic_awareness(self):
        result = build_runtime_consciousness(make_consciousness_context(), persist_memory=False)

        self.assertEqual(result["awareness_state"]["state"], "cinematic_awareness")

    def test_focus_evolution_tracks_degradation_pressure(self):
        result = build_runtime_consciousness(
            make_consciousness_context(
                degradation_risk=82,
                pressure_score=79,
                adaptation_pressure=72,
                runtime_resilience=46,
                cinematic_quality=52,
            ),
            persist_memory=False,
        )

        self.assertEqual(result["orchestration_focus"]["focus"], "degradation_focus")

    def test_cognitive_balance_fragments_under_pressure(self):
        result = build_runtime_consciousness(
            make_consciousness_context(
                degradation_risk=86,
                pressure_score=84,
                continuity_confidence=34,
                runtime_resilience=38,
                adaptation_pressure=74,
            ),
            persist_memory=False,
        )

        self.assertEqual(result["cognitive_balance"]["state"], "fragmented_cognition")

    def test_continuity_awareness_recovers_with_resilient_signals(self):
        result = build_runtime_consciousness(
            make_consciousness_context(
                continuity_state="resilient",
                continuity_confidence=72,
                switch_frequency=1,
                drift_score=16,
            ),
            persist_memory=False,
        )

        self.assertEqual(result["continuity_awareness"]["state"], "resilient_awareness")

    def test_reflection_behavior_degrades_under_high_risk(self):
        result = build_runtime_consciousness(
            make_consciousness_context(
                degradation_risk=88,
                runtime_resilience=40,
                adaptation_pressure=70,
                cinematic_direction="fallback_constrained",
            ),
            persist_memory=False,
        )

        self.assertEqual(result["runtime_reflection"]["state"], "degraded_reflection")

    def test_orchestration_intuition_prefers_resilience_path(self):
        result = build_runtime_consciousness(
            make_consciousness_context(
                runtime_resilience=86,
                degradation_risk=24,
                pressure_direction="steady",
                cinematic_direction="cinematic_stable",
            ),
            persist_memory=False,
        )

        self.assertEqual(result["orchestration_intuition"]["state"], "resilience_intuition")

    def test_perception_shaping_fragments_with_degraded_signals(self):
        result = build_runtime_consciousness(
            make_consciousness_context(
                degradation_risk=80,
                continuity_confidence=36,
                runtime_resilience=42,
                adaptation_pressure=66,
            ),
            persist_memory=False,
        )

        self.assertEqual(result["orchestration_perception"]["state"], "fragmented_perception")

    def test_corruption_recovery_rebuilds_memory(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            memory_path = Path(temp_dir) / "runtime_consciousness_memory.json"
            memory_path.write_text("{bad json", encoding="utf-8")

            result = build_runtime_consciousness(make_consciousness_context(), memory_path=memory_path)
            recovered = json.loads(memory_path.read_text(encoding="utf-8"))

            self.assertEqual(result["consciousness_memory"]["memory_status"], "recovered")
            self.assertEqual(recovered["corrupted_recoveries"], 1)

    def test_deterministic_outputs_without_persistence(self):
        context = make_consciousness_context()

        first = build_runtime_consciousness(context, persist_memory=False)
        second = build_runtime_consciousness(context, persist_memory=False)

        self.assertEqual(first, second)

    def test_governance_shaping_targets_fragmentation(self):
        result = build_runtime_consciousness(
            make_consciousness_context(
                degradation_risk=84,
                pressure_score=82,
                continuity_confidence=32,
                runtime_resilience=34,
                adaptation_pressure=78,
            ),
            persist_memory=False,
        )

        self.assertIn("preserve_continuity_awareness", result["consciousness_governance"]["governance_actions"])
        self.assertIn("stabilize_cognition", result["consciousness_governance"]["governance_actions"])
        self.assertIn("suppress_fragmentation", result["consciousness_governance"]["governance_actions"])

    def test_memory_persistence_accumulates_runs(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            memory_path = Path(temp_dir) / "runtime_consciousness_memory.json"
            build_runtime_consciousness(make_consciousness_context(), memory_path=memory_path)
            result = build_runtime_consciousness(
                make_consciousness_context(
                    playback_runtime="external_runtime",
                    runtime_profile="external_player_only",
                    degradation_risk=72,
                    pressure_score=66,
                    continuity_confidence=48,
                    cinematic_quality=50,
                ),
                memory_path=memory_path,
            )
            stored = load_consciousness_memory(path=memory_path)

            self.assertEqual(result["consciousness_memory"]["total_observations"], 2)
            self.assertEqual(stored["aggregates"]["total_runs"], 2)

    def test_prepare_playback_runtime_exposes_consciousness_payloads(self):
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
            "source_fingerprint": "src-consciousness-plan",
        }

        plan = prepare_playback_runtime(movie={"title": "Film"}, selected_source=source, sources=[source])

        self.assertIn("runtime_consciousness", plan)
        self.assertIn("awareness_state", plan["session_payload"])
        self.assertIn("consciousness_metrics", plan["readiness_snapshot"])
        self.assertIn("runtime_presence", plan["session_payload"])


if __name__ == "__main__":
    unittest.main()
