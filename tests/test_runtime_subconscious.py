import json
import tempfile
import unittest
from pathlib import Path

from domains.magnets.playback import prepare_playback_runtime
from domains.magnets.runtime_subconscious import build_runtime_subconscious, load_subconscious_memory


def make_subconscious_context(
    *,
    playback_runtime="browser_runtime",
    runtime_profile="browser_cinematic",
    degradation_risk=24,
    runtime_resilience=82,
    adaptation_pressure=22,
    pressure_direction="steady",
    pressure_score=34,
    continuity_confidence=78,
    switch_frequency=0,
    drift_score=14,
    equilibrium_state="equilibrium_stable",
    balance_state="balanced_ecosystem",
    topology="distributed_resilience",
    instinct_integrity=82,
    awareness_integrity=80,
    fallback_intensity=28,
    cinematic_quality=84,
    cinematic_direction="cinematic_stable",
    stabilization_state="strong_stabilization",
    resilience_state="resilience_preserving",
    fallback_state="fallback_balanced",
    continuity_state="continuity_resilient",
    cinematic_state="cinematic_preserving",
    survival_state="survival_resilient",
):
    return {
        "playback_runtime": playback_runtime,
        "runtime_profile": runtime_profile,
        "execution_metrics": {
            "degradation_risk": degradation_risk,
        },
        "coordination_metrics": {
            "runtime_resilience": runtime_resilience,
            "adaptation_pressure": adaptation_pressure,
        },
        "orchestration_pressure": {
            "pressure_direction": pressure_direction,
            "pressure_score": pressure_score,
        },
        "adaptation_history": {
            "switch_frequency": switch_frequency,
        },
        "continuity_state": {
            "continuity_confidence": continuity_confidence,
        },
        "continuity_awareness": {
            "state": "resilient_awareness" if continuity_confidence >= 70 else "adaptive_awareness",
        },
        "resilience_topology": {
            "topology": topology,
        },
        "ecosystem_balance": {
            "balance_state": balance_state,
        },
        "adaptive_equilibrium": {
            "equilibrium_state": equilibrium_state,
        },
        "behavioral_drift": {
            "drift_score": drift_score,
        },
        "instinct_metrics": {
            "instinct_integrity": instinct_integrity,
            "fallback_intensity": fallback_intensity,
        },
        "consciousness_metrics": {
            "awareness_integrity": awareness_integrity,
        },
        "cinematic_metrics": {
            "cinematic_quality": cinematic_quality,
        },
        "runtime_memory_summary": {
            "total_observations": 2,
        },
        "instinct_memory": {
            "total_observations": 2,
        },
        "consciousness_memory": {
            "total_observations": 2,
        },
        "stabilization_instinct": {
            "state": stabilization_state,
        },
        "resilience_instinct": {
            "state": resilience_state,
        },
        "fallback_instinct": {
            "state": fallback_state,
        },
        "continuity_instinct": {
            "state": continuity_state,
        },
        "cinematic_instinct": {
            "state": cinematic_state,
        },
        "runtime_survival": {
            "state": survival_state,
        },
        "cinematic_direction": {
            "style": cinematic_direction,
        },
    }


class RuntimeSubconsciousTests(unittest.TestCase):
    def test_latent_pattern_shaping_prefers_cinematic_preservation(self):
        result = build_runtime_subconscious(make_subconscious_context(), persist_memory=False)

        self.assertEqual(result["latent_patterns"]["pattern"], "latent_cinematic_preservation")

    def test_subconscious_pressure_elevates_under_degradation(self):
        result = build_runtime_subconscious(
            make_subconscious_context(degradation_risk=82, pressure_score=74, continuity_confidence=40, cinematic_quality=54),
            persist_memory=False,
        )

        self.assertGreaterEqual(result["subconscious_pressure"]["degradation_pressure"], 60)

    def test_hidden_equilibrium_fragments_when_underflow_fragments(self):
        result = build_runtime_subconscious(
            make_subconscious_context(
                degradation_risk=84,
                pressure_score=76,
                fallback_intensity=82,
                equilibrium_state="equilibrium_fragmented",
                balance_state="degraded_ecosystem",
            ),
            persist_memory=False,
        )

        self.assertEqual(result["hidden_equilibrium"]["state"], "hidden_fragmentation")

    def test_dormant_resilience_recovery_tracks_survival(self):
        result = build_runtime_subconscious(
            make_subconscious_context(
                degradation_risk=72,
                runtime_resilience=58,
                survival_state="survival_recovering",
            ),
            persist_memory=False,
        )

        self.assertEqual(result["dormant_resilience"]["state"], "dormant_recovering")

    def test_orchestration_residue_tracks_fallback(self):
        result = build_runtime_subconscious(
            make_subconscious_context(
                degradation_risk=80,
                fallback_intensity=78,
                fallback_state="fallback_aggressive",
            ),
            persist_memory=False,
        )

        self.assertEqual(result["orchestration_residue"]["pattern"], "fallback_residue")

    def test_cinematic_underflow_stabilizes_for_strong_cinematic_runtime(self):
        result = build_runtime_subconscious(make_subconscious_context(), persist_memory=False)

        self.assertEqual(result["cinematic_underflow"]["state"], "cinematic_underflow_stable")

    def test_corruption_recovery_rebuilds_memory(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            memory_path = Path(temp_dir) / "runtime_subconscious_memory.json"
            memory_path.write_text("{bad json", encoding="utf-8")

            result = build_runtime_subconscious(make_subconscious_context(), memory_path=memory_path)
            recovered = json.loads(memory_path.read_text(encoding="utf-8"))

            self.assertEqual(result["subconscious_memory"]["memory_status"], "recovered")
            self.assertEqual(recovered["corrupted_recoveries"], 1)

    def test_deterministic_outputs_without_persistence(self):
        context = make_subconscious_context()

        first = build_runtime_subconscious(context, persist_memory=False)
        second = build_runtime_subconscious(context, persist_memory=False)

        self.assertEqual(first, second)

    def test_governance_shaping_targets_equilibrium_residue_and_underflow(self):
        result = build_runtime_subconscious(
            make_subconscious_context(
                degradation_risk=84,
                pressure_score=78,
                fallback_state="fallback_aggressive",
                fallback_intensity=80,
                equilibrium_state="equilibrium_fragmented",
                balance_state="degraded_ecosystem",
                cinematic_direction="cinematic_constrained",
            ),
            persist_memory=False,
        )

        self.assertIn("preserve_hidden_equilibrium", result["subconscious_governance"]["governance_actions"])
        self.assertIn("contain_orchestration_residue", result["subconscious_governance"]["governance_actions"])
        self.assertIn("stabilize_cinematic_underflow", result["subconscious_governance"]["governance_actions"])

    def test_memory_persistence_accumulates_runs(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            memory_path = Path(temp_dir) / "runtime_subconscious_memory.json"
            build_runtime_subconscious(make_subconscious_context(), memory_path=memory_path)
            result = build_runtime_subconscious(
                make_subconscious_context(
                    degradation_risk=70,
                    runtime_resilience=54,
                    pressure_score=64,
                    cinematic_quality=58,
                ),
                memory_path=memory_path,
            )
            stored = load_subconscious_memory(path=memory_path)

            self.assertEqual(result["subconscious_memory"]["total_observations"], 2)
            self.assertEqual(stored["aggregates"]["total_runs"], 2)

    def test_prepare_playback_runtime_exposes_subconscious_payloads(self):
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
            "source_fingerprint": "src-subconscious-plan",
        }

        plan = prepare_playback_runtime(movie={"title": "Film"}, selected_source=source, sources=[source])

        self.assertIn("runtime_subconscious", plan)
        self.assertIn("latent_patterns", plan["session_payload"])
        self.assertIn("subconscious_metrics", plan["readiness_snapshot"])
        self.assertIn("cinematic_underflow", plan["session_payload"])


if __name__ == "__main__":
    unittest.main()
