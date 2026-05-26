import json
import tempfile
import unittest
from pathlib import Path

from domains.magnets.playback import prepare_playback_runtime
from domains.magnets.runtime_dreaming import build_runtime_dreaming, load_dreaming_memory


def make_dreaming_context(
    *,
    degradation_risk=24,
    runtime_resilience=82,
    continuity_confidence=78,
    prediction_confidence=76,
    cinematic_quality=84,
    subconscious_balance=82,
    latent_pattern="latent_cinematic_preservation",
    hidden_equilibrium="hidden_resilience",
    continuity_underlayers="cinematic_underlayers",
    residue_pattern="equilibrium_residue",
    dormant_resilience="dormant_resilient",
    cinematic_underflow="cinematic_underflow_stable",
    silent_adaptation="silent_resilience",
    stabilization_state="strong_stabilization",
    continuity_state="continuity_resilient",
    cinematic_instinct="cinematic_preserving",
    cinematic_direction="cinematic_stable",
    pressure_score=34,
    switch_frequency=0,
):
    return {
        "execution_metrics": {"degradation_risk": degradation_risk},
        "coordination_metrics": {"runtime_resilience": runtime_resilience},
        "continuity_state": {"continuity_confidence": continuity_confidence},
        "runtime_predictions": {"prediction_confidence": prediction_confidence},
        "resilience_topology": {"topology": "distributed_resilience"},
        "runtime_subconscious": {"subconscious_state": "persistent_orchestration_subconscious"},
        "latent_patterns": {"pattern": latent_pattern},
        "hidden_equilibrium": {"state": hidden_equilibrium},
        "continuity_underlayers": {"state": continuity_underlayers},
        "orchestration_residue": {"pattern": residue_pattern},
        "dormant_resilience": {"state": dormant_resilience},
        "cinematic_underflow": {"state": cinematic_underflow},
        "silent_adaptation": {"state": silent_adaptation},
        "stabilization_instinct": {"state": stabilization_state},
        "continuity_instinct": {"state": continuity_state},
        "cinematic_instinct": {"state": cinematic_instinct},
        "cinematic_direction": {"style": cinematic_direction},
        "subconscious_metrics": {"subconscious_balance": subconscious_balance},
        "cinematic_metrics": {"cinematic_quality": cinematic_quality},
        "orchestration_pressure": {"pressure_score": pressure_score},
        "adaptation_history": {"switch_frequency": switch_frequency},
    }


class RuntimeDreamingTests(unittest.TestCase):
    def test_cinematic_dreams_shape_immersive_projection(self):
        result = build_runtime_dreaming(make_dreaming_context(), persist_memory=False)

        self.assertEqual(result["cinematic_dreams"]["state"], "immersive_dream")

    def test_latent_projection_fragments_under_hidden_fragmentation(self):
        result = build_runtime_dreaming(
            make_dreaming_context(
                degradation_risk=84,
                prediction_confidence=40,
                latent_pattern="latent_fragmentation",
                hidden_equilibrium="hidden_fragmentation",
                residue_pattern="degradation_residue",
                cinematic_underflow="cinematic_underflow_fragile",
                pressure_score=78,
            ),
            persist_memory=False,
        )

        self.assertEqual(result["latent_projection"]["state"], "latent_fragmentation_projection")

    def test_dormant_pathways_follow_cinematic_path(self):
        result = build_runtime_dreaming(make_dreaming_context(), persist_memory=False)

        self.assertEqual(result["dormant_pathways"]["state"], "dormant_cinematic_path")

    def test_adaptive_dreaming_repairs_continuity_when_recovery_dominates(self):
        result = build_runtime_dreaming(
            make_dreaming_context(
                continuity_underlayers="fragmented_underlayers",
                continuity_state="continuity_recovering",
                silent_adaptation="silent_recovery",
                latent_pattern="latent_recovery",
                cinematic_underflow="cinematic_underflow_adaptive",
            ),
            persist_memory=False,
        )

        self.assertEqual(result["adaptive_dreaming"]["state"], "adaptive_continuity_repair")

    def test_runtime_mirroring_prefers_cinematic_mirroring(self):
        result = build_runtime_dreaming(make_dreaming_context(), persist_memory=False)

        self.assertEqual(result["runtime_mirroring"]["state"], "cinematic_mirroring")

    def test_continuity_dreaming_preserves_stable_projection(self):
        result = build_runtime_dreaming(make_dreaming_context(), persist_memory=False)

        self.assertEqual(result["continuity_dreams"]["state"], "continuity_preservation")

    def test_corruption_recovery_rebuilds_memory(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            memory_path = Path(temp_dir) / "runtime_dreaming_memory.json"
            memory_path.write_text("{bad json", encoding="utf-8")

            result = build_runtime_dreaming(make_dreaming_context(), memory_path=memory_path)
            recovered = json.loads(memory_path.read_text(encoding="utf-8"))

            self.assertEqual(result["dreaming_memory"]["memory_status"], "recovered")
            self.assertEqual(recovered["corrupted_recoveries"], 1)

    def test_deterministic_output_without_persistence(self):
        context = make_dreaming_context()

        first = build_runtime_dreaming(context, persist_memory=False)
        second = build_runtime_dreaming(context, persist_memory=False)

        self.assertEqual(first, second)

    def test_governance_shaping_targets_fragmentation_and_pathways(self):
        result = build_runtime_dreaming(
            make_dreaming_context(
                latent_pattern="latent_fragmentation",
                dormant_resilience="dormant_recovering",
                cinematic_underflow="cinematic_underflow_fragile",
                continuity_underlayers="fragmented_underlayers",
                pressure_score=80,
                cinematic_direction="cinematic_constrained",
            ),
            persist_memory=False,
        )

        self.assertIn("suppress_fragmentation_drift", result["dream_governance"]["governance_actions"])
        self.assertIn("stabilize_dormant_pathways", result["dream_governance"]["governance_actions"])
        self.assertIn("maintain_continuity_projection", result["dream_governance"]["governance_actions"])

    def test_memory_persistence_accumulates_runs(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            memory_path = Path(temp_dir) / "runtime_dreaming_memory.json"
            build_runtime_dreaming(make_dreaming_context(), memory_path=memory_path)
            result = build_runtime_dreaming(
                make_dreaming_context(
                    prediction_confidence=48,
                    cinematic_quality=58,
                    latent_pattern="latent_recovery",
                    cinematic_underflow="cinematic_underflow_adaptive",
                ),
                memory_path=memory_path,
            )
            stored = load_dreaming_memory(path=memory_path)

            self.assertEqual(result["dreaming_memory"]["total_observations"], 2)
            self.assertEqual(stored["aggregates"]["total_runs"], 2)

    def test_prepare_playback_runtime_exposes_dreaming_payloads(self):
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
            "source_fingerprint": "src-dreaming-plan",
        }

        plan = prepare_playback_runtime(movie={"title": "Film"}, selected_source=source, sources=[source])

        self.assertIn("runtime_dreaming", plan)
        self.assertIn("cinematic_dreams", plan["session_payload"])
        self.assertIn("dream_metrics", plan["readiness_snapshot"])
        self.assertIn("runtime_mirroring", plan["session_payload"])


if __name__ == "__main__":
    unittest.main()
