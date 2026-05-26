import json
import tempfile
import unittest
from pathlib import Path

from domains.magnets.playback import prepare_playback_runtime
from domains.magnets.runtime_ecosystem import build_runtime_ecosystem
from domains.magnets.runtime_federation import build_runtime_federation
from domains.magnets.runtime_resonance import build_runtime_resonance
from domains.magnets.runtime_symbiosis import build_runtime_symbiosis, load_symbiosis_memory
from domains.magnets.runtime_temporal import build_runtime_temporal
from domains.magnets.services.session_store import StreamSessionStore
from domains.magnets.sessions import StreamSession


def make_symbiosis_context(
    *,
    federation_coherence=78,
    federation_alignment=76,
    federation_divergence=28,
    federation_resilience=74,
    federation_integrity=76,
    temporal_stability=70,
    temporal_alignment=72,
    temporal_pressure=40,
    temporal_integrity=72,
    temporal_recovery_score=68,
    temporal_recovery_velocity="adaptive",
    resonance_stability=66,
    resonance_alignment=68,
    resonance_pressure=46,
    resonance_fragmentation=30,
    resonance_cohesion=70,
    resonance_integrity=68,
    resonance_recovery_score=64,
    ecosystem_stability=62,
    ecosystem_integrity=60,
    ecosystem_pressure=58,
    ecosystem_degradation=64,
    ecosystem_current="localized_degradation",
):
    return {
        "runtime_federation": {"state": "federation_balancing"},
        "federation_metrics": {
            "federation_coherence": federation_coherence,
            "federation_alignment": federation_alignment,
            "federation_divergence": federation_divergence,
            "federation_resilience": federation_resilience,
            "federation_integrity": federation_integrity,
        },
        "federation_coherence": federation_coherence,
        "federation_alignment": federation_alignment,
        "federation_divergence": federation_divergence,
        "federation_resilience": federation_resilience,
        "federation_integrity": federation_integrity,
        "federation_continuity": {"continuity_projection": "measured_continuity"},
        "runtime_temporal": {"state": "temporal_balancing"},
        "temporal_metrics": {
            "temporal_stability": temporal_stability,
            "temporal_alignment": temporal_alignment,
            "temporal_pressure": temporal_pressure,
            "temporal_integrity": temporal_integrity,
        },
        "temporal_stability": temporal_stability,
        "temporal_alignment": temporal_alignment,
        "temporal_pressure": temporal_pressure,
        "temporal_integrity": temporal_integrity,
        "temporal_recovery": {
            "recovery_score": temporal_recovery_score,
            "adaptive_recovery_velocity": temporal_recovery_velocity,
        },
        "runtime_resonance": {"state": "resonance_balancing"},
        "resonance_metrics": {
            "resonance_stability": resonance_stability,
            "resonance_alignment": resonance_alignment,
            "resonance_pressure": resonance_pressure,
            "resonance_fragmentation": resonance_fragmentation,
            "resonance_cohesion": resonance_cohesion,
            "resonance_integrity": resonance_integrity,
        },
        "resonance_stability": resonance_stability,
        "resonance_alignment": resonance_alignment,
        "resonance_pressure": resonance_pressure,
        "resonance_fragmentation": resonance_fragmentation,
        "resonance_cohesion": resonance_cohesion,
        "resonance_integrity": resonance_integrity,
        "resonance_recovery": {
            "recovery_score": resonance_recovery_score,
        },
        "resonance_equilibrium": {
            "equilibrium_state": "adaptive_resonant_equilibrium",
        },
        "runtime_ecosystem": {"ecosystem_state": "self_balancing_runtime_ecosystem"},
        "ecosystem_metrics": {
            "ecosystem_stability": ecosystem_stability,
            "ecosystem_integrity": ecosystem_integrity,
            "orchestration_pressure_score": ecosystem_pressure,
            "degradation_risk": ecosystem_degradation,
        },
        "orchestration_pressure": {
            "pressure_score": ecosystem_pressure,
        },
        "degradation_currents": {
            "current": ecosystem_current,
        },
    }


class RuntimeSymbiosisTests(unittest.TestCase):
    def test_deterministic_symbiosis_synthesis(self):
        context = make_symbiosis_context()

        first = build_runtime_symbiosis(context, persist_memory=False)
        second = build_runtime_symbiosis(context, persist_memory=False)

        self.assertEqual(first, second)

    def test_dependency_stress_shaping(self):
        result = build_runtime_symbiosis(
            make_symbiosis_context(
                federation_divergence=74,
                temporal_pressure=72,
                resonance_pressure=70,
                resonance_fragmentation=64,
                ecosystem_pressure=76,
                ecosystem_degradation=78,
                temporal_recovery_velocity="guarded",
            ),
            persist_memory=False,
        )

        self.assertGreaterEqual(result["dependency_stress"], 68)
        self.assertEqual(result["symbiotic_phase"], "strained_mutualism")

    def test_cooperative_recovery_prefers_improving_contexts(self):
        result = build_runtime_symbiosis(
            make_symbiosis_context(
                federation_coherence=82,
                federation_resilience=80,
                temporal_recovery_score=78,
                temporal_recovery_velocity="improving",
                resonance_recovery_score=74,
                ecosystem_stability=68,
                ecosystem_integrity=64,
                ecosystem_pressure=54,
                ecosystem_degradation=70,
            ),
            persist_memory=False,
        )

        self.assertEqual(result["systemic_runtime_health"], "recovering")
        self.assertIn(result["recovery_cohesion"], {"stable", "recovering"})
        self.assertEqual(result["cooperative_runtime_state"], "adaptive_shared_recovery")

    def test_fragmentation_handling_detects_isolation(self):
        result = build_runtime_symbiosis(
            make_symbiosis_context(
                federation_divergence=84,
                federation_integrity=40,
                temporal_pressure=78,
                resonance_fragmentation=86,
                resonance_pressure=80,
                ecosystem_integrity=34,
                ecosystem_degradation=88,
                ecosystem_current="cascading_degradation",
            ),
            persist_memory=False,
        )

        self.assertGreaterEqual(result["symbiosis_fragmentation"], 68)
        self.assertEqual(result["symbiotic_phase"], "fractured_symbiosis")
        self.assertIn("stabilize_runtime_isolation", result["symbiosis_governance"]["governance_actions"])

    def test_corruption_recovery_rebuilds_memory(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            memory_path = Path(temp_dir) / "runtime_symbiosis_memory.json"
            memory_path.write_text("{bad json", encoding="utf-8")

            result = build_runtime_symbiosis(make_symbiosis_context(), memory_path=memory_path)
            recovered = json.loads(memory_path.read_text(encoding="utf-8"))

            self.assertEqual(result["symbiosis_memory_summary"]["memory_status"], "recovered")
            self.assertEqual(recovered["corrupted_recoveries"], 1)

    def test_symbiosis_memory_persistence_accumulates_runs(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            memory_path = Path(temp_dir) / "runtime_symbiosis_memory.json"
            build_runtime_symbiosis(make_symbiosis_context(), memory_path=memory_path)
            result = build_runtime_symbiosis(
                make_symbiosis_context(
                    resonance_fragmentation=54,
                    ecosystem_pressure=66,
                    ecosystem_degradation=72,
                ),
                memory_path=memory_path,
            )
            stored = load_symbiosis_memory(path=memory_path)

            self.assertEqual(result["symbiosis_memory_summary"]["total_observations"], 2)
            self.assertEqual(stored["aggregates"]["total_runs"], 2)

    def test_resonance_integration_consumes_real_resonance_outputs(self):
        orchestration = {
            "coordination_metrics": {"coordination_confidence": 76, "runtime_resilience": 72, "adaptation_pressure": 28},
            "runtime_negotiation": {"selected_runtime": "browser_runtime"},
            "authority_confidence": 70,
            "runtime_temperament": "adaptive_temperament",
            "identity_confidence": 72,
            "ecosystem_climate": {"climate": "stable_climate"},
            "runtime_atmosphere": {"atmosphere": "immersive_atmosphere"},
            "consciousness_metrics": {
                "orchestration_clarity": 76,
                "awareness_integrity": 74,
                "perception_integrity": 72,
            },
            "orchestration_focus": {"focus": "stable_focus"},
            "runtime_instinct": {"instinct_state": "balanced_instinct"},
            "instinct_metrics": {
                "instinct_integrity": 72,
                "survival_pressure": 42,
                "orchestration_survival_score": 74,
            },
            "runtime_subconscious": {"subconscious_state": "steady_subconscious"},
            "subconscious_metrics": {
                "subconscious_integrity": 72,
                "subconscious_balance": 70,
                "dormant_resilience_strength": 68,
            },
            "dream_forecast": {"forecast": "optimistic_recovery_projection"},
            "dream_metrics": {
                "dreaming_integrity": 74,
                "orchestration_dream_balance": 72,
                "continuity_projection_strength": 70,
                "runtime_mirroring_integrity": 68,
            },
            "orchestration_pressure": {"pressure_score": 42},
            "instinct_pressure": {"pressure_score": 40},
            "subconscious_pressure": {"latent_pressure": 36},
            "continuity_state": {"continuity_confidence": 70},
            "governance_actions": [],
            "forced_constraints": [],
            "blocked_paths": [],
            "execution_metrics": {"stability_score": 74, "degradation_risk": 46},
            "execution_timeline": {"fallback_probability": 0.18},
            "runtime_predictions": {"prediction_confidence": 72},
            "selected_source": {"high_bandwidth_required": False},
            "playback_runtime": "browser_runtime",
            "runtime_profile": "browser_cinematic",
            "startup_confidence": "high",
        }
        orchestration.update(build_runtime_ecosystem(orchestration, persist_memory=False))
        orchestration.update(build_runtime_federation(orchestration, persist_memory=False))
        orchestration.update(build_runtime_temporal(orchestration, persist_memory=False))
        orchestration.update(build_runtime_resonance(orchestration, persist_memory=False))

        result = build_runtime_symbiosis(orchestration, persist_memory=False)

        self.assertIn("runtime_symbiosis", result)
        self.assertTrue(result["symbiotic_phase"])
        self.assertIn(result["runtime_coexistence"], {"adaptive_runtime_coexistence", "stable_runtime_coexistence", "measured_runtime_coexistence"})

    def test_malformed_payload_degrades_without_crashing(self):
        result = build_runtime_symbiosis(
            {
                "federation_metrics": {"federation_coherence": "NaN"},
                "temporal_metrics": {"temporal_stability": "bad"},
                "resonance_metrics": {"resonance_fragmentation": "bad"},
                "ecosystem_metrics": {"ecosystem_integrity": "oops"},
                "temporal_recovery": "bad-payload",
            },
            persist_memory=False,
        )

        self.assertIn("runtime_symbiosis", result)
        self.assertIsInstance(result["symbiosis_events"], list)
        self.assertIsInstance(result["symbiosis_metrics"], dict)

    def test_playback_integration_exposes_symbiosis_payloads(self):
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
            "source_fingerprint": "src-symbiosis-plan",
        }

        plan = prepare_playback_runtime(movie={"title": "Film"}, selected_source=source, sources=[source])

        self.assertIn("runtime_symbiosis", plan)
        self.assertIn("symbiosis_metrics", plan["session_payload"])
        self.assertIn("symbiosis_events", plan["readiness_snapshot"])
        self.assertTrue(plan["symbiotic_phase"])

    def test_symbiosis_forecast_shaping_reflects_pressure(self):
        result = build_runtime_symbiosis(
            make_symbiosis_context(
                federation_divergence=72,
                temporal_pressure=74,
                resonance_fragmentation=58,
                resonance_pressure=72,
                ecosystem_pressure=80,
                ecosystem_degradation=76,
                temporal_recovery_velocity="improving",
            ),
            persist_memory=False,
        )

        self.assertEqual(result["symbiotic_phase"], "strained_mutualism")
        self.assertEqual(result["systemic_runtime_health"], "recovering")
        self.assertEqual(result["symbiosis_projection"]["forecast"], "cooperative_recovery_strengthening")

    def test_session_store_persists_symbiosis_fields(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = StreamSessionStore(path=Path(temp_dir) / "sessions.json")
            session = StreamSession(
                session_id="tmp789",
                movie_id="film-3",
                source_fingerprint="src-sym",
                handoff_mode="browser_handoff",
                preferred_runtime="browser_stream",
                session_state="prepared",
                runtime_symbiosis={"state": "symbiosis_balancing"},
                symbiosis_metrics={"symbiosis_stability": 61},
                symbiosis_events=[{"event_type": "symbiosis_synthesized"}],
                symbiosis_stability=61,
                dependency_stress=57,
                cooperative_runtime_state="adaptive_shared_recovery",
                systemic_runtime_health="recovering",
                symbiotic_phase="strained_mutualism",
                symbiosis_memory_summary={"total_observations": 2},
            )

            store.save_session(session)
            saved = store.get_session("tmp789")

            self.assertEqual(saved["runtime_symbiosis"]["state"], "symbiosis_balancing")
            self.assertEqual(saved["symbiosis_stability"], 61)
            self.assertEqual(saved["cooperative_runtime_state"], "adaptive_shared_recovery")


if __name__ == "__main__":
    unittest.main()
