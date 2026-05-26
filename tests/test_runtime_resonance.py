import json
import tempfile
import unittest
from pathlib import Path

from domains.magnets.playback import prepare_playback_runtime
from domains.magnets.runtime_federation import build_runtime_federation
from domains.magnets.runtime_resonance import build_runtime_resonance, load_resonance_memory
from domains.magnets.runtime_temporal import build_runtime_temporal
from domains.magnets.services.session_store import StreamSessionStore
from domains.magnets.sessions import StreamSession


def make_resonance_context(
    *,
    temporal_stability=72,
    temporal_alignment=74,
    temporal_pressure=42,
    runtime_cycle_phase="stable_progression",
    temporal_flow="steady_cinematic_flow",
    recovery_velocity="adaptive",
    federation_coherence=78,
    federation_harmony=76,
    federation_alignment=74,
    federation_divergence=28,
    federation_pressure=40,
    federation_integrity=76,
    federation_resilience=72,
    orchestration_unity="moderate",
    runtime_phase_transition="steady_continuity",
    continuity_projection="measured_continuity",
    consciousness_clarity=76,
    awareness_integrity=74,
    cinematic_quality=80,
    immersion_depth=78,
    runtime_polish=76,
    instinct_integrity=70,
    survival_pressure=44,
    subconscious_integrity=72,
):
    return {
        "runtime_temporal": {
            "state": "temporal_balancing",
            "temporal_phase": runtime_cycle_phase,
            "temporal_rhythm": "measured_pacing",
            "temporal_forecast": "measured_future_shaping",
        },
        "temporal_metrics": {
            "temporal_stability": temporal_stability,
            "temporal_alignment": temporal_alignment,
            "temporal_pressure": temporal_pressure,
        },
        "temporal_stability": temporal_stability,
        "temporal_alignment": temporal_alignment,
        "temporal_pressure": temporal_pressure,
        "runtime_cycle_phase": runtime_cycle_phase,
        "cinematic_temporal_flow": temporal_flow,
        "temporal_recovery": {
            "adaptive_recovery_velocity": recovery_velocity,
        },
        "runtime_federation": {
            "state": "federation_balancing",
        },
        "federation_metrics": {
            "federation_coherence": federation_coherence,
            "federation_harmony": federation_harmony,
            "federation_alignment": federation_alignment,
            "federation_divergence": federation_divergence,
            "federation_pressure": federation_pressure,
            "federation_integrity": federation_integrity,
            "federation_resilience": federation_resilience,
        },
        "federation_coherence": federation_coherence,
        "federation_harmony": federation_harmony,
        "federation_alignment": federation_alignment,
        "federation_divergence": federation_divergence,
        "federation_pressure": federation_pressure,
        "federation_integrity": federation_integrity,
        "federation_resilience": federation_resilience,
        "orchestration_unity": orchestration_unity,
        "runtime_phase_transition": runtime_phase_transition,
        "continuity_projection": continuity_projection,
        "consciousness_metrics": {
            "orchestration_clarity": consciousness_clarity,
            "awareness_integrity": awareness_integrity,
        },
        "cinematic_metrics": {
            "cinematic_quality": cinematic_quality,
            "immersion_depth": immersion_depth,
            "runtime_polish": runtime_polish,
        },
        "instinct_metrics": {
            "instinct_integrity": instinct_integrity,
            "survival_pressure": survival_pressure,
        },
        "subconscious_metrics": {
            "subconscious_integrity": subconscious_integrity,
        },
    }


class RuntimeResonanceTests(unittest.TestCase):
    def test_deterministic_resonance_synthesis(self):
        context = make_resonance_context()

        first = build_runtime_resonance(context, persist_memory=False)
        second = build_runtime_resonance(context, persist_memory=False)

        self.assertEqual(first, second)

    def test_synchronization_shaping_reflects_balanced_context(self):
        result = build_runtime_resonance(
            make_resonance_context(
                temporal_stability=80,
                temporal_alignment=82,
                federation_coherence=84,
                federation_alignment=82,
                cinematic_quality=86,
                immersion_depth=84,
                survival_pressure=32,
            ),
            persist_memory=False,
        )

        self.assertIn(result["resonance_sync"]["sync_state"], {"synchronized", "pressured"})
        self.assertGreaterEqual(result["runtime_harmony_index"], 60)

    def test_fragmentation_handling_detects_interference(self):
        result = build_runtime_resonance(
            make_resonance_context(
                temporal_stability=36,
                temporal_alignment=40,
                temporal_pressure=82,
                federation_divergence=74,
                federation_pressure=78,
                consciousness_clarity=42,
                survival_pressure=84,
                subconscious_integrity=34,
                cinematic_quality=58,
                immersion_depth=54,
            ),
            persist_memory=False,
        )

        self.assertGreaterEqual(result["resonance_fragmentation"], 55)
        self.assertIn(result["resonance_phase"], {"strained_harmony", "fractured_resonance"})

    def test_resonance_recovery_prefers_recoverable_contexts(self):
        result = build_runtime_resonance(
            make_resonance_context(
                temporal_stability=64,
                temporal_alignment=68,
                temporal_pressure=58,
                federation_divergence=48,
                survival_pressure=52,
                recovery_velocity="strong",
            ),
            persist_memory=False,
        )

        self.assertIn(result["resonance_recovery"]["adaptive_sync_recovery"], {"adaptive", "strong"})
        self.assertGreaterEqual(result["adaptive_sync_balance"], 0)

    def test_corruption_recovery_rebuilds_memory(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            memory_path = Path(temp_dir) / "runtime_resonance_memory.json"
            memory_path.write_text("{bad json", encoding="utf-8")

            result = build_runtime_resonance(make_resonance_context(), memory_path=memory_path)
            recovered = json.loads(memory_path.read_text(encoding="utf-8"))

            self.assertEqual(result["resonance_memory_summary"]["memory_status"], "recovered")
            self.assertEqual(recovered["corrupted_recoveries"], 1)

    def test_resonance_memory_persistence_accumulates_runs(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            memory_path = Path(temp_dir) / "runtime_resonance_memory.json"
            build_runtime_resonance(make_resonance_context(), memory_path=memory_path)
            result = build_runtime_resonance(
                make_resonance_context(
                    temporal_pressure=68,
                    federation_divergence=56,
                    survival_pressure=64,
                ),
                memory_path=memory_path,
            )
            stored = load_resonance_memory(path=memory_path)

            self.assertEqual(result["resonance_memory_summary"]["total_observations"], 2)
            self.assertEqual(stored["aggregates"]["total_runs"], 2)

    def test_temporal_integration_consumes_temporal_outputs(self):
        orchestration = {
            **make_resonance_context(),
            **build_runtime_federation(
                {
                    "coordination_metrics": {"coordination_confidence": 76, "runtime_resilience": 72},
                    "runtime_negotiation": {"selected_runtime": "browser_runtime"},
                    "authority_confidence": 70,
                    "runtime_temperament": "adaptive_temperament",
                    "identity_confidence": 72,
                    "ecosystem_metrics": {"ecosystem_integrity": 74, "ecosystem_stability": 72},
                    "ecosystem_climate": {"climate": "stable_climate"},
                    "cinematic_metrics": {"cinematic_quality": 80, "runtime_polish": 78},
                    "runtime_atmosphere": {"atmosphere": "immersive_atmosphere"},
                    "consciousness_metrics": {
                        "orchestration_clarity": 76,
                        "awareness_integrity": 74,
                        "perception_integrity": 72,
                    },
                    "orchestration_focus": {"focus": "stable_focus"},
                    "runtime_instinct": {"instinct_state": "balanced_instinct"},
                    "instinct_metrics": {"instinct_integrity": 72, "orchestration_survival_score": 74},
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
                },
                persist_memory=False,
            ),
        }
        orchestration.update(build_runtime_temporal(orchestration, persist_memory=False))

        result = build_runtime_resonance(orchestration, persist_memory=False)

        self.assertIn("runtime_resonance", result)
        self.assertTrue(result["resonance_phase"])
        self.assertIn(result["harmonic_runtime_state"], {"stable_resonant_equilibrium", "adaptive_resonant_equilibrium", "unstable_resonant_equilibrium"})

    def test_malformed_payload_degrades_without_crashing(self):
        result = build_runtime_resonance(
            {
                "runtime_temporal": "bad-payload",
                "temporal_metrics": {"temporal_stability": "NaN"},
                "federation_metrics": {"federation_coherence": "NaN"},
                "consciousness_metrics": {"orchestration_clarity": "bad"},
                "cinematic_metrics": {"cinematic_quality": "bad"},
                "instinct_metrics": {"survival_pressure": "bad"},
            },
            persist_memory=False,
        )

        self.assertIn("runtime_resonance", result)
        self.assertIsInstance(result["resonance_events"], list)
        self.assertIsInstance(result["resonance_metrics"], dict)

    def test_playback_integration_exposes_resonance_payloads(self):
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
            "source_fingerprint": "src-resonance-plan",
        }

        plan = prepare_playback_runtime(movie={"title": "Film"}, selected_source=source, sources=[source])

        self.assertIn("runtime_resonance", plan)
        self.assertIn("resonance_metrics", plan["session_payload"])
        self.assertIn("resonance_events", plan["readiness_snapshot"])
        self.assertTrue(plan["resonance_phase"])

    def test_resonance_forecast_shaping_reflects_drift(self):
        result = build_runtime_resonance(
            make_resonance_context(
                temporal_stability=38,
                temporal_alignment=42,
                temporal_pressure=84,
                federation_divergence=80,
                survival_pressure=80,
                subconscious_integrity=30,
                recovery_velocity="guarded",
            ),
            persist_memory=False,
        )

        self.assertEqual(result["resonance_projection"]["forecast"], "fragmentation_risk_rising")
        self.assertIn("stabilize_sync_drift", result["resonance_governance"]["governance_actions"])

    def test_session_store_persists_resonance_fields(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = StreamSessionStore(path=Path(temp_dir) / "sessions.json")
            session = StreamSession(
                session_id="tmp456",
                movie_id="film-2",
                source_fingerprint="src-res",
                handoff_mode="browser_handoff",
                preferred_runtime="browser_stream",
                session_state="prepared",
                runtime_resonance={"state": "resonance_balancing"},
                resonance_metrics={"resonance_stability": 66},
                resonance_events=[{"event_type": "resonance_synthesized"}],
                resonance_stability=66,
                resonance_pressure=48,
                resonance_fragmentation=34,
                harmonic_runtime_state="adaptive_resonant_equilibrium",
                orchestration_resonance="moderate",
                resonance_phase="adaptive_equilibrium",
                sync_drift=38,
                resonance_memory_summary={"total_observations": 2},
            )

            store.save_session(session)
            saved = store.get_session("tmp456")

            self.assertEqual(saved["runtime_resonance"]["state"], "resonance_balancing")
            self.assertEqual(saved["resonance_stability"], 66)
            self.assertEqual(saved["harmonic_runtime_state"], "adaptive_resonant_equilibrium")


if __name__ == "__main__":
    unittest.main()
