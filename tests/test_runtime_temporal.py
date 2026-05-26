import json
import tempfile
import unittest
from pathlib import Path

from domains.magnets.playback import prepare_playback_runtime
from domains.magnets.runtime_temporal import build_runtime_temporal, load_temporal_memory
from domains.magnets.services.session_store import StreamSessionStore
from domains.magnets.sessions import StreamSession


def make_temporal_context(
    *,
    federation_pressure=44,
    federation_alignment=76,
    federation_integrity=78,
    federation_resilience=74,
    federation_divergence=30,
    subconscious_state="persistent_orchestration_subconscious",
    subconscious_integrity=72,
    subconscious_balance=70,
    consciousness_focus="equilibrium_focus",
    consciousness_clarity=78,
    awareness_integrity=76,
    continuity_awareness="stable_continuity_awareness",
    dream_forecast="optimistic_projection",
    dreaming_integrity=74,
    dream_balance=70,
    continuity_projection_strength=72,
    cinematic_runtime_state="adaptive_cinematic_balance",
    continuity_projection="measured_continuity",
    federation_state="federation_convergent",
):
    return {
        "runtime_federation": {
            "state": federation_state,
            "continuity_projection": continuity_projection,
            "cinematic_runtime_state": cinematic_runtime_state,
        },
        "federation_projection": {
            "continuity_projection": continuity_projection,
            "cinematic_runtime_state": cinematic_runtime_state,
        },
        "federation_continuity": {
            "continuity_projection": continuity_projection,
        },
        "federation_metrics": {
            "federation_coherence": 0,
            "federation_harmony": 0,
            "federation_pressure": federation_pressure,
            "federation_integrity": federation_integrity,
            "federation_resilience": federation_resilience,
            "federation_alignment": federation_alignment,
            "federation_divergence": federation_divergence,
        },
        "federation_pressure": federation_pressure,
        "federation_alignment": federation_alignment,
        "federation_integrity": federation_integrity,
        "federation_resilience": federation_resilience,
        "federation_divergence": federation_divergence,
        "runtime_subconscious": {
            "subconscious_state": subconscious_state,
        },
        "subconscious_metrics": {
            "subconscious_integrity": subconscious_integrity,
            "subconscious_balance": subconscious_balance,
        },
        "orchestration_focus": {
            "focus": consciousness_focus,
        },
        "consciousness_metrics": {
            "orchestration_clarity": consciousness_clarity,
            "awareness_integrity": awareness_integrity,
        },
        "continuity_awareness": {
            "state": continuity_awareness,
        },
        "runtime_dreaming": {
            "projection_anchor": "latent_stability_projection",
        },
        "dream_forecast": {
            "forecast": dream_forecast,
        },
        "dream_metrics": {
            "dreaming_integrity": dreaming_integrity,
            "orchestration_dream_balance": dream_balance,
            "continuity_projection_strength": continuity_projection_strength,
        },
    }


class RuntimeTemporalTests(unittest.TestCase):
    def test_deterministic_temporal_synthesis(self):
        context = make_temporal_context()

        first = build_runtime_temporal(context, persist_memory=False)
        second = build_runtime_temporal(context, persist_memory=False)

        self.assertEqual(first, second)

    def test_temporal_decay_shaping_reflects_instability(self):
        result = build_runtime_temporal(
            make_temporal_context(
                federation_pressure=88,
                federation_divergence=72,
                subconscious_state="unstable_continuity_subconscious",
                subconscious_integrity=38,
                subconscious_balance=42,
                continuity_awareness="fragmented_continuity_awareness",
                consciousness_focus="high_focus",
                consciousness_clarity=84,
                dream_forecast="optimistic_recovery_projection",
                dreaming_integrity=76,
            ),
            persist_memory=False,
        )

        self.assertEqual(result["runtime_cycle_phase"], "adaptive_transition")
        self.assertEqual(result["cinematic_temporal_flow"], "unstable_but_recovering")
        self.assertEqual(result["orchestration_phase_velocity"], "accelerating")
        self.assertGreaterEqual(result["continuity_decay_rate"], 56)

    def test_momentum_evolution_prefers_resilient_contexts(self):
        restrained = build_runtime_temporal(
            make_temporal_context(
                federation_resilience=40,
                consciousness_clarity=48,
                dreaming_integrity=46,
                continuity_projection_strength=42,
            ),
            persist_memory=False,
        )
        progressive = build_runtime_temporal(
            make_temporal_context(
                federation_resilience=86,
                consciousness_clarity=84,
                dreaming_integrity=82,
                continuity_projection_strength=80,
            ),
            persist_memory=False,
        )

        self.assertGreater(progressive["temporal_momentum"], restrained["temporal_momentum"])

    def test_recovery_balancing_shapes_velocity(self):
        result = build_runtime_temporal(
            make_temporal_context(
                federation_pressure=74,
                federation_resilience=82,
                consciousness_clarity=80,
                dreaming_integrity=78,
                dream_forecast="recovery_projection",
            ),
            persist_memory=False,
        )

        self.assertIn(result["temporal_recovery"]["adaptive_recovery_velocity"], {"adaptive", "strong"})
        self.assertGreaterEqual(result["adaptive_temporal_balance"], 0)

    def test_continuity_persistence_uses_prior_memory(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            memory_path = Path(temp_dir) / "runtime_temporal_memory.json"
            first = build_runtime_temporal(
                make_temporal_context(
                    federation_alignment=88,
                    federation_integrity=84,
                    continuity_awareness="stable_continuity_awareness",
                ),
                memory_path=memory_path,
            )
            second = build_runtime_temporal(
                make_temporal_context(
                    federation_alignment=80,
                    federation_integrity=76,
                    continuity_awareness="stable_continuity_awareness",
                ),
                memory_path=memory_path,
            )

            self.assertEqual(first["temporal_continuity"]["state"], "persistent_temporal_continuity")
            self.assertEqual(second["temporal_continuity"]["state"], "persistent_temporal_continuity")
            self.assertEqual(second["temporal_memory_summary"]["total_observations"], 2)

    def test_corruption_recovery_rebuilds_memory(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            memory_path = Path(temp_dir) / "runtime_temporal_memory.json"
            memory_path.write_text("{bad json", encoding="utf-8")

            result = build_runtime_temporal(make_temporal_context(), memory_path=memory_path)
            recovered = json.loads(memory_path.read_text(encoding="utf-8"))

            self.assertEqual(result["temporal_memory_summary"]["memory_status"], "recovered")
            self.assertEqual(recovered["corrupted_recoveries"], 1)

    def test_temporal_memory_persistence_accumulates_runs(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            memory_path = Path(temp_dir) / "runtime_temporal_memory.json"
            build_runtime_temporal(make_temporal_context(), memory_path=memory_path)
            result = build_runtime_temporal(
                make_temporal_context(
                    federation_pressure=68,
                    federation_divergence=50,
                    dream_forecast="recovery_projection",
                ),
                memory_path=memory_path,
            )
            stored = load_temporal_memory(path=memory_path)

            self.assertEqual(result["temporal_memory_summary"]["total_observations"], 2)
            self.assertEqual(stored["aggregates"]["total_runs"], 2)

    def test_forecast_shaping_reflects_recovery_arc(self):
        result = build_runtime_temporal(
            make_temporal_context(
                federation_pressure=84,
                federation_resilience=90,
                federation_divergence=66,
                subconscious_state="unstable_continuity_subconscious",
                subconscious_integrity=36,
                consciousness_clarity=86,
                dream_forecast="optimistic_recovery_projection",
                dreaming_integrity=78,
            ),
            persist_memory=False,
        )

        self.assertEqual(result["temporal_projection"], "recovering_temporal_arc")
        self.assertIn("stabilize_continuity_decay", result["temporal_governance"]["governance_actions"])

    def test_federation_integration_consumes_federation_outputs(self):
        result = build_runtime_temporal(
            make_temporal_context(
                federation_pressure=70,
                federation_alignment=82,
                continuity_projection="adaptive_recovery",
                cinematic_runtime_state="unstable_cinematic_transition",
            ),
            persist_memory=False,
        )

        self.assertEqual(result["temporal_continuity"]["continuity_reference"], "adaptive_recovery")
        self.assertIn(result["cinematic_temporal_flow"], {"unstable_but_recovering", "fractured_cinematic_flow"})

    def test_malformed_payload_degrades_without_crashing(self):
        result = build_runtime_temporal(
            {
                "runtime_federation": "bad-payload",
                "federation_metrics": {"federation_pressure": "NaN"},
                "runtime_subconscious": None,
                "consciousness_metrics": {"orchestration_clarity": "bad"},
                "dream_metrics": {"dreaming_integrity": "bad"},
            },
            persist_memory=False,
        )

        self.assertIn("runtime_temporal", result)
        self.assertIsInstance(result["temporal_events"], list)
        self.assertIsInstance(result["temporal_metrics"], dict)

    def test_prepare_playback_runtime_exposes_temporal_payloads(self):
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
            "source_fingerprint": "src-temporal-plan",
        }

        plan = prepare_playback_runtime(movie={"title": "Film"}, selected_source=source, sources=[source])

        self.assertIn("runtime_temporal", plan)
        self.assertIn("temporal_metrics", plan["session_payload"])
        self.assertIn("temporal_events", plan["readiness_snapshot"])
        self.assertTrue(plan["runtime_cycle_phase"])

    def test_session_store_persists_temporal_fields(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = StreamSessionStore(path=Path(temp_dir) / "sessions.json")
            session = StreamSession(
                session_id="tmp123",
                movie_id="film-1",
                source_fingerprint="src-temp",
                handoff_mode="browser_handoff",
                preferred_runtime="browser_stream",
                session_state="prepared",
                runtime_temporal={"state": "temporal_balancing"},
                temporal_metrics={"temporal_stability": 68},
                temporal_events=[{"event_type": "temporal_synthesized"}],
                temporal_stability=68,
                temporal_momentum=62,
                temporal_pressure=44,
                temporal_alignment=72,
                temporal_integrity=70,
                continuity_decay_rate=28,
                runtime_rhythm_state="measured_pacing",
                cinematic_temporal_flow="steady_cinematic_flow",
                runtime_cycle_phase="stable_progression",
                temporal_projection="stable_temporal_expansion",
                temporal_memory_summary={"total_observations": 3},
            )

            store.save_session(session)
            saved = store.get_session("tmp123")

            self.assertEqual(saved["runtime_temporal"]["state"], "temporal_balancing")
            self.assertEqual(saved["temporal_stability"], 68)
            self.assertEqual(saved["runtime_cycle_phase"], "stable_progression")
            self.assertEqual(saved["temporal_projection"], "stable_temporal_expansion")


if __name__ == "__main__":
    unittest.main()
