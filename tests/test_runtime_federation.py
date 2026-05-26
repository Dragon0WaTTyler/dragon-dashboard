import json
import tempfile
import unittest
from pathlib import Path

from domains.magnets.playback import prepare_playback_runtime
from domains.magnets.runtime_federation import build_runtime_federation, load_federation_memory
from domains.magnets.services.session_store import StreamSessionStore
from domains.magnets.sessions import StreamSession


def make_federation_context(
    *,
    coordination_confidence=74,
    runtime_resilience=70,
    authority_state="approved",
    authority_confidence=68,
    forced_constraints=None,
    blocked_paths=None,
    runtime_temperament="calm",
    identity_confidence=72,
    orchestration_maturity=70,
    ecosystem_climate="calm_climate",
    ecosystem_integrity=68,
    ecosystem_stability=66,
    runtime_atmosphere="cinematic_stable",
    cinematic_quality=78,
    runtime_polish=72,
    orchestration_focus="equilibrium_focus",
    awareness_integrity=80,
    orchestration_clarity=78,
    perception_integrity=76,
    runtime_instinct_state="stable_instinct",
    instinct_integrity=74,
    orchestration_survival_score=76,
    subconscious_integrity=72,
    subconscious_balance=70,
    dormant_resilience_strength=74,
    dream_forecast="continuity_holding",
    dreaming_integrity=76,
    continuity_projection_strength=74,
    runtime_mirroring_integrity=72,
    orchestration_dream_balance=70,
    pressure_score=34,
    instinct_pressure=28,
    latent_pressure=24,
    continuity_confidence=76,
):
    return {
        "coordination_metrics": {
            "coordination_confidence": coordination_confidence,
            "runtime_resilience": runtime_resilience,
        },
        "runtime_negotiation": {
            "selected_runtime": "browser_runtime",
        },
        "authority_state": authority_state,
        "authority_confidence": authority_confidence,
        "governance_actions": list(forced_constraints or []),
        "forced_constraints": [{"constraint": item} for item in (forced_constraints or [])],
        "blocked_paths": list(blocked_paths or []),
        "runtime_temperament": runtime_temperament,
        "identity_confidence": identity_confidence,
        "identity_metrics": {
            "orchestration_maturity": orchestration_maturity,
        },
        "ecosystem_climate": {
            "climate": ecosystem_climate,
        },
        "ecosystem_metrics": {
            "ecosystem_integrity": ecosystem_integrity,
            "ecosystem_stability": ecosystem_stability,
        },
        "runtime_atmosphere": {
            "atmosphere": runtime_atmosphere,
        },
        "cinematic_metrics": {
            "cinematic_quality": cinematic_quality,
            "runtime_polish": runtime_polish,
        },
        "orchestration_focus": {
            "focus": orchestration_focus,
        },
        "consciousness_metrics": {
            "awareness_integrity": awareness_integrity,
            "orchestration_clarity": orchestration_clarity,
            "perception_integrity": perception_integrity,
        },
        "runtime_instinct": {
            "instinct_state": runtime_instinct_state,
        },
        "instinct_metrics": {
            "instinct_integrity": instinct_integrity,
            "orchestration_survival_score": orchestration_survival_score,
        },
        "runtime_subconscious": {
            "subconscious_state": "persistent_orchestration_subconscious",
        },
        "subconscious_metrics": {
            "subconscious_integrity": subconscious_integrity,
            "subconscious_balance": subconscious_balance,
            "dormant_resilience_strength": dormant_resilience_strength,
        },
        "dream_forecast": {
            "forecast": dream_forecast,
        },
        "dream_metrics": {
            "dreaming_integrity": dreaming_integrity,
            "continuity_projection_strength": continuity_projection_strength,
            "runtime_mirroring_integrity": runtime_mirroring_integrity,
            "orchestration_dream_balance": orchestration_dream_balance,
        },
        "orchestration_pressure": {
            "pressure_score": pressure_score,
        },
        "instinct_pressure": {
            "pressure_score": instinct_pressure,
        },
        "subconscious_pressure": {
            "latent_pressure": latent_pressure,
        },
        "continuity_state": {
            "continuity_confidence": continuity_confidence,
        },
    }


class RuntimeFederationTests(unittest.TestCase):
    def test_deterministic_federation_synthesis(self):
        context = make_federation_context()

        first = build_runtime_federation(context, persist_memory=False)
        second = build_runtime_federation(context, persist_memory=False)

        self.assertEqual(first, second)

    def test_convergence_detection_shapes_harmonic_state(self):
        result = build_runtime_federation(make_federation_context(), persist_memory=False)

        self.assertEqual(result["federation_state"]["state"], "federation_convergent")
        self.assertEqual(result["orchestration_unity"], "high")

    def test_divergence_handling_shapes_volatile_stabilization(self):
        result = build_runtime_federation(
            make_federation_context(
                authority_state="guarded",
                forced_constraints=["authority_override"],
                blocked_paths=["browser_runtime"],
                ecosystem_climate="degraded_climate",
                ecosystem_integrity=38,
                ecosystem_stability=40,
                orchestration_focus="focused_recovery",
                runtime_instinct_state="unstable_instinct",
                instinct_integrity=34,
                dream_forecast="stabilization_with_recovery",
                dreaming_integrity=72,
                pressure_score=74,
                instinct_pressure=70,
                latent_pressure=62,
            ),
            persist_memory=False,
        )

        self.assertEqual(result["runtime_phase_transition"], "volatile_stabilization")
        self.assertEqual(result["orchestration_unity"], "moderate")
        self.assertGreaterEqual(result["federation_pressure"], 65)
        self.assertEqual(result["continuity_projection"], "adaptive_recovery")
        self.assertEqual(result["cinematic_runtime_state"], "unstable_cinematic_transition")

    def test_corruption_recovery_rebuilds_memory(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            memory_path = Path(temp_dir) / "runtime_federation_memory.json"
            memory_path.write_text("{bad json", encoding="utf-8")

            result = build_runtime_federation(make_federation_context(), memory_path=memory_path)
            recovered = json.loads(memory_path.read_text(encoding="utf-8"))

            self.assertEqual(result["federation_memory_summary"]["memory_status"], "recovered")
            self.assertEqual(recovered["corrupted_recoveries"], 1)

    def test_continuity_projection_and_governance_are_shaped(self):
        result = build_runtime_federation(
            make_federation_context(
                authority_state="guarded",
                forced_constraints=["authority_override"],
                runtime_instinct_state="unstable_instinct",
                instinct_integrity=42,
                pressure_score=66,
            ),
            persist_memory=False,
        )

        self.assertIn("stabilize_instinct_disruption", result["federation_governance"]["governance_actions"])
        self.assertIn(result["continuity_projection"], {"adaptive_recovery", "guarded_rebalancing"})

    def test_orchestration_unity_and_resilience_scores_are_exposed(self):
        result = build_runtime_federation(make_federation_context(), persist_memory=False)

        self.assertIsInstance(result["adaptive_federation_balance"], int)
        self.assertGreaterEqual(result["federation_resilience"], 0)
        self.assertIn("federation_resilience", result["federation_metrics"])

    def test_federation_memory_persistence_accumulates_runs(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            memory_path = Path(temp_dir) / "runtime_federation_memory.json"
            build_runtime_federation(make_federation_context(), memory_path=memory_path)
            result = build_runtime_federation(
                make_federation_context(
                    authority_state="guarded",
                    forced_constraints=["authority_override"],
                    pressure_score=64,
                ),
                memory_path=memory_path,
            )
            stored = load_federation_memory(path=memory_path)

            self.assertEqual(result["federation_memory_summary"]["total_observations"], 2)
            self.assertEqual(stored["aggregates"]["total_runs"], 2)

    def test_malformed_payload_degrades_without_crashing(self):
        result = build_runtime_federation(
            {
                "coordination_metrics": None,
                "runtime_instinct": "bad-payload",
                "dream_metrics": {"dreaming_integrity": "NaN"},
                "blocked_paths": "not-a-list",
            },
            persist_memory=False,
        )

        self.assertIn("runtime_federation", result)
        self.assertIsInstance(result["federation_events"], list)
        self.assertIsInstance(result["federation_metrics"], dict)

    def test_prepare_playback_runtime_exposes_federation_payloads(self):
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
            "source_fingerprint": "src-federation-plan",
        }

        plan = prepare_playback_runtime(movie={"title": "Film"}, selected_source=source, sources=[source])

        self.assertIn("runtime_federation", plan)
        self.assertIn("federation_metrics", plan["session_payload"])
        self.assertIn("federation_events", plan["readiness_snapshot"])
        self.assertTrue(plan["runtime_phase_transition"])

    def test_session_store_persists_federation_fields(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = StreamSessionStore(path=Path(temp_dir) / "sessions.json")
            session = StreamSession(
                session_id="fed123",
                movie_id="film-1",
                source_fingerprint="src-fed",
                handoff_mode="browser_handoff",
                preferred_runtime="browser_stream",
                session_state="prepared",
                runtime_federation={"state": "federation_convergent"},
                federation_metrics={"federation_coherence": 82},
                federation_events=[{"event_type": "federation_synthesized"}],
                federation_coherence=82,
                federation_pressure=34,
                federation_integrity=78,
                federation_resilience=80,
                federation_alignment=84,
                orchestration_unity="high",
                cinematic_runtime_state="cinematic_runtime_harmony",
                runtime_phase_transition="harmonic_continuation",
                continuity_projection="cinematic_continuation",
                federation_memory_summary={"total_observations": 3},
            )

            store.save_session(session)
            saved = store.get_session("fed123")

            self.assertEqual(saved["runtime_federation"]["state"], "federation_convergent")
            self.assertEqual(saved["federation_coherence"], 82)
            self.assertEqual(saved["orchestration_unity"], "high")
            self.assertEqual(saved["runtime_phase_transition"], "harmonic_continuation")


if __name__ == "__main__":
    unittest.main()
