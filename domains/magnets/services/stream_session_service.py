from __future__ import annotations

import hashlib
from typing import Any, Mapping

from ..browser_runtime import (
    build_browser_runtime_session,
    get_browser_runtime_session_registry,
)
from ..playback import prepare_playback_runtime
from ..analytics import SessionAnalyticsService
from ..runtime.identifiers import normalize_token, source_fingerprint
from ..runtime.observability import emit_event
from ..runtime.playback_policy import evaluate_playback_admission
from ..runtime.session_runtime import normalize_runtime_intent, resolve_runtime_intent
from ..stream_runtime import (
    append_runtime_event,
    build_runtime_event,
    build_runtime_manifest,
    evolve_runtime_state,
    get_runtime_registry,
)
from ..sessions import StreamSession, normalize_session_state, utc_now_iso
from .session_store import StreamSessionStore


HARD_BLOCK_REASONS = {
    "invalid_magnet",
    "low_streamability_confidence",
    "low_quality_source",
    "unsupported_release_type",
}


class StreamSessionService:
    def __init__(
        self,
        *,
        store: StreamSessionStore | None = None,
        analytics_service: SessionAnalyticsService | None = None,
        runtime_registry=None,
        browser_runtime_session_registry=None,
    ) -> None:
        self.store = store or StreamSessionStore()
        self.analytics_service = analytics_service or SessionAnalyticsService()
        self.runtime_registry = runtime_registry or get_runtime_registry()
        self.browser_runtime_session_registry = browser_runtime_session_registry or get_browser_runtime_session_registry()

    def create_session(
        self,
        *,
        movie: Mapping[str, Any] | None = None,
        source: Mapping[str, Any] | None = None,
        handoff_mode: str = "",
        preferred_runtime: str = "",
        player_sources: list[Mapping[str, Any]] | None = None,
        fallback_urls: list[str] | None = None,
    ) -> dict[str, Any]:
        movie_data = dict(movie or {})
        source_data = dict(source or {})
        movie_id = self._movie_id(movie_data)
        fingerprint = source_fingerprint(source_data) or self._fallback_source_fingerprint(source_data)
        admission = evaluate_playback_admission(source_data, movie=movie_data)
        playback_plan = prepare_playback_runtime(
            movie=movie_data,
            selected_source=source_data,
            sources=[source_data],
            player_sources=player_sources,
            fallback_urls=fallback_urls,
        )
        session_playback = dict(playback_plan.get("session_payload") or {})
        runtime_preference = normalize_runtime_intent(preferred_runtime)
        session_id = self._session_id(
            movie_id=movie_id,
            source_fingerprint=fingerprint,
            handoff_mode=handoff_mode,
            preferred_runtime=runtime_preference,
        )
        runtime_id = self.runtime_registry.build_runtime_id(
            session_id=session_id,
            source_fingerprint=fingerprint,
            runtime_profile=str(session_playback.get("runtime_profile") or ""),
        )
        runtime_manifest = build_runtime_manifest(
            runtime_id=runtime_id,
            session_id=session_id,
            selected_source=dict(session_playback.get("selected_source") or {}),
            runtime_mode=str(session_playback.get("runtime_mode") or ""),
            runtime_state=str(session_playback.get("runtime_state") or ""),
            startup_confidence=str(session_playback.get("startup_confidence") or ""),
            capability_snapshot=dict(playback_plan.get("readiness_snapshot") or {}),
            diagnostics=dict(playback_plan.get("runtime_diagnostics") or {}),
            fallbacks=list(playback_plan.get("fallbacks") or []),
            preflight=dict(session_playback.get("runtime_preflight") or {}),
            transport=dict(playback_plan.get("runtime_transport") or {}),
        )
        browser_runtime_bridge = dict(playback_plan.get("browser_runtime_bridge") or {})
        browser_runtime_session = build_browser_runtime_session(
            linked_stream_runtime_id=runtime_id,
            playback_session_id=session_id,
            runtime_state=str(session_playback.get("runtime_state") or ""),
            capability_snapshot=dict(browser_runtime_bridge.get("capability_snapshot") or {}),
            bootstrap_summary=dict(browser_runtime_bridge.get("bootstrap") or {}),
            execution_state=str(session_playback.get("execution_state") or ""),
            execution_metrics=dict(session_playback.get("execution_metrics") or {}),
            execution_timeline=dict(session_playback.get("execution_timeline") or {}),
            simulated_runtime_health=str(session_playback.get("simulated_runtime_health") or ""),
            recovery_path=dict(session_playback.get("recovery_path") or {}),
            execution_events=list(session_playback.get("execution_events") or []),
            coordination_state=str(session_playback.get("coordination_state") or ""),
            coordination_metrics=dict(session_playback.get("coordination_metrics") or {}),
        )
        runtime_events = list(session_playback.get("runtime_events") or [])
        runtime_events = append_runtime_event(
            runtime_events,
            build_runtime_event(
                "runtime-created",
                runtime_id=runtime_id,
                session_id=session_id,
                runtime_state=str(session_playback.get("runtime_state") or ""),
                runtime_mode=str(session_playback.get("runtime_mode") or ""),
                details={"playback_runtime": str(session_playback.get("playback_runtime") or "")},
            ),
        )
        session = StreamSession(
            session_id=session_id,
            movie_id=movie_id,
            source_fingerprint=fingerprint,
            handoff_mode=self._handoff_mode(handoff_mode, admission["policy"]),
            preferred_runtime=runtime_preference,
            session_state="created",
            compatibility_snapshot=admission["snapshot"],
            created_at=utc_now_iso(),
            updated_at=utc_now_iso(),
            runtime_intent=resolve_runtime_intent(
                preferred_runtime=runtime_preference,
                handoff_mode=handoff_mode,
                admission_policy=admission["policy"],
            ),
            admission_policy=admission["policy"],
            movie_title=self._movie_name(movie_data),
            playback_runtime=str(session_playback.get("playback_runtime") or ""),
            runtime_mode=str(session_playback.get("runtime_mode") or ""),
            runtime_state=str(session_playback.get("runtime_state") or ""),
            runtime_profile=str(session_playback.get("runtime_profile") or ""),
            selected_source=dict(session_playback.get("selected_source") or {}),
            playback_readiness=str(session_playback.get("playback_readiness") or ""),
            startup_confidence=str(session_playback.get("startup_confidence") or ""),
            runtime_warnings=list(session_playback.get("runtime_warnings") or []),
            runtime_preflight=dict(session_playback.get("runtime_preflight") or {}),
            runtime_manifest=runtime_manifest,
            runtime_events=runtime_events,
            browser_runtime_bridge=browser_runtime_bridge,
            browser_runtime_session=browser_runtime_session,
            readiness_snapshot=dict(session_playback.get("readiness_snapshot") or {}),
            execution_state=str(session_playback.get("execution_state") or ""),
            execution_metrics=dict(session_playback.get("execution_metrics") or {}),
            execution_timeline=dict(session_playback.get("execution_timeline") or {}),
            runtime_grade=dict(session_playback.get("runtime_grade") or {}),
            simulated_runtime_health=str(session_playback.get("simulated_runtime_health") or ""),
            recovery_path=dict(session_playback.get("recovery_path") or {}),
            execution_events=list(session_playback.get("execution_events") or []),
            coordination_state=str(session_playback.get("coordination_state") or ""),
            coordination_metrics=dict(session_playback.get("coordination_metrics") or {}),
            orchestration_graph=dict(session_playback.get("orchestration_graph") or {}),
            runtime_negotiation=dict(session_playback.get("runtime_negotiation") or {}),
            adaptive_strategy=dict(session_playback.get("adaptive_strategy") or {}),
            runtime_switch_history=list(session_playback.get("runtime_switch_history") or []),
            fallback_negotiation=dict(session_playback.get("fallback_negotiation") or {}),
            coordination_events=list(session_playback.get("coordination_events") or []),
            runtime_predictions=dict(session_playback.get("runtime_predictions") or {}),
            runtime_memory_summary=dict(session_playback.get("runtime_memory_summary") or {}),
            adaptation_history=dict(session_playback.get("adaptation_history") or {}),
            confidence_evolution=dict(session_playback.get("confidence_evolution") or {}),
            runtime_reputation=dict(session_playback.get("runtime_reputation") or {}),
            orchestration_forecast=dict(session_playback.get("orchestration_forecast") or {}),
            intelligence_metrics=dict(session_playback.get("intelligence_metrics") or {}),
            historical_patterns=list(session_playback.get("historical_patterns") or []),
            runtime_learning=dict(session_playback.get("runtime_learning") or {}),
            intelligence_events=list(session_playback.get("intelligence_events") or []),
            authority_state=str(session_playback.get("authority_state") or ""),
            authority_confidence=int(session_playback.get("authority_confidence", 0) or 0),
            authority_reasoning=list(session_playback.get("authority_reasoning") or []),
            runtime_risk=dict(session_playback.get("runtime_risk") or {}),
            arbitration_result=dict(session_playback.get("arbitration_result") or {}),
            arbitration_trace=list(session_playback.get("arbitration_trace") or []),
            governance_actions=list(session_playback.get("governance_actions") or []),
            stability_state=dict(session_playback.get("stability_state") or {}),
            execution_policy=dict(session_playback.get("execution_policy") or {}),
            forced_constraints=list(session_playback.get("forced_constraints") or []),
            blocked_paths=list(session_playback.get("blocked_paths") or []),
            forced_fallback=bool(session_playback.get("forced_fallback")),
            fallback_authority=dict(session_playback.get("fallback_authority") or {}),
            confidence_governance=dict(session_playback.get("confidence_governance") or {}),
            authority_memory_summary=dict(session_playback.get("authority_memory_summary") or {}),
            authority_events=list(session_playback.get("authority_events") or []),
            authority_metrics=dict(session_playback.get("authority_metrics") or {}),
            runtime_identity=dict(session_playback.get("runtime_identity") or {}),
            orchestration_archetype=str(session_playback.get("orchestration_archetype") or ""),
            runtime_temperament=str(session_playback.get("runtime_temperament") or ""),
            adaptation_profile=str(session_playback.get("adaptation_profile") or ""),
            behavioral_drift=dict(session_playback.get("behavioral_drift") or {}),
            continuity_state=dict(session_playback.get("continuity_state") or {}),
            identity_confidence=int(session_playback.get("identity_confidence", 0) or 0),
            identity_forecast=dict(session_playback.get("identity_forecast") or {}),
            persistent_traits=list(session_playback.get("persistent_traits") or []),
            orchestration_traits=list(session_playback.get("orchestration_traits") or []),
            identity_metrics=dict(session_playback.get("identity_metrics") or {}),
            identity_warnings=list(session_playback.get("identity_warnings") or []),
            identity_events=list(session_playback.get("identity_events") or []),
            runtime_ecosystem=dict(session_playback.get("runtime_ecosystem") or {}),
            ecosystem_balance=dict(session_playback.get("ecosystem_balance") or {}),
            orchestration_pressure=dict(session_playback.get("orchestration_pressure") or {}),
            runtime_clusters=dict(session_playback.get("runtime_clusters") or {}),
            stability_zone=dict(session_playback.get("stability_zone") or {}),
            ecosystem_climate=dict(session_playback.get("ecosystem_climate") or {}),
            degradation_currents=dict(session_playback.get("degradation_currents") or {}),
            resilience_topology=dict(session_playback.get("resilience_topology") or {}),
            adaptive_equilibrium=dict(session_playback.get("adaptive_equilibrium") or {}),
            ecosystem_forecast=dict(session_playback.get("ecosystem_forecast") or {}),
            ecosystem_governance=dict(session_playback.get("ecosystem_governance") or {}),
            ecosystem_metrics=dict(session_playback.get("ecosystem_metrics") or {}),
            ecosystem_events=list(session_playback.get("ecosystem_events") or []),
            ecosystem_memory=dict(session_playback.get("ecosystem_memory") or {}),
            runtime_cinema=dict(session_playback.get("runtime_cinema") or {}),
            cinematic_direction=dict(session_playback.get("cinematic_direction") or {}),
            runtime_pacing=dict(session_playback.get("runtime_pacing") or {}),
            immersion_state=dict(session_playback.get("immersion_state") or {}),
            runtime_atmosphere=dict(session_playback.get("runtime_atmosphere") or {}),
            dramatic_tension=dict(session_playback.get("dramatic_tension") or {}),
            continuity_cinema=dict(session_playback.get("continuity_cinema") or {}),
            orchestration_mood=dict(session_playback.get("orchestration_mood") or {}),
            scene_energy=dict(session_playback.get("scene_energy") or {}),
            cinematic_balance=dict(session_playback.get("cinematic_balance") or {}),
            runtime_aesthetics=dict(session_playback.get("runtime_aesthetics") or {}),
            cinematic_forecast=dict(session_playback.get("cinematic_forecast") or {}),
            cinematic_governance=dict(session_playback.get("cinematic_governance") or {}),
            cinematic_metrics=dict(session_playback.get("cinematic_metrics") or {}),
            cinematic_events=list(session_playback.get("cinematic_events") or []),
            cinematic_memory=dict(session_playback.get("cinematic_memory") or {}),
            runtime_consciousness=dict(session_playback.get("runtime_consciousness") or {}),
            awareness_state=dict(session_playback.get("awareness_state") or {}),
            orchestration_attention=dict(session_playback.get("orchestration_attention") or {}),
            continuity_awareness=dict(session_playback.get("continuity_awareness") or {}),
            runtime_reflection=dict(session_playback.get("runtime_reflection") or {}),
            orchestration_intuition=dict(session_playback.get("orchestration_intuition") or {}),
            cognitive_balance=dict(session_playback.get("cognitive_balance") or {}),
            awareness_pressure=dict(session_playback.get("awareness_pressure") or {}),
            orchestration_focus=dict(session_playback.get("orchestration_focus") or {}),
            runtime_presence=dict(session_playback.get("runtime_presence") or {}),
            orchestration_perception=dict(session_playback.get("orchestration_perception") or {}),
            consciousness_forecast=dict(session_playback.get("consciousness_forecast") or {}),
            consciousness_governance=dict(session_playback.get("consciousness_governance") or {}),
            consciousness_metrics=dict(session_playback.get("consciousness_metrics") or {}),
            consciousness_events=list(session_playback.get("consciousness_events") or []),
            consciousness_memory=dict(session_playback.get("consciousness_memory") or {}),
            runtime_instinct=dict(session_playback.get("runtime_instinct") or {}),
            stabilization_instinct=dict(session_playback.get("stabilization_instinct") or {}),
            resilience_instinct=dict(session_playback.get("resilience_instinct") or {}),
            fallback_instinct=dict(session_playback.get("fallback_instinct") or {}),
            continuity_instinct=dict(session_playback.get("continuity_instinct") or {}),
            cinematic_instinct=dict(session_playback.get("cinematic_instinct") or {}),
            equilibrium_instinct=dict(session_playback.get("equilibrium_instinct") or {}),
            orchestration_reflexes=dict(session_playback.get("orchestration_reflexes") or {}),
            instinct_pressure=dict(session_playback.get("instinct_pressure") or {}),
            adaptive_instinct=dict(session_playback.get("adaptive_instinct") or {}),
            runtime_survival=dict(session_playback.get("runtime_survival") or {}),
            instinct_forecast=dict(session_playback.get("instinct_forecast") or {}),
            instinct_governance=dict(session_playback.get("instinct_governance") or {}),
            instinct_metrics=dict(session_playback.get("instinct_metrics") or {}),
            instinct_events=list(session_playback.get("instinct_events") or []),
            instinct_memory=dict(session_playback.get("instinct_memory") or {}),
            runtime_subconscious=dict(session_playback.get("runtime_subconscious") or {}),
            latent_patterns=dict(session_playback.get("latent_patterns") or {}),
            orchestration_underflow=dict(session_playback.get("orchestration_underflow") or {}),
            hidden_equilibrium=dict(session_playback.get("hidden_equilibrium") or {}),
            subconscious_pressure=dict(session_playback.get("subconscious_pressure") or {}),
            silent_adaptation=dict(session_playback.get("silent_adaptation") or {}),
            continuity_underlayers=dict(session_playback.get("continuity_underlayers") or {}),
            orchestration_residue=dict(session_playback.get("orchestration_residue") or {}),
            dormant_resilience=dict(session_playback.get("dormant_resilience") or {}),
            cinematic_underflow=dict(session_playback.get("cinematic_underflow") or {}),
            orchestration_echoes=dict(session_playback.get("orchestration_echoes") or {}),
            subconscious_forecast=dict(session_playback.get("subconscious_forecast") or {}),
            subconscious_governance=dict(session_playback.get("subconscious_governance") or {}),
            subconscious_metrics=dict(session_playback.get("subconscious_metrics") or {}),
            subconscious_events=list(session_playback.get("subconscious_events") or []),
            subconscious_memory=dict(session_playback.get("subconscious_memory") or {}),
            runtime_dreaming=dict(session_playback.get("runtime_dreaming") or {}),
            cinematic_dreams=dict(session_playback.get("cinematic_dreams") or {}),
            orchestration_visions=dict(session_playback.get("orchestration_visions") or {}),
            latent_projection=dict(session_playback.get("latent_projection") or {}),
            stabilization_dreams=dict(session_playback.get("stabilization_dreams") or {}),
            resilience_dreams=dict(session_playback.get("resilience_dreams") or {}),
            continuity_dreams=dict(session_playback.get("continuity_dreams") or {}),
            subconscious_projection=dict(session_playback.get("subconscious_projection") or {}),
            dormant_pathways=dict(session_playback.get("dormant_pathways") or {}),
            adaptive_dreaming=dict(session_playback.get("adaptive_dreaming") or {}),
            runtime_mirroring=dict(session_playback.get("runtime_mirroring") or {}),
            dream_forecast=dict(session_playback.get("dream_forecast") or {}),
            dream_governance=dict(session_playback.get("dream_governance") or {}),
            dream_metrics=dict(session_playback.get("dream_metrics") or {}),
            dream_events=list(session_playback.get("dream_events") or []),
            dreaming_memory=dict(session_playback.get("dreaming_memory") or {}),
            runtime_federation=dict(session_playback.get("runtime_federation") or {}),
            federation_state=dict(session_playback.get("federation_state") or {}),
            federation_projection=dict(session_playback.get("federation_projection") or {}),
            federation_forecast=dict(session_playback.get("federation_forecast") or {}),
            federation_governance=dict(session_playback.get("federation_governance") or {}),
            federation_continuity=dict(session_playback.get("federation_continuity") or {}),
            federation_metrics=dict(session_playback.get("federation_metrics") or {}),
            federation_events=list(session_playback.get("federation_events") or []),
            federation_coherence=int(session_playback.get("federation_coherence", 0) or 0),
            federation_harmony=int(session_playback.get("federation_harmony", 0) or 0),
            federation_pressure=int(session_playback.get("federation_pressure", 0) or 0),
            federation_integrity=int(session_playback.get("federation_integrity", 0) or 0),
            federation_resilience=int(session_playback.get("federation_resilience", 0) or 0),
            federation_alignment=int(session_playback.get("federation_alignment", 0) or 0),
            federation_divergence=int(session_playback.get("federation_divergence", 0) or 0),
            runtime_continuity_profile=str(session_playback.get("runtime_continuity_profile") or ""),
            cinematic_runtime_state=str(session_playback.get("cinematic_runtime_state") or ""),
            orchestration_unity=str(session_playback.get("orchestration_unity") or ""),
            adaptive_federation_balance=int(session_playback.get("adaptive_federation_balance", 0) or 0),
            runtime_phase_transition=str(session_playback.get("runtime_phase_transition") or ""),
            continuity_projection=str(session_playback.get("continuity_projection") or ""),
            federation_memory_summary=dict(session_playback.get("federation_memory_summary") or {}),
            runtime_temporal=dict(session_playback.get("runtime_temporal") or {}),
            temporal_state=dict(session_playback.get("temporal_state") or {}),
            temporal_phase=dict(session_playback.get("temporal_phase") or {}),
            temporal_rhythm=dict(session_playback.get("temporal_rhythm") or {}),
            temporal_forecast=dict(session_playback.get("temporal_forecast") or {}),
            temporal_continuity=dict(session_playback.get("temporal_continuity") or {}),
            temporal_decay=dict(session_playback.get("temporal_decay") or {}),
            temporal_recovery=dict(session_playback.get("temporal_recovery") or {}),
            temporal_governance=dict(session_playback.get("temporal_governance") or {}),
            temporal_metrics=dict(session_playback.get("temporal_metrics") or {}),
            temporal_events=list(session_playback.get("temporal_events") or []),
            temporal_integrity=int(session_playback.get("temporal_integrity", 0) or 0),
            temporal_alignment=int(session_playback.get("temporal_alignment", 0) or 0),
            temporal_stability=int(session_playback.get("temporal_stability", 0) or 0),
            temporal_momentum=int(session_playback.get("temporal_momentum", 0) or 0),
            temporal_pressure=int(session_playback.get("temporal_pressure", 0) or 0),
            continuity_decay_rate=int(session_playback.get("continuity_decay_rate", 0) or 0),
            runtime_rhythm_state=str(session_playback.get("runtime_rhythm_state") or ""),
            cinematic_temporal_flow=str(session_playback.get("cinematic_temporal_flow") or ""),
            orchestration_phase_velocity=str(session_playback.get("orchestration_phase_velocity") or ""),
            adaptive_temporal_balance=int(session_playback.get("adaptive_temporal_balance", 0) or 0),
            runtime_cycle_phase=str(session_playback.get("runtime_cycle_phase") or ""),
            temporal_projection=str(session_playback.get("temporal_projection") or ""),
            temporal_memory_summary=dict(session_playback.get("temporal_memory_summary") or {}),
            runtime_resonance=dict(session_playback.get("runtime_resonance") or {}),
            resonance_state=dict(session_playback.get("resonance_state") or {}),
            resonance_harmony=dict(session_playback.get("resonance_harmony") or {}),
            resonance_sync=dict(session_playback.get("resonance_sync") or {}),
            resonance_projection=dict(session_playback.get("resonance_projection") or {}),
            resonance_equilibrium=dict(session_playback.get("resonance_equilibrium") or {}),
            resonance_governance=dict(session_playback.get("resonance_governance") or {}),
            resonance_metrics=dict(session_playback.get("resonance_metrics") or {}),
            resonance_events=list(session_playback.get("resonance_events") or []),
            resonance_recovery=dict(session_playback.get("resonance_recovery") or {}),
            resonance_integrity=int(session_playback.get("resonance_integrity", 0) or 0),
            resonance_alignment=int(session_playback.get("resonance_alignment", 0) or 0),
            resonance_stability=int(session_playback.get("resonance_stability", 0) or 0),
            resonance_pressure=int(session_playback.get("resonance_pressure", 0) or 0),
            resonance_fragmentation=int(session_playback.get("resonance_fragmentation", 0) or 0),
            resonance_cohesion=int(session_playback.get("resonance_cohesion", 0) or 0),
            harmonic_runtime_state=str(session_playback.get("harmonic_runtime_state") or ""),
            cinematic_resonance=str(session_playback.get("cinematic_resonance") or ""),
            orchestration_resonance=str(session_playback.get("orchestration_resonance") or ""),
            adaptive_sync_balance=int(session_playback.get("adaptive_sync_balance", 0) or 0),
            resonance_phase=str(session_playback.get("resonance_phase") or ""),
            sync_drift=int(session_playback.get("sync_drift", 0) or 0),
            runtime_harmony_index=int(session_playback.get("runtime_harmony_index", 0) or 0),
            resonance_memory_summary=dict(session_playback.get("resonance_memory_summary") or {}),
            runtime_symbiosis=dict(session_playback.get("runtime_symbiosis") or {}),
            symbiosis_state=dict(session_playback.get("symbiosis_state") or {}),
            symbiosis_balance=dict(session_playback.get("symbiosis_balance") or {}),
            symbiosis_cooperation=dict(session_playback.get("symbiosis_cooperation") or {}),
            symbiosis_dependencies=dict(session_playback.get("symbiosis_dependencies") or {}),
            symbiosis_recovery=dict(session_playback.get("symbiosis_recovery") or {}),
            symbiosis_projection=dict(session_playback.get("symbiosis_projection") or {}),
            symbiosis_equilibrium=dict(session_playback.get("symbiosis_equilibrium") or {}),
            symbiosis_governance=dict(session_playback.get("symbiosis_governance") or {}),
            symbiosis_metrics=dict(session_playback.get("symbiosis_metrics") or {}),
            symbiosis_events=list(session_playback.get("symbiosis_events") or []),
            symbiosis_integrity=int(session_playback.get("symbiosis_integrity", 0) or 0),
            symbiosis_alignment=int(session_playback.get("symbiosis_alignment", 0) or 0),
            symbiosis_stability=int(session_playback.get("symbiosis_stability", 0) or 0),
            symbiosis_pressure=int(session_playback.get("symbiosis_pressure", 0) or 0),
            symbiosis_mutualism=int(session_playback.get("symbiosis_mutualism", 0) or 0),
            symbiosis_fragmentation=int(session_playback.get("symbiosis_fragmentation", 0) or 0),
            cooperative_runtime_state=str(session_playback.get("cooperative_runtime_state") or ""),
            runtime_coexistence=str(session_playback.get("runtime_coexistence") or ""),
            adaptive_mutual_balance=str(session_playback.get("adaptive_mutual_balance") or ""),
            systemic_runtime_health=str(session_playback.get("systemic_runtime_health") or ""),
            recovery_cohesion=str(session_playback.get("recovery_cohesion") or ""),
            dependency_stress=int(session_playback.get("dependency_stress", 0) or 0),
            symbiotic_phase=str(session_playback.get("symbiotic_phase") or ""),
            symbiosis_memory_summary=dict(session_playback.get("symbiosis_memory_summary") or {}),
        )
        saved = self.store.save_session(session)
        self.runtime_registry.create(
            {
                "runtime_id": runtime_id,
                "session_id": session_id,
                "selected_source": dict(saved.get("selected_source") or {}),
                "runtime_profile": str(saved.get("runtime_profile") or ""),
                "runtime_state": str(saved.get("runtime_state") or ""),
                "startup_confidence": str(saved.get("startup_confidence") or ""),
                "created_at": str(saved.get("created_at") or ""),
                "updated_at": str(saved.get("updated_at") or ""),
            }
        )
        self.browser_runtime_session_registry.create(browser_runtime_session)
        self.analytics_service.track_session_event("session_created", session=saved, source=source_data)
        self._emit_session_event(saved)
        return {"ok": True, "session": saved}

    def prepare_session(self, session_id: str) -> dict[str, Any]:
        session = self.store.get_session(session_id)
        if not session:
            return {"ok": False, "error": "Unknown session."}
        session = dict(session)
        policy = dict(session.get("admission_policy") or {})
        if self._is_hard_blocked(policy):
            session["session_state"] = "failed"
            session["failure_reason"] = str(policy.get("blocked_reason") or "").strip()
            session["playback_readiness"] = "blocked"
            session["runtime_state"] = evolve_runtime_state(session.get("runtime_state"), "failed")
        else:
            session["session_state"] = "prepared"
            runtime_mode = str((session.get("runtime_preflight") or {}).get("runtime_mode") or session.get("runtime_mode") or "").strip()
            target_state = "handoff_ready" if runtime_mode in {"browser_runtime", "external_runtime"} else "runtime_limited"
            session["runtime_state"] = evolve_runtime_state(session.get("runtime_state"), target_state)
            if not str(session.get("playback_readiness") or "").strip():
                session["playback_readiness"] = "prepared"
        session["runtime_manifest"] = self._refresh_runtime_manifest(session)
        session["browser_runtime_session"] = self._refresh_browser_runtime_session(session)
        session["runtime_events"] = append_runtime_event(
            session.get("runtime_events"),
            build_runtime_event(
                "runtime-preflight-passed" if session.get("session_state") == "prepared" else "runtime-preflight-blocked",
                runtime_id=self._runtime_id(session),
                session_id=str(session.get("session_id") or ""),
                runtime_state=str(session.get("runtime_state") or ""),
                runtime_mode=str(session.get("runtime_mode") or ""),
                details={"playback_readiness": str(session.get("playback_readiness") or "")},
            ),
        )
        session["updated_at"] = utc_now_iso()
        saved = self.store.save_session(session)
        self._sync_runtime_registry(saved)
        self._sync_browser_runtime_session(saved)
        self.analytics_service.track_session_event("session_prepared", session=saved)
        self._emit_session_event(saved)
        return {"ok": True, "session": saved}

    def handoff_session(self, session_id: str, *, runtime_intent: str = "") -> dict[str, Any]:
        session = self.store.get_session(session_id)
        if not session:
            return {"ok": False, "error": "Unknown session."}
        session = dict(session)
        policy = dict(session.get("admission_policy") or {})
        resolved_intent = normalize_runtime_intent(runtime_intent) or str(session.get("runtime_intent") or "").strip()
        session["runtime_intent"] = resolved_intent or "external_player"
        if self._is_hard_blocked(policy):
            session["session_state"] = "failed"
            session["failure_reason"] = str(policy.get("blocked_reason") or "").strip()
            session["runtime_state"] = evolve_runtime_state(session.get("runtime_state"), "failed")
        else:
            session["session_state"] = "handed_off"
            session["runtime_state"] = evolve_runtime_state(session.get("runtime_state"), "handoff_ready")
        session["runtime_manifest"] = self._refresh_runtime_manifest(session)
        session["browser_runtime_session"] = self._refresh_browser_runtime_session(session)
        session["runtime_events"] = append_runtime_event(
            session.get("runtime_events"),
            build_runtime_event(
                "runtime-fallback-selected" if session.get("runtime_mode") == "external_runtime" else "runtime-preflight-passed",
                runtime_id=self._runtime_id(session),
                session_id=str(session.get("session_id") or ""),
                runtime_state=str(session.get("runtime_state") or ""),
                runtime_mode=str(session.get("runtime_mode") or ""),
                details={"runtime_intent": session["runtime_intent"]},
            ),
        )
        session["updated_at"] = utc_now_iso()
        saved = self.store.save_session(session)
        self._sync_runtime_registry(saved)
        self._sync_browser_runtime_session(saved)
        self._track_handoff_analytics(saved)
        self._emit_session_event(saved)
        return {"ok": True, "session": saved}

    def fail_session(self, session_id: str, *, reason: str = "") -> dict[str, Any]:
        session = self.store.get_session(session_id)
        if not session:
            return {"ok": False, "error": "Unknown session."}
        session = dict(session)
        session["session_state"] = "failed"
        session["failure_reason"] = str(reason or session.get("failure_reason") or "failed").strip()
        session["runtime_state"] = evolve_runtime_state(session.get("runtime_state"), "failed")
        session["execution_state"] = "runtime_failed"
        session["runtime_manifest"] = self._refresh_runtime_manifest(session)
        session["browser_runtime_session"] = self._refresh_browser_runtime_session(session)
        session["runtime_events"] = append_runtime_event(
            session.get("runtime_events"),
            build_runtime_event(
                "runtime-preflight-blocked",
                runtime_id=self._runtime_id(session),
                session_id=str(session.get("session_id") or ""),
                runtime_state=str(session.get("runtime_state") or ""),
                runtime_mode=str(session.get("runtime_mode") or ""),
                details={"reason": session["failure_reason"]},
            ),
        )
        session["updated_at"] = utc_now_iso()
        saved = self.store.save_session(session)
        self._sync_runtime_registry(saved)
        self._sync_browser_runtime_session(saved)
        self.analytics_service.track_session_event("session_handoff_failed", session=saved)
        self._emit_session_event(saved)
        return {"ok": True, "session": saved}

    def expire_session(self, session_id: str) -> dict[str, Any]:
        session = self.store.get_session(session_id)
        if not session:
            return {"ok": False, "error": "Unknown session."}
        session = dict(session)
        session["session_state"] = "expired"
        session["runtime_state"] = evolve_runtime_state(session.get("runtime_state"), "expired")
        if str(session.get("execution_state") or "").strip() != "runtime_failed":
            session["execution_state"] = "runtime_completed"
        session["runtime_manifest"] = self._refresh_runtime_manifest(session)
        session["browser_runtime_session"] = self._refresh_browser_runtime_session(session)
        session["runtime_events"] = append_runtime_event(
            session.get("runtime_events"),
            build_runtime_event(
                "runtime-expired",
                runtime_id=self._runtime_id(session),
                session_id=str(session.get("session_id") or ""),
                runtime_state=str(session.get("runtime_state") or ""),
                runtime_mode=str(session.get("runtime_mode") or ""),
            ),
        )
        session["updated_at"] = utc_now_iso()
        saved = self.store.save_session(session)
        self._sync_runtime_registry(saved)
        self._sync_browser_runtime_session(saved)
        self.analytics_service.track_session_event("session_expired", session=saved)
        self._emit_session_event(saved)
        return {"ok": True, "session": saved}

    def get_session(self, session_id: str) -> dict[str, Any]:
        session = self.store.get_session(session_id)
        if not session:
            return {"ok": False, "error": "Unknown session."}
        return {"ok": True, "session": session}

    def list_sessions(self, *, movie_id: str = "") -> dict[str, Any]:
        normalized_movie_id = str(movie_id or "").strip()
        sessions = self.store.list_sessions()
        if normalized_movie_id:
            sessions = [item for item in sessions if str(item.get("movie_id") or "").strip() == normalized_movie_id]
        return {"ok": True, "sessions": sessions}

    def _session_id(
        self,
        *,
        movie_id: str,
        source_fingerprint: str,
        handoff_mode: str,
        preferred_runtime: str,
    ) -> str:
        payload = "|".join(
            [
                normalize_token(movie_id),
                normalize_token(source_fingerprint),
                normalize_token(handoff_mode),
                normalize_token(preferred_runtime),
            ]
        )
        if not payload.strip("|"):
            payload = "session"
        return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:20]

    def _movie_id(self, movie: Mapping[str, Any]) -> str:
        explicit = str(movie.get("movie_id") or movie.get("id") or movie.get("imdb_id") or "").strip()
        if explicit:
            return explicit
        title = self._movie_name(movie)
        year = str(movie.get("year") or "").strip()
        slug = normalize_token(f"{title}-{year}").replace(" ", "-")
        return slug or "unknown-movie"

    def _movie_name(self, movie: Mapping[str, Any]) -> str:
        return str(movie.get("title") or movie.get("name") or "").strip() or "unknown"

    def _fallback_source_fingerprint(self, source: Mapping[str, Any]) -> str:
        payload = "|".join(
            [
                normalize_token(source.get("magnet")),
                normalize_token(source.get("title")),
                normalize_token(source.get("source")),
            ]
        )
        if not payload.strip("|"):
            return "malformed-source"
        return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]

    def _handoff_mode(self, requested_mode: str, policy: Mapping[str, Any]) -> str:
        normalized_mode = str(requested_mode or "").strip().lower()
        if normalized_mode:
            return normalized_mode
        if policy.get("allowed_for_browser"):
            return "browser_handoff"
        if policy.get("mobile_safe"):
            return "mobile_handoff"
        if policy.get("external_only"):
            return "external_handoff"
        if policy.get("blocked_reason"):
            return "blocked"
        return "external_handoff"

    def _emit_session_event(self, session: Mapping[str, Any]) -> None:
        emit_event(
            "[stream-session]",
            movie=str(session.get("movie_title") or "unknown"),
            state=normalize_session_state(session.get("session_state")),
            runtime=str(session.get("runtime_intent") or "unknown"),
            playback_runtime=str(session.get("playback_runtime") or "unknown"),
            profile=str(session.get("runtime_profile") or "unknown"),
            session=str(session.get("session_id") or "").strip(),
        )

    def _is_hard_blocked(self, policy: Mapping[str, Any]) -> bool:
        return str(policy.get("blocked_reason") or "").strip() in HARD_BLOCK_REASONS

    def _track_handoff_analytics(self, session: Mapping[str, Any]) -> None:
        runtime_intent = str(session.get("runtime_intent") or "").strip()
        if runtime_intent == "browser_stream":
            self.analytics_service.track_session_event("browser_attempted", session=session)
        if normalize_session_state(session.get("session_state")) == "failed":
            self.analytics_service.track_session_event("session_handoff_failed", session=session)
            return
        self.analytics_service.track_session_event("session_handoff_success", session=session)
        if runtime_intent == "external_player":
            self.analytics_service.track_session_event("external_player_used", session=session)
        elif runtime_intent == "mobile_handoff":
            self.analytics_service.track_session_event("mobile_handoff_used", session=session)

    def _runtime_id(self, session: Mapping[str, Any]) -> str:
        manifest = dict(session.get("runtime_manifest") or {})
        return str(manifest.get("runtime_id") or "").strip()

    def _refresh_runtime_manifest(self, session: Mapping[str, Any]) -> dict[str, Any]:
        manifest = dict(session.get("runtime_manifest") or {})
        return build_runtime_manifest(
            runtime_id=str(manifest.get("runtime_id") or "").strip(),
            session_id=str(session.get("session_id") or "").strip(),
            selected_source=dict(session.get("selected_source") or {}),
            runtime_mode=str(session.get("runtime_mode") or ""),
            runtime_state=str(session.get("runtime_state") or ""),
            startup_confidence=str(session.get("startup_confidence") or ""),
            capability_snapshot=dict(session.get("readiness_snapshot") or {}),
            diagnostics=dict(manifest.get("diagnostics") or {}),
            fallbacks=list(manifest.get("fallbacks") or []),
            preflight=dict(session.get("runtime_preflight") or {}),
            transport=dict(manifest.get("transport") or {}),
            created_at=str(manifest.get("created_at") or ""),
        )

    def _sync_runtime_registry(self, session: Mapping[str, Any]) -> None:
        runtime_id = self._runtime_id(session)
        if not runtime_id:
            return
        self.runtime_registry.update(
            runtime_id,
            {
                "session_id": str(session.get("session_id") or ""),
                "selected_source": dict(session.get("selected_source") or {}),
                "runtime_profile": str(session.get("runtime_profile") or ""),
                "runtime_state": str(session.get("runtime_state") or ""),
                "startup_confidence": str(session.get("startup_confidence") or ""),
            },
        )

    def _refresh_browser_runtime_session(self, session: Mapping[str, Any]) -> dict[str, Any]:
        current = dict(session.get("browser_runtime_session") or {})
        bridge = dict(session.get("browser_runtime_bridge") or {})
        refreshed = build_browser_runtime_session(
            linked_stream_runtime_id=self._runtime_id(session),
            playback_session_id=str(session.get("session_id") or ""),
            runtime_state=str(session.get("runtime_state") or ""),
            capability_snapshot=dict(bridge.get("capability_snapshot") or {}),
            bootstrap_summary=dict(bridge.get("bootstrap") or {}),
            execution_state=str(session.get("execution_state") or ""),
            execution_metrics=dict(session.get("execution_metrics") or {}),
            execution_timeline=dict(session.get("execution_timeline") or {}),
            simulated_runtime_health=str(session.get("simulated_runtime_health") or ""),
            recovery_path=dict(session.get("recovery_path") or {}),
            execution_events=list(session.get("execution_events") or []),
            coordination_state=str(session.get("coordination_state") or ""),
            coordination_metrics=dict(session.get("coordination_metrics") or {}),
        )
        if current.get("runtime_session_id"):
            refreshed["runtime_session_id"] = str(current.get("runtime_session_id") or "")
        if current.get("created_at"):
            refreshed["created_at"] = str(current.get("created_at") or "")
        if current.get("expires_at"):
            refreshed["expires_at"] = str(current.get("expires_at") or "")
        return refreshed

    def _sync_browser_runtime_session(self, session: Mapping[str, Any]) -> None:
        runtime_session = dict(session.get("browser_runtime_session") or {})
        runtime_session_id = str(runtime_session.get("runtime_session_id") or "").strip()
        if not runtime_session_id:
            return
        if self.browser_runtime_session_registry.get(runtime_session_id):
            self.browser_runtime_session_registry.update(runtime_session_id, runtime_session)
            return
        self.browser_runtime_session_registry.create(runtime_session)
