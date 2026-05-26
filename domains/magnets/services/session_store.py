from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

from dragon.cache import load_json_file, save_json_file
from dragon.paths import CACHE_DIR

from ..sessions import StreamSession, normalize_session_state

DEFAULT_SESSION_PAYLOAD = {
    "version": 1,
    "sessions": {},
}


class StreamSessionStore:
    def __init__(self, path: Path | None = None) -> None:
        self.path = Path(path or (CACHE_DIR / "magnets" / "sessions.json"))
        self._lock = threading.Lock()

    def list_sessions(self) -> list[dict[str, Any]]:
        with self._lock:
            payload = self._load()
        sessions = list((payload.get("sessions") or {}).values())
        sessions.sort(key=lambda item: str(item.get("updated_at") or ""), reverse=True)
        return sessions

    def get_session(self, session_id: str) -> dict[str, Any] | None:
        normalized_session_id = str(session_id or "").strip()
        if not normalized_session_id:
            return None
        with self._lock:
            payload = self._load()
            session = (payload.get("sessions") or {}).get(normalized_session_id)
            return dict(session) if isinstance(session, dict) else None

    def save_session(self, session: StreamSession | dict[str, Any]) -> dict[str, Any]:
        session_payload = session.to_dict() if isinstance(session, StreamSession) else dict(session or {})
        session_id = str(session_payload.get("session_id") or "").strip()
        if not session_id:
            raise ValueError("session_id is required")
        with self._lock:
            payload = self._load()
            payload["sessions"][session_id] = self._normalize_session(session_payload)
            save_json_file(self.path, payload)
            return dict(payload["sessions"][session_id])

    def delete_session(self, session_id: str) -> bool:
        normalized_session_id = str(session_id or "").strip()
        if not normalized_session_id:
            return False
        with self._lock:
            payload = self._load()
            sessions = payload.get("sessions") or {}
            if normalized_session_id not in sessions:
                return False
            sessions.pop(normalized_session_id, None)
            return save_json_file(self.path, payload)

    def _load(self) -> dict[str, Any]:
        payload = load_json_file(self.path, DEFAULT_SESSION_PAYLOAD)
        if not isinstance(payload, dict):
            payload = {}
        sessions_payload = payload.get("sessions")
        normalized_sessions: dict[str, dict[str, Any]] = {}
        if isinstance(sessions_payload, dict):
            items = sessions_payload.items()
        else:
            items = []
            if isinstance(sessions_payload, list):
                items = (
                    (str(item.get("session_id") or "").strip(), item)
                    for item in sessions_payload
                    if isinstance(item, dict)
                )
        for session_id, session in items:
            normalized_id = str(session_id or "").strip()
            if not normalized_id or not isinstance(session, dict):
                continue
            normalized_sessions[normalized_id] = self._normalize_session(session)
        return {
            "version": 1,
            "sessions": normalized_sessions,
        }

    def _normalize_session(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = StreamSession(
            session_id=str(payload.get("session_id") or "").strip(),
            movie_id=str(payload.get("movie_id") or "").strip(),
            source_fingerprint=str(payload.get("source_fingerprint") or "").strip(),
            handoff_mode=str(payload.get("handoff_mode") or "").strip(),
            preferred_runtime=str(payload.get("preferred_runtime") or "").strip(),
            session_state=normalize_session_state(payload.get("session_state")),
            compatibility_snapshot=dict(payload.get("compatibility_snapshot") or {}),
            created_at=str(payload.get("created_at") or "").strip(),
            updated_at=str(payload.get("updated_at") or "").strip(),
            runtime_intent=str(payload.get("runtime_intent") or "").strip(),
            admission_policy=dict(payload.get("admission_policy") or {}),
            movie_title=str(payload.get("movie_title") or "").strip(),
            failure_reason=str(payload.get("failure_reason") or "").strip(),
            playback_runtime=str(payload.get("playback_runtime") or "").strip(),
            runtime_mode=str(payload.get("runtime_mode") or "").strip(),
            runtime_state=str(payload.get("runtime_state") or "").strip(),
            runtime_profile=str(payload.get("runtime_profile") or "").strip(),
            selected_source=dict(payload.get("selected_source") or {}),
            playback_readiness=str(payload.get("playback_readiness") or "").strip(),
            startup_confidence=str(payload.get("startup_confidence") or "").strip(),
            runtime_warnings=list(payload.get("runtime_warnings") or []),
            runtime_preflight=dict(payload.get("runtime_preflight") or {}),
            runtime_manifest=dict(payload.get("runtime_manifest") or {}),
            runtime_events=[dict(item) for item in payload.get("runtime_events") or [] if isinstance(item, dict)],
            browser_runtime_bridge=dict(payload.get("browser_runtime_bridge") or {}),
            browser_runtime_session=dict(payload.get("browser_runtime_session") or {}),
            readiness_snapshot=dict(payload.get("readiness_snapshot") or {}),
            execution_state=str(payload.get("execution_state") or "").strip(),
            execution_metrics=dict(payload.get("execution_metrics") or {}),
            execution_timeline=dict(payload.get("execution_timeline") or {}),
            runtime_grade=dict(payload.get("runtime_grade") or {}),
            simulated_runtime_health=str(payload.get("simulated_runtime_health") or "").strip(),
            recovery_path=dict(payload.get("recovery_path") or {}),
            execution_events=[dict(item) for item in payload.get("execution_events") or [] if isinstance(item, dict)],
            coordination_state=str(payload.get("coordination_state") or "").strip(),
            coordination_metrics=dict(payload.get("coordination_metrics") or {}),
            orchestration_graph=dict(payload.get("orchestration_graph") or {}),
            runtime_negotiation=dict(payload.get("runtime_negotiation") or {}),
            adaptive_strategy=dict(payload.get("adaptive_strategy") or {}),
            runtime_switch_history=[dict(item) for item in payload.get("runtime_switch_history") or [] if isinstance(item, dict)],
            fallback_negotiation=dict(payload.get("fallback_negotiation") or {}),
            coordination_events=[dict(item) for item in payload.get("coordination_events") or [] if isinstance(item, dict)],
            runtime_predictions=dict(payload.get("runtime_predictions") or {}),
            runtime_memory_summary=dict(payload.get("runtime_memory_summary") or {}),
            adaptation_history=dict(payload.get("adaptation_history") or {}),
            confidence_evolution=dict(payload.get("confidence_evolution") or {}),
            runtime_reputation=dict(payload.get("runtime_reputation") or {}),
            orchestration_forecast=dict(payload.get("orchestration_forecast") or {}),
            intelligence_metrics=dict(payload.get("intelligence_metrics") or {}),
            historical_patterns=[dict(item) for item in payload.get("historical_patterns") or [] if isinstance(item, dict)],
            runtime_learning=dict(payload.get("runtime_learning") or {}),
            intelligence_events=[dict(item) for item in payload.get("intelligence_events") or [] if isinstance(item, dict)],
            authority_state=str(payload.get("authority_state") or "").strip(),
            authority_confidence=int(payload.get("authority_confidence", 0) or 0),
            authority_reasoning=[str(item) for item in payload.get("authority_reasoning") or [] if str(item or "").strip()],
            runtime_risk=dict(payload.get("runtime_risk") or {}),
            arbitration_result=dict(payload.get("arbitration_result") or {}),
            arbitration_trace=[dict(item) for item in payload.get("arbitration_trace") or [] if isinstance(item, dict)],
            governance_actions=[str(item) for item in payload.get("governance_actions") or [] if str(item or "").strip()],
            stability_state=dict(payload.get("stability_state") or {}),
            execution_policy=dict(payload.get("execution_policy") or {}),
            forced_constraints=[dict(item) for item in payload.get("forced_constraints") or [] if isinstance(item, dict)],
            blocked_paths=[str(item) for item in payload.get("blocked_paths") or [] if str(item or "").strip()],
            forced_fallback=bool(payload.get("forced_fallback")),
            fallback_authority=dict(payload.get("fallback_authority") or {}),
            confidence_governance=dict(payload.get("confidence_governance") or {}),
            authority_memory_summary=dict(payload.get("authority_memory_summary") or {}),
            authority_events=[dict(item) for item in payload.get("authority_events") or [] if isinstance(item, dict)],
            authority_metrics=dict(payload.get("authority_metrics") or {}),
            runtime_identity=dict(payload.get("runtime_identity") or {}),
            orchestration_archetype=str(payload.get("orchestration_archetype") or "").strip(),
            runtime_temperament=str(payload.get("runtime_temperament") or "").strip(),
            adaptation_profile=str(payload.get("adaptation_profile") or "").strip(),
            behavioral_drift=dict(payload.get("behavioral_drift") or {}),
            continuity_state=dict(payload.get("continuity_state") or {}),
            identity_confidence=int(payload.get("identity_confidence", 0) or 0),
            identity_forecast=dict(payload.get("identity_forecast") or {}),
            persistent_traits=[str(item) for item in payload.get("persistent_traits") or [] if str(item or "").strip()],
            orchestration_traits=[str(item) for item in payload.get("orchestration_traits") or [] if str(item or "").strip()],
            identity_metrics=dict(payload.get("identity_metrics") or {}),
            identity_warnings=[str(item) for item in payload.get("identity_warnings") or [] if str(item or "").strip()],
            identity_events=[dict(item) for item in payload.get("identity_events") or [] if isinstance(item, dict)],
            runtime_ecosystem=dict(payload.get("runtime_ecosystem") or {}),
            ecosystem_balance=dict(payload.get("ecosystem_balance") or {}),
            orchestration_pressure=dict(payload.get("orchestration_pressure") or {}),
            runtime_clusters=dict(payload.get("runtime_clusters") or {}),
            stability_zone=dict(payload.get("stability_zone") or {}),
            ecosystem_climate=dict(payload.get("ecosystem_climate") or {}),
            degradation_currents=dict(payload.get("degradation_currents") or {}),
            resilience_topology=dict(payload.get("resilience_topology") or {}),
            adaptive_equilibrium=dict(payload.get("adaptive_equilibrium") or {}),
            ecosystem_forecast=dict(payload.get("ecosystem_forecast") or {}),
            ecosystem_governance=dict(payload.get("ecosystem_governance") or {}),
            ecosystem_metrics=dict(payload.get("ecosystem_metrics") or {}),
            ecosystem_events=[dict(item) for item in payload.get("ecosystem_events") or [] if isinstance(item, dict)],
            ecosystem_memory=dict(payload.get("ecosystem_memory") or {}),
            runtime_cinema=dict(payload.get("runtime_cinema") or {}),
            cinematic_direction=dict(payload.get("cinematic_direction") or {}),
            runtime_pacing=dict(payload.get("runtime_pacing") or {}),
            immersion_state=dict(payload.get("immersion_state") or {}),
            runtime_atmosphere=dict(payload.get("runtime_atmosphere") or {}),
            dramatic_tension=dict(payload.get("dramatic_tension") or {}),
            continuity_cinema=dict(payload.get("continuity_cinema") or {}),
            orchestration_mood=dict(payload.get("orchestration_mood") or {}),
            scene_energy=dict(payload.get("scene_energy") or {}),
            cinematic_balance=dict(payload.get("cinematic_balance") or {}),
            runtime_aesthetics=dict(payload.get("runtime_aesthetics") or {}),
            cinematic_forecast=dict(payload.get("cinematic_forecast") or {}),
            cinematic_governance=dict(payload.get("cinematic_governance") or {}),
            cinematic_metrics=dict(payload.get("cinematic_metrics") or {}),
            cinematic_events=[dict(item) for item in payload.get("cinematic_events") or [] if isinstance(item, dict)],
            cinematic_memory=dict(payload.get("cinematic_memory") or {}),
            runtime_consciousness=dict(payload.get("runtime_consciousness") or {}),
            awareness_state=dict(payload.get("awareness_state") or {}),
            orchestration_attention=dict(payload.get("orchestration_attention") or {}),
            continuity_awareness=dict(payload.get("continuity_awareness") or {}),
            runtime_reflection=dict(payload.get("runtime_reflection") or {}),
            orchestration_intuition=dict(payload.get("orchestration_intuition") or {}),
            cognitive_balance=dict(payload.get("cognitive_balance") or {}),
            awareness_pressure=dict(payload.get("awareness_pressure") or {}),
            orchestration_focus=dict(payload.get("orchestration_focus") or {}),
            runtime_presence=dict(payload.get("runtime_presence") or {}),
            orchestration_perception=dict(payload.get("orchestration_perception") or {}),
            consciousness_forecast=dict(payload.get("consciousness_forecast") or {}),
            consciousness_governance=dict(payload.get("consciousness_governance") or {}),
            consciousness_metrics=dict(payload.get("consciousness_metrics") or {}),
            consciousness_events=[dict(item) for item in payload.get("consciousness_events") or [] if isinstance(item, dict)],
            consciousness_memory=dict(payload.get("consciousness_memory") or {}),
            runtime_instinct=dict(payload.get("runtime_instinct") or {}),
            stabilization_instinct=dict(payload.get("stabilization_instinct") or {}),
            resilience_instinct=dict(payload.get("resilience_instinct") or {}),
            fallback_instinct=dict(payload.get("fallback_instinct") or {}),
            continuity_instinct=dict(payload.get("continuity_instinct") or {}),
            cinematic_instinct=dict(payload.get("cinematic_instinct") or {}),
            equilibrium_instinct=dict(payload.get("equilibrium_instinct") or {}),
            orchestration_reflexes=dict(payload.get("orchestration_reflexes") or {}),
            instinct_pressure=dict(payload.get("instinct_pressure") or {}),
            adaptive_instinct=dict(payload.get("adaptive_instinct") or {}),
            runtime_survival=dict(payload.get("runtime_survival") or {}),
            instinct_forecast=dict(payload.get("instinct_forecast") or {}),
            instinct_governance=dict(payload.get("instinct_governance") or {}),
            instinct_metrics=dict(payload.get("instinct_metrics") or {}),
            instinct_events=[dict(item) for item in payload.get("instinct_events") or [] if isinstance(item, dict)],
            instinct_memory=dict(payload.get("instinct_memory") or {}),
            runtime_subconscious=dict(payload.get("runtime_subconscious") or {}),
            latent_patterns=dict(payload.get("latent_patterns") or {}),
            orchestration_underflow=dict(payload.get("orchestration_underflow") or {}),
            hidden_equilibrium=dict(payload.get("hidden_equilibrium") or {}),
            subconscious_pressure=dict(payload.get("subconscious_pressure") or {}),
            silent_adaptation=dict(payload.get("silent_adaptation") or {}),
            continuity_underlayers=dict(payload.get("continuity_underlayers") or {}),
            orchestration_residue=dict(payload.get("orchestration_residue") or {}),
            dormant_resilience=dict(payload.get("dormant_resilience") or {}),
            cinematic_underflow=dict(payload.get("cinematic_underflow") or {}),
            orchestration_echoes=dict(payload.get("orchestration_echoes") or {}),
            subconscious_forecast=dict(payload.get("subconscious_forecast") or {}),
            subconscious_governance=dict(payload.get("subconscious_governance") or {}),
            subconscious_metrics=dict(payload.get("subconscious_metrics") or {}),
            subconscious_events=[dict(item) for item in payload.get("subconscious_events") or [] if isinstance(item, dict)],
            subconscious_memory=dict(payload.get("subconscious_memory") or {}),
            runtime_dreaming=dict(payload.get("runtime_dreaming") or {}),
            cinematic_dreams=dict(payload.get("cinematic_dreams") or {}),
            orchestration_visions=dict(payload.get("orchestration_visions") or {}),
            latent_projection=dict(payload.get("latent_projection") or {}),
            stabilization_dreams=dict(payload.get("stabilization_dreams") or {}),
            resilience_dreams=dict(payload.get("resilience_dreams") or {}),
            continuity_dreams=dict(payload.get("continuity_dreams") or {}),
            subconscious_projection=dict(payload.get("subconscious_projection") or {}),
            dormant_pathways=dict(payload.get("dormant_pathways") or {}),
            adaptive_dreaming=dict(payload.get("adaptive_dreaming") or {}),
            runtime_mirroring=dict(payload.get("runtime_mirroring") or {}),
            dream_forecast=dict(payload.get("dream_forecast") or {}),
            dream_governance=dict(payload.get("dream_governance") or {}),
            dream_metrics=dict(payload.get("dream_metrics") or {}),
            dream_events=[dict(item) for item in payload.get("dream_events") or [] if isinstance(item, dict)],
            dreaming_memory=dict(payload.get("dreaming_memory") or {}),
            runtime_federation=dict(payload.get("runtime_federation") or {}),
            federation_state=dict(payload.get("federation_state") or {}),
            federation_projection=dict(payload.get("federation_projection") or {}),
            federation_forecast=dict(payload.get("federation_forecast") or {}),
            federation_governance=dict(payload.get("federation_governance") or {}),
            federation_continuity=dict(payload.get("federation_continuity") or {}),
            federation_metrics=dict(payload.get("federation_metrics") or {}),
            federation_events=[dict(item) for item in payload.get("federation_events") or [] if isinstance(item, dict)],
            federation_coherence=int(payload.get("federation_coherence", 0) or 0),
            federation_harmony=int(payload.get("federation_harmony", 0) or 0),
            federation_pressure=int(payload.get("federation_pressure", 0) or 0),
            federation_integrity=int(payload.get("federation_integrity", 0) or 0),
            federation_resilience=int(payload.get("federation_resilience", 0) or 0),
            federation_alignment=int(payload.get("federation_alignment", 0) or 0),
            federation_divergence=int(payload.get("federation_divergence", 0) or 0),
            runtime_continuity_profile=str(payload.get("runtime_continuity_profile") or "").strip(),
            cinematic_runtime_state=str(payload.get("cinematic_runtime_state") or "").strip(),
            orchestration_unity=str(payload.get("orchestration_unity") or "").strip(),
            adaptive_federation_balance=int(payload.get("adaptive_federation_balance", 0) or 0),
            runtime_phase_transition=str(payload.get("runtime_phase_transition") or "").strip(),
            continuity_projection=str(payload.get("continuity_projection") or "").strip(),
            federation_memory_summary=dict(payload.get("federation_memory_summary") or {}),
            runtime_temporal=dict(payload.get("runtime_temporal") or {}),
            temporal_state=dict(payload.get("temporal_state") or {}),
            temporal_phase=dict(payload.get("temporal_phase") or {}),
            temporal_rhythm=dict(payload.get("temporal_rhythm") or {}),
            temporal_forecast=dict(payload.get("temporal_forecast") or {}),
            temporal_continuity=dict(payload.get("temporal_continuity") or {}),
            temporal_decay=dict(payload.get("temporal_decay") or {}),
            temporal_recovery=dict(payload.get("temporal_recovery") or {}),
            temporal_governance=dict(payload.get("temporal_governance") or {}),
            temporal_metrics=dict(payload.get("temporal_metrics") or {}),
            temporal_events=[dict(item) for item in payload.get("temporal_events") or [] if isinstance(item, dict)],
            temporal_integrity=int(payload.get("temporal_integrity", 0) or 0),
            temporal_alignment=int(payload.get("temporal_alignment", 0) or 0),
            temporal_stability=int(payload.get("temporal_stability", 0) or 0),
            temporal_momentum=int(payload.get("temporal_momentum", 0) or 0),
            temporal_pressure=int(payload.get("temporal_pressure", 0) or 0),
            continuity_decay_rate=int(payload.get("continuity_decay_rate", 0) or 0),
            runtime_rhythm_state=str(payload.get("runtime_rhythm_state") or "").strip(),
            cinematic_temporal_flow=str(payload.get("cinematic_temporal_flow") or "").strip(),
            orchestration_phase_velocity=str(payload.get("orchestration_phase_velocity") or "").strip(),
            adaptive_temporal_balance=int(payload.get("adaptive_temporal_balance", 0) or 0),
            runtime_cycle_phase=str(payload.get("runtime_cycle_phase") or "").strip(),
            temporal_projection=str(payload.get("temporal_projection") or "").strip(),
            temporal_memory_summary=dict(payload.get("temporal_memory_summary") or {}),
            runtime_resonance=dict(payload.get("runtime_resonance") or {}),
            resonance_state=dict(payload.get("resonance_state") or {}),
            resonance_harmony=dict(payload.get("resonance_harmony") or {}),
            resonance_sync=dict(payload.get("resonance_sync") or {}),
            resonance_projection=dict(payload.get("resonance_projection") or {}),
            resonance_equilibrium=dict(payload.get("resonance_equilibrium") or {}),
            resonance_governance=dict(payload.get("resonance_governance") or {}),
            resonance_metrics=dict(payload.get("resonance_metrics") or {}),
            resonance_events=[dict(item) for item in payload.get("resonance_events") or [] if isinstance(item, dict)],
            resonance_recovery=dict(payload.get("resonance_recovery") or {}),
            resonance_integrity=int(payload.get("resonance_integrity", 0) or 0),
            resonance_alignment=int(payload.get("resonance_alignment", 0) or 0),
            resonance_stability=int(payload.get("resonance_stability", 0) or 0),
            resonance_pressure=int(payload.get("resonance_pressure", 0) or 0),
            resonance_fragmentation=int(payload.get("resonance_fragmentation", 0) or 0),
            resonance_cohesion=int(payload.get("resonance_cohesion", 0) or 0),
            harmonic_runtime_state=str(payload.get("harmonic_runtime_state") or "").strip(),
            cinematic_resonance=str(payload.get("cinematic_resonance") or "").strip(),
            orchestration_resonance=str(payload.get("orchestration_resonance") or "").strip(),
            adaptive_sync_balance=int(payload.get("adaptive_sync_balance", 0) or 0),
            resonance_phase=str(payload.get("resonance_phase") or "").strip(),
            sync_drift=int(payload.get("sync_drift", 0) or 0),
            runtime_harmony_index=int(payload.get("runtime_harmony_index", 0) or 0),
            resonance_memory_summary=dict(payload.get("resonance_memory_summary") or {}),
            runtime_symbiosis=dict(payload.get("runtime_symbiosis") or {}),
            symbiosis_state=dict(payload.get("symbiosis_state") or {}),
            symbiosis_balance=dict(payload.get("symbiosis_balance") or {}),
            symbiosis_cooperation=dict(payload.get("symbiosis_cooperation") or {}),
            symbiosis_dependencies=dict(payload.get("symbiosis_dependencies") or {}),
            symbiosis_recovery=dict(payload.get("symbiosis_recovery") or {}),
            symbiosis_projection=dict(payload.get("symbiosis_projection") or {}),
            symbiosis_equilibrium=dict(payload.get("symbiosis_equilibrium") or {}),
            symbiosis_governance=dict(payload.get("symbiosis_governance") or {}),
            symbiosis_metrics=dict(payload.get("symbiosis_metrics") or {}),
            symbiosis_events=[dict(item) for item in payload.get("symbiosis_events") or [] if isinstance(item, dict)],
            symbiosis_integrity=int(payload.get("symbiosis_integrity", 0) or 0),
            symbiosis_alignment=int(payload.get("symbiosis_alignment", 0) or 0),
            symbiosis_stability=int(payload.get("symbiosis_stability", 0) or 0),
            symbiosis_pressure=int(payload.get("symbiosis_pressure", 0) or 0),
            symbiosis_mutualism=int(payload.get("symbiosis_mutualism", 0) or 0),
            symbiosis_fragmentation=int(payload.get("symbiosis_fragmentation", 0) or 0),
            cooperative_runtime_state=str(payload.get("cooperative_runtime_state") or "").strip(),
            runtime_coexistence=str(payload.get("runtime_coexistence") or "").strip(),
            adaptive_mutual_balance=str(payload.get("adaptive_mutual_balance") or "").strip(),
            systemic_runtime_health=str(payload.get("systemic_runtime_health") or "").strip(),
            recovery_cohesion=str(payload.get("recovery_cohesion") or "").strip(),
            dependency_stress=int(payload.get("dependency_stress", 0) or 0),
            symbiotic_phase=str(payload.get("symbiotic_phase") or "").strip(),
            symbiosis_memory_summary=dict(payload.get("symbiosis_memory_summary") or {}),
        )
        if not normalized.created_at:
            normalized.created_at = normalized.updated_at or ""
        if not normalized.updated_at:
            normalized.updated_at = normalized.created_at or ""
        return normalized.to_dict()
