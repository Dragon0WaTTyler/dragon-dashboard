from __future__ import annotations

from typing import Any


def build_runtime_ecology(
    *,
    runtime_resilience: int = 0,
    degradation_risk: int = 0,
    fallback_probability: float = 0.0,
    authority_state: str = "",
    cluster_alignment: str = "",
) -> dict[str, Any]:
    relationships: list[str] = []
    if runtime_resilience >= 70:
        relationships.append("resilience_dependency")
    if fallback_probability >= 0.56:
        relationships.append("fallback_dominance")
    if degradation_risk >= 58:
        relationships.append("browser_fragility")
    if cluster_alignment in {"harmonic", "adaptive"}:
        relationships.append("coordination_harmonics")
    if authority_state in {"approved", "guarded"}:
        relationships.append("authority_stabilization")
    if degradation_risk >= 52 and runtime_resilience <= 56:
        relationships.append("capability_imbalance")
    return {
        "ecology_state": relationships[0] if relationships else "coordination_harmonics",
        "relationships": relationships or ["coordination_harmonics"],
        "ecology_tension": _ecology_tension(relationships),
    }


def _ecology_tension(relationships: list[str]) -> str:
    relation_set = set(relationships)
    if "fallback_dominance" in relation_set and "capability_imbalance" in relation_set:
        return "elevated"
    if "browser_fragility" in relation_set:
        return "watchful"
    return "stable"
