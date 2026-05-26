from __future__ import annotations

from typing import Any, Mapping


def build_runtime_clusters(
    *,
    runtime_profile: str = "",
    playback_runtime: str = "",
    startup_confidence: str = "",
    runtime_resilience: int = 0,
    degradation_risk: int = 0,
    fallback_pressure: int = 0,
    adaptation_pressure: int = 0,
    authority_state: str = "",
) -> dict[str, Any]:
    clusters: list[str] = []
    if "cinematic" in runtime_profile or playback_runtime == "browser_runtime":
        clusters.append("cinematic_cluster")
    if degradation_risk >= 60 or fallback_pressure >= 58:
        clusters.append("fallback_cluster")
    if adaptation_pressure >= 56:
        clusters.append("adaptive_cluster")
    if runtime_resilience >= 72:
        clusters.append("resilience_cluster")
    if startup_confidence == "low" or authority_state == "guarded":
        clusters.append("constrained_cluster")
    if degradation_risk >= 72 or playback_runtime == "blocked":
        clusters.append("volatile_cluster")
    if not clusters:
        clusters.append("constrained_cluster")
    return {
        "primary_cluster": clusters[0],
        "clusters": clusters,
        "cluster_density": len(clusters),
        "cluster_alignment": _cluster_alignment(clusters),
    }


def _cluster_alignment(clusters: list[str]) -> str:
    cluster_set = set(clusters)
    if {"cinematic_cluster", "resilience_cluster"} <= cluster_set and "volatile_cluster" not in cluster_set:
        return "harmonic"
    if "volatile_cluster" in cluster_set and "fallback_cluster" in cluster_set:
        return "fragmented"
    if "adaptive_cluster" in cluster_set:
        return "adaptive"
    return "stable"
