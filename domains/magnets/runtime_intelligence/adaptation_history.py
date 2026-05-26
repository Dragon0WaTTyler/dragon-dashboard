from __future__ import annotations

from typing import Any, Mapping


def build_adaptation_history(
    runtime_memory_summary: Mapping[str, Any] | None,
    *,
    current_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    summary = dict(runtime_memory_summary or {})
    context = dict(current_context or {})
    switch_history = [dict(item) for item in context.get("runtime_switch_history") or [] if isinstance(item, Mapping)]
    traces = []
    for index, item in enumerate(switch_history, start=1):
        traces.append(
            {
                "step": index,
                "from_runtime": str(item.get("current_runtime") or "").strip() or "unknown",
                "to_runtime": str(item.get("target_runtime") or "").strip() or "unknown",
                "switch_strategy": str(item.get("switch_strategy") or "").strip() or "retain_runtime",
            }
        )
    chains = dict(summary.get("adaptation_chains") or {})
    dominant_chain = max(chains.items(), key=lambda item: (int(item[1] or 0), str(item[0])), default=("retain_runtime", 0))
    switch_frequency = sum(int(value or 0) for value in chains.values()) + len(switch_history)
    return {
        "downgrade_chains": chains,
        "switch_frequency": switch_frequency,
        "recovery_sequences": _count_sequences(summary, "recovered"),
        "fallback_escalations": _count_sequences(summary, "fallback"),
        "adaptation_summaries": [
            f"dominant_chain:{dominant_chain[0]}",
            f"observed_switches:{switch_frequency}",
        ],
        "runtime_evolution_trace": traces or [{"step": 1, "from_runtime": str(context.get("playback_runtime") or "unknown"), "to_runtime": str(context.get("playback_runtime") or "unknown"), "switch_strategy": "retain_runtime"}],
    }


def _count_sequences(summary: Mapping[str, Any], key: str) -> int:
    total = 0
    for payload in dict(summary.get("playback_runtimes") or {}).values():
        total += int(dict(payload or {}).get(key, 0) or 0)
    return total
