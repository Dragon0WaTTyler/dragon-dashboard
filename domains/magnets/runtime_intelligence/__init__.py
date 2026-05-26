from .adaptation_history import build_adaptation_history
from .confidence_evolution import build_confidence_evolution
from .historical_patterns import build_historical_patterns
from .intelligence_events import build_intelligence_events
from .intelligence_metrics import build_intelligence_metrics
from .prediction_engine import predict_runtime_outcome
from .runtime_forecasting import forecast_runtime_behavior
from .runtime_learning import build_runtime_learning
from .runtime_memory import (
    build_runtime_memory_summary,
    extract_runtime_memory_record,
    load_runtime_memory,
    update_runtime_memory,
)
from .runtime_reputation import build_runtime_reputation


def build_runtime_intelligence(
    orchestration: dict | None,
    *,
    persist_memory: bool = True,
    memory_path=None,
    timestamp: str = "",
) -> dict:
    payload = dict(orchestration or {})
    memory_summary = (
        update_runtime_memory(payload, path=memory_path, timestamp=timestamp)
        if persist_memory
        else build_runtime_memory_summary(load_runtime_memory(path=memory_path), current_context=payload)
    )
    historical_patterns = build_historical_patterns(memory_summary, current_context=payload)
    runtime_learning = build_runtime_learning(memory_summary, current_context=payload, historical_patterns=historical_patterns)
    runtime_reputation = build_runtime_reputation(memory_summary, current_context=payload)
    runtime_predictions = predict_runtime_outcome(
        execution_metrics=payload.get("execution_metrics"),
        coordination_metrics=payload.get("coordination_metrics"),
        runtime_history=memory_summary,
        capability_snapshot=payload.get("capability_snapshot") or payload.get("readiness_snapshot"),
        readiness_snapshot=payload.get("readiness_snapshot"),
        runtime_learning=runtime_learning,
        runtime_reputation=runtime_reputation,
    )
    adaptation_history = build_adaptation_history(memory_summary, current_context=payload)
    confidence_evolution = build_confidence_evolution(
        current_context=payload,
        runtime_predictions=runtime_predictions,
        runtime_learning=runtime_learning,
    )
    orchestration_forecast = forecast_runtime_behavior(
        runtime_predictions=runtime_predictions,
        historical_patterns=historical_patterns,
        runtime_reputation=runtime_reputation,
        confidence_evolution=confidence_evolution,
        current_context=payload,
    )
    intelligence_metrics = build_intelligence_metrics(
        memory_summary=memory_summary,
        runtime_predictions=runtime_predictions,
        confidence_evolution=confidence_evolution,
        runtime_reputation=runtime_reputation,
        adaptation_history=adaptation_history,
        orchestration_forecast=orchestration_forecast,
    )
    intelligence_events = build_intelligence_events(
        memory_summary=memory_summary,
        historical_patterns=historical_patterns,
        runtime_predictions=runtime_predictions,
        confidence_evolution=confidence_evolution,
        adaptation_history=adaptation_history,
        orchestration_forecast=orchestration_forecast,
    )
    return {
        "runtime_memory_summary": memory_summary,
        "historical_patterns": historical_patterns,
        "runtime_learning": runtime_learning,
        "runtime_reputation": runtime_reputation,
        "runtime_predictions": runtime_predictions,
        "adaptation_history": adaptation_history,
        "confidence_evolution": confidence_evolution,
        "orchestration_forecast": orchestration_forecast,
        "intelligence_metrics": intelligence_metrics,
        "intelligence_events": intelligence_events,
    }


__all__ = [
    "build_adaptation_history",
    "build_confidence_evolution",
    "build_historical_patterns",
    "build_intelligence_events",
    "build_intelligence_metrics",
    "build_runtime_intelligence",
    "build_runtime_learning",
    "build_runtime_memory_summary",
    "build_runtime_reputation",
    "extract_runtime_memory_record",
    "forecast_runtime_behavior",
    "load_runtime_memory",
    "predict_runtime_outcome",
    "update_runtime_memory",
]
