import tempfile
import unittest
from pathlib import Path

from domains.magnets.runtime_capabilities import build_runtime_capability_engine, load_capability_memory


def make_capability_context(
    *,
    playback_runtime="browser_runtime",
    runtime_profile="browser_balanced",
    startup_confidence="high",
    degradation_risk=20,
    resolution="1080p",
    codec="x264",
    mobile_friendly=True,
):
    return {
        "playback_runtime": playback_runtime,
        "runtime_mode": playback_runtime,
        "approved_runtime": playback_runtime,
        "authority_state": "approved",
        "runtime_profile": runtime_profile,
        "startup_confidence": startup_confidence,
        "selected_source": {
            "title": "Capability Source",
            "quality_label": resolution,
            "resolution": resolution,
            "codec": codec,
            "mobile_friendly": mobile_friendly,
            "high_bandwidth_required": resolution == "2160p",
        },
        "capability_snapshot": {
            "startup_viability": "viable",
            "browser_friendly": codec == "x264",
            "mobile_friendly": mobile_friendly,
            "bandwidth_class": "high" if resolution == "2160p" else "low",
        },
        "execution_metrics": {
            "degradation_risk": degradation_risk,
        },
        "execution_timeline": {
            "estimated_startup_ms": 6200,
            "fallback_probability": 0.18 if codec == "x264" else 0.66,
        },
        "runtime_predictions": {
            "predicted_outcome": "likely_stable_browser_runtime" if codec == "x264" else "likely_external_fallback",
        },
    }


class RuntimeCapabilitiesTests(unittest.TestCase):
    def test_capability_engine_returns_deterministic_payload(self):
        result = build_runtime_capability_engine(make_capability_context(), persist_memory=False)

        self.assertIn("capability_state", result)
        self.assertIn("runtime_feasibility", result)
        self.assertIn("capability_confidence", result)
        self.assertIsInstance(result["capability_warnings"], list)

    def test_capability_memory_persists(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            memory_path = Path(temp_dir) / "runtime_capability_memory.json"
            build_runtime_capability_engine(make_capability_context(), memory_path=memory_path)
            result = build_runtime_capability_engine(
                make_capability_context(codec="hevc", resolution="2160p", degradation_risk=72, mobile_friendly=False),
                memory_path=memory_path,
            )
            stored = load_capability_memory(path=memory_path)

            self.assertEqual(result["capability_memory_summary"]["total_observations"], 2)
            self.assertEqual(stored["aggregates"]["total_runs"], 2)


if __name__ == "__main__":
    unittest.main()
