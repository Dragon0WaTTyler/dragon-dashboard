"""Runtime helpers for the magnets domain."""

from .compatibility import (
    browser_codec_friendly,
    external_player_codec_friendly,
    is_high_bandwidth_profile,
    mobile_codec_friendly,
)
from .config import DEFAULT_HEADERS, DEFAULT_TRACKERS
from .http import build_session, safe_json_get
from .identifiers import source_fingerprint
from .intelligence import build_release_pattern, counter_to_ranked_list, failure_ratio_items, top_ratio_items
from .magnet import is_valid_magnet_uri, parse_magnet_uri
from .observability import emit_event
from .playback_policy import build_compatibility_snapshot, evaluate_playback_admission
from .session_runtime import RUNTIME_INTENTS, normalize_runtime_intent, resolve_runtime_intent

__all__ = [
    "DEFAULT_HEADERS",
    "DEFAULT_TRACKERS",
    "RUNTIME_INTENTS",
    "browser_codec_friendly",
    "build_session",
    "build_release_pattern",
    "build_compatibility_snapshot",
    "counter_to_ranked_list",
    "emit_event",
    "evaluate_playback_admission",
    "external_player_codec_friendly",
    "failure_ratio_items",
    "is_high_bandwidth_profile",
    "is_valid_magnet_uri",
    "mobile_codec_friendly",
    "normalize_runtime_intent",
    "parse_magnet_uri",
    "resolve_runtime_intent",
    "safe_json_get",
    "source_fingerprint",
    "top_ratio_items",
]
