"""Movie magnet search domain for Dragon."""

from .models import MagnetCandidate
from .playback import get_runtime_profiles_catalog, prepare_playback_runtime, select_playback_candidates
from .runtime_consciousness import build_runtime_consciousness
from .runtime_resonance import build_runtime_resonance
from .runtime_symbiosis import build_runtime_symbiosis
from .runtime_temporal import build_runtime_temporal
from .services import (
    ExperimentalRuntimeService,
    ExperimentalSessionStore,
    MagnetSearchService,
    MovieSourcesService,
    SessionAnalyticsService,
    SourceActionService,
    SourceHandoffService,
    StreamSessionService,
    StreamSessionStore,
)

_default_service = MagnetSearchService()


def search_movie_magnets(movie, *, force_refresh: bool = False):
    """Search normalized movie magnet candidates across configured providers."""
    return _default_service.search_movie_magnets(movie, force_refresh=force_refresh)


__all__ = [
    "ExperimentalRuntimeService",
    "ExperimentalSessionStore",
    "MagnetCandidate",
    "MagnetSearchService",
    "MovieSourcesService",
    "get_runtime_profiles_catalog",
    "prepare_playback_runtime",
    "select_playback_candidates",
    "SessionAnalyticsService",
    "SourceActionService",
    "SourceHandoffService",
    "build_runtime_consciousness",
    "build_runtime_resonance",
    "build_runtime_symbiosis",
    "build_runtime_temporal",
    "StreamSessionService",
    "StreamSessionStore",
    "search_movie_magnets",
]
