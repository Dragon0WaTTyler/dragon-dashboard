"""Movie magnet search domain for Dragon."""

from .models import MagnetCandidate
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
    "SessionAnalyticsService",
    "SourceActionService",
    "SourceHandoffService",
    "StreamSessionService",
    "StreamSessionStore",
    "search_movie_magnets",
]
