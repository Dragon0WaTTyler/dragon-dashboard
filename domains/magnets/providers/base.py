from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Mapping


class MagnetProvider(ABC):
    source = "unknown"

    @abstractmethod
    def search_movie_magnets(self, movie: Mapping[str, Any]) -> list[dict[str, Any]]:
        """Return normalized magnet candidates for a movie."""
