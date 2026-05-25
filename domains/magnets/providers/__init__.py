"""Providers for movie magnet search."""

from .base import MagnetProvider
from .torrentio import TorrentioProvider
from .yts import YtsProvider

__all__ = [
    "MagnetProvider",
    "TorrentioProvider",
    "YtsProvider",
]
