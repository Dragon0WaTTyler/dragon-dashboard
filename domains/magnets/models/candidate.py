from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(slots=True)
class MagnetCandidate:
    source: str
    title: str
    magnet: str
    size_gb: float
    resolution: str
    codec: str
    seeders: int
    language: str
    imdb_id: str

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["size_gb"] = round(float(payload.get("size_gb", 0.0) or 0.0), 3)
        payload["seeders"] = int(payload.get("seeders", 0) or 0)
        return payload
