"""Chess data runtime ownership."""

import threading


def default_chess_data():
    return {
        "profiles": [],
        "imports": [],
        "games": [],
        "openings": [],
        "review_queue": [],
        "puzzle_seeds": [],
        "auto_puzzle_candidates": [],
        "puzzle_attempts": [],
        "lichess_puzzle_progress": {},
        "settings": {
            "active_profile_id": None,
        },
        "updated_at": "",
    }


CHESS_DATA_LOCK = threading.RLock()


class ChessStorageRuntime:
    def __init__(
        self,
        *,
        chess_data_path,
        load_json_file,
        save_json_file,
        current_timestamp,
        coerce_lichess_progress_map,
    ):
        self.chess_data_path = chess_data_path
        self.load_json_file = load_json_file
        self.save_json_file = save_json_file
        self.current_timestamp = current_timestamp
        self.coerce_lichess_progress_map = coerce_lichess_progress_map

    def load_chess_data(self):
        with CHESS_DATA_LOCK:
            raw = self.load_json_file(self.chess_data_path, default_chess_data())
            if not isinstance(raw, dict):
                raw = {}
            data = default_chess_data()
            for key in ("profiles", "imports", "games", "openings", "review_queue", "puzzle_seeds", "auto_puzzle_candidates", "puzzle_attempts"):
                value = raw.get(key, [])
                data[key] = [dict(item) for item in value if isinstance(item, dict)] if isinstance(value, list) else []
            data["lichess_puzzle_progress"] = self.coerce_lichess_progress_map(
                raw.get("lichess_puzzle_progress", {})
            )
            settings = raw.get("settings", {})
            if not isinstance(settings, dict):
                settings = {}
            active_profile_id = str(settings.get("active_profile_id", "") or "").strip() or None
            data["settings"]["active_profile_id"] = active_profile_id
            data["updated_at"] = str(raw.get("updated_at", "") or "").strip()
            if self.chess_data_path.exists() and data != raw:
                self.save_json_file(self.chess_data_path, data)
            return data

    def save_chess_data(self, data):
        with CHESS_DATA_LOCK:
            payload = default_chess_data()
            incoming = data if isinstance(data, dict) else {}
            for key in ("profiles", "imports", "games", "openings", "review_queue", "puzzle_seeds", "auto_puzzle_candidates", "puzzle_attempts"):
                value = incoming.get(key, [])
                payload[key] = [dict(item) for item in value if isinstance(item, dict)] if isinstance(value, list) else []
            payload["lichess_puzzle_progress"] = self.coerce_lichess_progress_map(
                incoming.get("lichess_puzzle_progress", {})
            )
            settings = incoming.get("settings", {})
            if not isinstance(settings, dict):
                settings = {}
            payload["settings"]["active_profile_id"] = str(settings.get("active_profile_id", "") or "").strip() or None
            payload["updated_at"] = self.current_timestamp()
            self.save_json_file(self.chess_data_path, payload)
            return payload


_CHESS_STORAGE = None


def configure_chess_storage(**kwargs):
    global _CHESS_STORAGE
    _CHESS_STORAGE = ChessStorageRuntime(**kwargs)
    return _CHESS_STORAGE


def get_chess_storage():
    return _CHESS_STORAGE


def load_chess_data():
    storage = get_chess_storage()
    if storage is None:
        raise RuntimeError("Chess storage is not configured.")
    return storage.load_chess_data()


def save_chess_data(data):
    storage = get_chess_storage()
    if storage is None:
        raise RuntimeError("Chess storage is not configured.")
    return storage.save_chess_data(data)
