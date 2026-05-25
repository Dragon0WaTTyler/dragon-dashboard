from datetime import datetime


class LichessProgressService:
    def __init__(self, *, current_timestamp, default_chess_data):
        self.current_timestamp = current_timestamp
        self.default_chess_data = default_chess_data

    def normalize_lichess_progress_status(self, value):
        normalized = str(value or "").strip().lower()
        if normalized in {"solved", "missed", "skipped", "started"}:
            return normalized
        return "started"

    def _empty_lichess_progress_entry(self, puzzle_id=""):
        return {
            "puzzle_id": str(puzzle_id or "").strip(),
            "status": "started",
            "times_seen": 0,
            "times_solved": 0,
            "times_missed": 0,
            "last_seen_at": "",
            "solved_at": "",
            "last_wrong_move": "",
            "reveal_used": False,
            "completed_clean": False,
            "created_at": "",
            "updated_at": "",
        }

    def _coerce_lichess_progress_entry(self, puzzle_id, raw_entry):
        puzzle_id_value = str(puzzle_id or "").strip()
        payload = self._empty_lichess_progress_entry(puzzle_id_value)
        source = dict(raw_entry or {}) if isinstance(raw_entry, dict) else {}
        payload["status"] = self.normalize_lichess_progress_status(source.get("status"))
        for field_name in ("last_seen_at", "solved_at", "last_wrong_move", "created_at", "updated_at"):
            payload[field_name] = str(source.get(field_name, "") or "").strip()
        payload["times_seen"] = max(0, int(source.get("times_seen", 0) or 0))
        payload["times_solved"] = max(0, int(source.get("times_solved", 0) or 0))
        payload["times_missed"] = max(0, int(source.get("times_missed", 0) or 0))
        payload["reveal_used"] = bool(source.get("reveal_used", False))
        payload["completed_clean"] = bool(source.get("completed_clean", False))
        return payload

    def coerce_lichess_progress_map(self, progress_map):
        if not isinstance(progress_map, dict):
            return {}
        normalized_map = {}
        for puzzle_id, raw_entry in progress_map.items():
            puzzle_key = str(puzzle_id or "").strip()
            if not puzzle_key or not isinstance(raw_entry, dict):
                continue
            normalized_map[puzzle_key] = self._coerce_lichess_progress_entry(puzzle_key, raw_entry)
        return normalized_map

    def get_lichess_puzzle_progress_map(self, data):
        payload = data if isinstance(data, dict) else self.default_chess_data()
        progress_map = self.coerce_lichess_progress_map(payload.get("lichess_puzzle_progress", {}))
        payload["lichess_puzzle_progress"] = progress_map
        return progress_map

    def get_lichess_puzzle_progress_entry(self, data, puzzle_id="", create=False):
        puzzle_key = str(puzzle_id or "").strip()
        if not puzzle_key:
            return None
        progress_map = self.get_lichess_puzzle_progress_map(data)
        entry = progress_map.get(puzzle_key)
        if entry is None and create:
            now = self.current_timestamp()
            entry = self._empty_lichess_progress_entry(puzzle_key)
            entry["created_at"] = now
            entry["updated_at"] = now
            progress_map[puzzle_key] = entry
        return entry

    def _mark_lichess_progress_missed(self, entry):
        if not isinstance(entry, dict):
            return
        current_status = self.normalize_lichess_progress_status(entry.get("status"))
        if current_status not in {"missed", "skipped"}:
            entry["times_missed"] = max(0, int(entry.get("times_missed", 0) or 0)) + 1
        entry["status"] = "missed"
        entry["completed_clean"] = False

    def record_lichess_puzzle_started(self, data, puzzle_id=""):
        entry = self.get_lichess_puzzle_progress_entry(data, puzzle_id, create=True)
        if not isinstance(entry, dict):
            return {"ok": False, "error": "Lichess puzzle unavailable.", "progress": None}
        now = self.current_timestamp()
        increment_seen = True
        last_seen_at = str(entry.get("last_seen_at", "") or "").strip()
        if last_seen_at:
            try:
                last_seen_dt = datetime.fromisoformat(last_seen_at)
                now_dt = datetime.fromisoformat(now)
                increment_seen = (now_dt - last_seen_dt).total_seconds() > 600
            except Exception:
                increment_seen = True
        if increment_seen:
            entry["times_seen"] = max(0, int(entry.get("times_seen", 0) or 0)) + 1
        entry["last_seen_at"] = now
        if self.normalize_lichess_progress_status(entry.get("status")) != "solved":
            entry["status"] = "started"
        if not str(entry.get("created_at", "") or "").strip():
            entry["created_at"] = now
        entry["updated_at"] = now
        return {"ok": True, "error": "", "progress": dict(entry)}

    def record_lichess_puzzle_wrong_move(self, data, puzzle_id="", attempted_move=""):
        entry = self.get_lichess_puzzle_progress_entry(data, puzzle_id, create=True)
        if not isinstance(entry, dict):
            return {"ok": False, "error": "Lichess puzzle unavailable.", "progress": None}
        self._mark_lichess_progress_missed(entry)
        entry["last_wrong_move"] = str(attempted_move or "").strip()
        entry["updated_at"] = self.current_timestamp()
        return {"ok": True, "error": "", "progress": dict(entry)}

    def record_lichess_puzzle_reveal(self, data, puzzle_id=""):
        entry = self.get_lichess_puzzle_progress_entry(data, puzzle_id, create=True)
        if not isinstance(entry, dict):
            return {"ok": False, "error": "Lichess puzzle unavailable.", "progress": None}
        entry["reveal_used"] = True
        self._mark_lichess_progress_missed(entry)
        entry["updated_at"] = self.current_timestamp()
        return {"ok": True, "error": "", "progress": dict(entry)}

    def record_lichess_puzzle_skipped(self, data, puzzle_id=""):
        entry = self.get_lichess_puzzle_progress_entry(data, puzzle_id, create=True)
        if not isinstance(entry, dict):
            return {"ok": False, "error": "Lichess puzzle unavailable.", "progress": None}
        current_status = self.normalize_lichess_progress_status(entry.get("status"))
        if current_status not in {"skipped", "missed"}:
            entry["times_missed"] = max(0, int(entry.get("times_missed", 0) or 0)) + 1
        entry["status"] = "skipped"
        entry["completed_clean"] = False
        entry["updated_at"] = self.current_timestamp()
        return {"ok": True, "error": "", "progress": dict(entry)}

    def record_lichess_puzzle_complete(self, data, puzzle_id="", completed_clean=True):
        entry = self.get_lichess_puzzle_progress_entry(data, puzzle_id, create=True)
        if not isinstance(entry, dict):
            return {"ok": False, "error": "Lichess puzzle unavailable.", "progress": None}
        now = self.current_timestamp()
        previous_status = self.normalize_lichess_progress_status(entry.get("status"))
        clean_value = bool(completed_clean) and previous_status not in {"missed", "skipped"} and not bool(entry.get("reveal_used", False))
        entry["status"] = "solved"
        entry["times_solved"] = max(0, int(entry.get("times_solved", 0) or 0)) + 1
        entry["solved_at"] = now
        entry["completed_clean"] = clean_value
        if not clean_value:
            entry["times_missed"] = max(0, int(entry.get("times_missed", 0) or 0)) + (0 if previous_status in {"missed", "skipped"} else 1)
        entry["updated_at"] = now
        return {"ok": True, "error": "", "progress": dict(entry)}

    def classify_lichess_progress_bucket(self, progress_entry):
        if not isinstance(progress_entry, dict):
            return 0
        status_value = self.normalize_lichess_progress_status(progress_entry.get("status"))
        if status_value == "solved" and bool(progress_entry.get("completed_clean", False)):
            return 2
        return 1

    def build_lichess_progress_snapshot(self, data, valid_items=None):
        progress_map = self.get_lichess_puzzle_progress_map(data)
        valid_puzzle_ids = [
            str((item.get("row", {}) or {}).get("puzzle_id", "") or "").strip()
            for item in (valid_items or [])
            if isinstance(item, dict)
        ]
        valid_puzzle_ids = [item for item in valid_puzzle_ids if item]
        relevant_entries = [progress_map.get(puzzle_id) for puzzle_id in valid_puzzle_ids if progress_map.get(puzzle_id)]
        solved_clean_count = sum(1 for entry in relevant_entries if self.classify_lichess_progress_bucket(entry) == 2)
        solved_count = sum(
            1
            for entry in relevant_entries
            if self.normalize_lichess_progress_status((entry or {}).get("status")) == "solved"
        )
        review_needed_count = sum(1 for entry in relevant_entries if self.classify_lichess_progress_bucket(entry) == 1)
        total_count = len(valid_puzzle_ids)
        remaining_count = max(0, total_count - solved_clean_count)
        all_clean_solved = bool(total_count) and solved_clean_count >= total_count
        return {
            "total_count": total_count,
            "solved_count": solved_count,
            "solved_clean_count": solved_clean_count,
            "remaining_count": remaining_count,
            "review_needed_count": review_needed_count,
            "all_clean_solved": all_clean_solved,
        }
