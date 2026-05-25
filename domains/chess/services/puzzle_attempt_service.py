import hashlib
from datetime import datetime, timedelta, timezone


class PuzzleAttemptService:
    def __init__(
        self,
        *,
        current_timestamp,
        parse_timestamp,
        default_chess_data,
        safe_non_negative_int,
        candidate_progress_updater=None,
        normalize_candidate_status=None,
        format_move_label=None,
    ):
        self.current_timestamp = current_timestamp
        self.parse_timestamp = parse_timestamp
        self.default_chess_data = default_chess_data
        self.safe_non_negative_int = safe_non_negative_int
        self.candidate_progress_updater = candidate_progress_updater
        self.normalize_candidate_status = normalize_candidate_status
        self.format_move_label = format_move_label

    def set_candidate_progress_updater(self, candidate_progress_updater):
        self.candidate_progress_updater = candidate_progress_updater

    def set_candidate_status_normalizer(self, normalize_candidate_status):
        self.normalize_candidate_status = normalize_candidate_status

    def set_move_label_formatter(self, format_move_label):
        self.format_move_label = format_move_label

    def normalize_puzzle_attempt_status(self, value):
        normalized = str(value or "").strip().lower()
        if normalized in {"started", "completed", "skipped", "archived"}:
            return normalized
        return "started"

    def normalize_puzzle_review_state(self, value):
        normalized = str(value or "").strip().lower()
        if normalized in {"new", "repeat_due", "scheduled", "review_due", "mastered"}:
            return normalized
        return "new"

    def safe_puzzle_attempt_int(self, value, default=0):
        try:
            return int(str(value or "").strip() or default)
        except Exception:
            return int(default)

    def _puzzle_attempt_sort_value(self, attempt):
        if not isinstance(attempt, dict):
            return datetime.min
        return self.parse_timestamp(attempt.get("updated_at")) or self.parse_timestamp(attempt.get("created_at")) or datetime.min

    def _safe_attempt_timestamp_value(self, value):
        return self.parse_timestamp(value) or None

    def build_puzzle_attempt_id(self, candidate_id, started_at=""):
        candidate_value = str(candidate_id or "").strip()
        timestamp_value = str(started_at or self.current_timestamp()).strip() or self.current_timestamp()
        digest = hashlib.sha1(f"{candidate_value}|{timestamp_value}".encode("utf-8")).hexdigest()[:14]
        return f"attempt:{candidate_value}:{digest}"

    def _copy_puzzle_review_fields(self, attempt):
        payload = dict(attempt or {}) if isinstance(attempt, dict) else {}
        return {
            "review_state": self.normalize_puzzle_review_state(payload.get("review_state")),
            "last_attempt_at": str(payload.get("last_attempt_at", "") or "").strip(),
            "next_due_at": str(payload.get("next_due_at", "") or "").strip(),
            "clean_streak": max(0, int(payload.get("clean_streak", 0) or 0)),
            "miss_streak": max(0, int(payload.get("miss_streak", 0) or 0)),
            "times_seen": max(0, int(payload.get("times_seen", 0) or 0)),
            "times_clean": max(0, int(payload.get("times_clean", 0) or 0)),
            "times_missed": max(0, int(payload.get("times_missed", 0) or 0)),
            "mastered": bool(payload.get("mastered", False)),
        }

    def build_puzzle_review_schedule(self, clean_streak=0, needs_repeat=False, skipped=False):
        streak_value = max(0, int(clean_streak or 0))
        now = datetime.now(timezone.utc)
        if needs_repeat or skipped:
            next_due = now
            review_state = "repeat_due"
            mastered = False
        elif streak_value >= 3:
            next_due = now + timedelta(days=14)
            review_state = "mastered"
            mastered = True
        elif streak_value >= 2:
            next_due = now + timedelta(days=3)
            review_state = "scheduled"
            mastered = False
        else:
            next_due = now + timedelta(days=1)
            review_state = "scheduled"
            mastered = False
        return {
            "review_state": review_state,
            "next_due_at": next_due.isoformat(),
            "mastered": mastered,
        }

    def find_previous_resolved_puzzle_attempt(self, data, candidate_id="", exclude_attempt_id=""):
        payload = data if isinstance(data, dict) else self.default_chess_data()
        candidate_value = str(candidate_id or "").strip()
        exclude_value = str(exclude_attempt_id or "").strip()
        matches = []
        for item in payload.get("puzzle_attempts", []) or []:
            if not isinstance(item, dict):
                continue
            if candidate_value and str(item.get("candidate_id", "") or "").strip() != candidate_value:
                continue
            if exclude_value and str(item.get("id", "") or "").strip() == exclude_value:
                continue
            if self.normalize_puzzle_attempt_status(item.get("status")) not in {"completed", "skipped"}:
                continue
            matches.append(item)
        if not matches:
            return None
        matches.sort(key=self._puzzle_attempt_sort_value, reverse=True)
        return matches[0]

    def latest_puzzle_attempt_needs_repeat(self, attempt):
        payload = dict(attempt or {}) if isinstance(attempt, dict) else {}
        if not payload:
            return False
        status_value = self.normalize_puzzle_attempt_status(payload.get("status"))
        if status_value == "skipped":
            return True
        if bool(payload.get("needs_repeat", False)):
            return True
        if max(0, int(payload.get("wrong_count", 0) or 0)) > 0:
            return True
        if bool(payload.get("reveal_used", False)):
            return True
        return False

    def puzzle_attempt_due_now(self, attempt, now=None):
        payload = dict(attempt or {}) if isinstance(attempt, dict) else {}
        if not payload:
            return False
        status_value = self.normalize_puzzle_attempt_status(payload.get("status"))
        if status_value in {"started", "archived"}:
            return False
        if self.latest_puzzle_attempt_needs_repeat(payload):
            return True
        mastered = bool(payload.get("mastered", False))
        due_value = self._safe_attempt_timestamp_value(payload.get("next_due_at"))
        if due_value is None:
            return not mastered
        compare_now = now if isinstance(now, datetime) else datetime.now(timezone.utc)
        return due_value <= compare_now

    def find_puzzle_attempt(self, data, attempt_id="", candidate_id="", status=None):
        payload = data if isinstance(data, dict) else self.default_chess_data()
        attempt_value = str(attempt_id or "").strip()
        candidate_value = str(candidate_id or "").strip()
        status_value = self.normalize_puzzle_attempt_status(status) if status else ""
        matches = []
        for item in payload.get("puzzle_attempts", []) or []:
            if not isinstance(item, dict):
                continue
            if attempt_value and str(item.get("id", "") or "").strip() != attempt_value:
                continue
            if candidate_value and str(item.get("candidate_id", "") or "").strip() != candidate_value:
                continue
            if status_value and self.normalize_puzzle_attempt_status(item.get("status")) != status_value:
                continue
            matches.append(item)
        if not matches:
            return None
        matches.sort(key=self._puzzle_attempt_sort_value, reverse=True)
        return matches[0]

    def build_puzzle_attempt_summary(self, attempt):
        payload = dict(attempt or {}) if isinstance(attempt, dict) else {}
        wrong_count = max(0, int(payload.get("wrong_count", 0) or 0))
        reveal_used = bool(payload.get("reveal_used", False))
        status_value = self.normalize_puzzle_attempt_status(payload.get("status"))
        completed_clean = bool(payload.get("completed_clean", False))
        needs_repeat = bool(payload.get("needs_repeat", False))
        due_now = self.puzzle_attempt_due_now(payload)
        mastered = bool(payload.get("mastered", False))
        parts = []
        if status_value == "completed":
            if completed_clean:
                parts.append("Clean solve")
            elif needs_repeat:
                parts.append("Needs another try")
        elif status_value == "skipped":
            parts.append("Skipped")
        if due_now and status_value in {"completed", "skipped"} and not needs_repeat and not mastered:
            parts.append("Due for review")
        if mastered:
            parts.append("Mastered")
        if wrong_count > 0:
            parts.append(f"{wrong_count} wrong move{'s' if wrong_count != 1 else ''}")
        if reveal_used:
            parts.append("Reveal used")
        return {
            "text": " · ".join(parts),
            "wrong_count": wrong_count,
            "reveal_used": reveal_used,
            "completed_clean": completed_clean,
            "needs_repeat": needs_repeat,
            "due_now": due_now,
            "mastered": mastered,
            "status": status_value,
        }

    def build_puzzle_repeat_note(self, candidate, latest_attempt):
        candidate_payload = dict(candidate or {}) if isinstance(candidate, dict) else {}
        attempt_payload = dict(latest_attempt or {}) if isinstance(latest_attempt, dict) else {}
        summary = self.build_puzzle_attempt_summary(attempt_payload)
        if not self.latest_puzzle_attempt_needs_repeat(attempt_payload):
            return {
                "repeat_needed": False,
                "note": "",
                "title": "",
                "reason": "",
                "last_wrong_move": "",
                "expected_move": "",
                "comparison": "",
                "highlight_move": "",
                "summary": summary,
            }

        status_value = self.normalize_puzzle_attempt_status(attempt_payload.get("status"))
        wrong_count = max(0, int(attempt_payload.get("wrong_count", 0) or 0))
        reveal_used = bool(attempt_payload.get("reveal_used", False))
        wrong_moves = attempt_payload.get("wrong_moves", [])
        if not isinstance(wrong_moves, list):
            wrong_moves = []
        last_wrong = dict(wrong_moves[-1] or {}) if wrong_moves and isinstance(wrong_moves[-1], dict) else {}
        format_move_label = self.format_move_label if callable(self.format_move_label) else None
        if format_move_label:
            last_wrong_move = format_move_label(
                last_wrong.get("fen", ""),
                last_wrong.get("attempted_move_uci", ""),
                last_wrong.get("attempted_move_san", ""),
            )
            expected_move = format_move_label(
                last_wrong.get("fen", ""),
                last_wrong.get("expected_move_uci", ""),
                candidate_payload.get("next_move_label", ""),
            )
        else:
            last_wrong_move = str(last_wrong.get("attempted_move_san", "") or last_wrong.get("attempted_move_uci", "") or "").strip()
            expected_move = str(last_wrong.get("expected_move_uci", "") or candidate_payload.get("next_move_label", "") or "").strip()
        reason = ""
        if status_value == "skipped":
            reason = "Skipped before."
        elif reveal_used and wrong_count > 0:
            reason = "Reveal was used before."
        elif reveal_used:
            reason = "Reveal was used before."
        elif wrong_count > 0:
            reason = "Wrong move before."
        else:
            reason = "Needs another try."
        note_bits = ["This puzzle came back because you missed it before."]
        if reason:
            note_bits.append(reason)
        if last_wrong_move:
            note_bits.append(f"Last time you tried {last_wrong_move}.")
        if expected_move:
            note_bits.append(f"This time, try to find {expected_move}.")
        else:
            note_bits.append("This time, try to find the line move.")
        note_bits.append("Try to solve it cleanly this time.")
        return {
            "repeat_needed": True,
            "note": " ".join(note_bits),
            "title": "Try again",
            "reason": reason,
            "last_wrong_move": last_wrong_move,
            "expected_move": expected_move,
            "comparison": f"Last time: {last_wrong_move}." if last_wrong_move else "",
            "highlight_move": last_wrong.get("attempted_move_uci", "") or "",
            "summary": summary,
        }

    def get_latest_puzzle_attempt_map(self, data):
        payload = data if isinstance(data, dict) else self.default_chess_data()
        latest_map = {}
        for raw_attempt in payload.get("puzzle_attempts", []) or []:
            if not isinstance(raw_attempt, dict):
                continue
            candidate_id = str(raw_attempt.get("candidate_id", "") or "").strip()
            if not candidate_id:
                continue
            existing = latest_map.get(candidate_id)
            if existing is None or self._puzzle_attempt_sort_value(raw_attempt) >= self._puzzle_attempt_sort_value(existing):
                latest_map[candidate_id] = raw_attempt
        return latest_map

    def build_puzzle_attempt_history_map(self, data):
        payload = data if isinstance(data, dict) else self.default_chess_data()
        history_map = {}
        for raw_attempt in payload.get("puzzle_attempts", []) or []:
            if not isinstance(raw_attempt, dict):
                continue
            candidate_id = str(raw_attempt.get("candidate_id", "") or "").strip()
            if not candidate_id:
                continue
            entry = history_map.setdefault(candidate_id, {
                "training_key": candidate_id,
                "attempt_count": 0,
                "completed_count": 0,
                "clean_completed_count": 0,
                "skipped_count": 0,
                "difficult_attempt_count": 0,
                "last_attempt_at": "",
                "last_completed_at": "",
                "last_result": "",
                "last_wrong_count": 0,
                "last_reveal_used": False,
            })
            entry["attempt_count"] += 1
            status_value = self.normalize_puzzle_attempt_status(raw_attempt.get("status"))
            completed_at = str(raw_attempt.get("completed_at", "") or "").strip()
            updated_at = str(raw_attempt.get("updated_at", "") or "").strip()
            created_at = str(raw_attempt.get("created_at", "") or "").strip()
            last_attempt_at = completed_at or updated_at or created_at
            previous_attempt_at = self._safe_attempt_timestamp_value(entry.get("last_attempt_at"))
            current_attempt_at = self._safe_attempt_timestamp_value(last_attempt_at)
            if current_attempt_at and (previous_attempt_at is None or current_attempt_at >= previous_attempt_at):
                entry["last_attempt_at"] = last_attempt_at
                entry["last_result"] = status_value
                entry["last_wrong_count"] = self.safe_non_negative_int(raw_attempt.get("wrong_count", 0), 0)
                entry["last_reveal_used"] = bool(raw_attempt.get("reveal_used", False))
            if status_value == "completed":
                entry["completed_count"] += 1
                if bool(raw_attempt.get("completed_clean", False)):
                    entry["clean_completed_count"] += 1
            elif status_value == "skipped":
                entry["skipped_count"] += 1
            if status_value in {"completed", "skipped"} and self.latest_puzzle_attempt_needs_repeat(raw_attempt):
                entry["difficult_attempt_count"] += 1
            if status_value == "completed" and completed_at:
                previous_completed_at = self._safe_attempt_timestamp_value(entry.get("last_completed_at"))
                current_completed_at = self._safe_attempt_timestamp_value(completed_at)
                if current_completed_at and (previous_completed_at is None or current_completed_at >= previous_completed_at):
                    entry["last_completed_at"] = completed_at
        return history_map

    def get_candidate_repeat_state(self, candidate, latest_attempt):
        candidate_payload = dict(candidate or {}) if isinstance(candidate, dict) else {}
        attempt_payload = dict(latest_attempt or {}) if isinstance(latest_attempt, dict) else {}
        normalize_candidate_status = self.normalize_candidate_status if callable(self.normalize_candidate_status) else None
        base_status = normalize_candidate_status(candidate_payload.get("status", "candidate")) if normalize_candidate_status else str(candidate_payload.get("status", "candidate") or "").strip().lower()
        if base_status == "archived":
            repeat_note = self.build_puzzle_repeat_note(candidate_payload, attempt_payload)
            return {
                "repeat_needed": False,
                "effective_status": "archived",
                "latest_attempt": attempt_payload,
                "latest_attempt_status": self.normalize_puzzle_attempt_status(attempt_payload.get("status")) if attempt_payload else "",
                "summary": self.build_puzzle_attempt_summary(attempt_payload),
                "reason": "",
                "coach_note": "",
                "repeat_note": repeat_note,
            }

        latest_status = self.normalize_puzzle_attempt_status(attempt_payload.get("status")) if attempt_payload else ""
        repeat_needed = self.latest_puzzle_attempt_needs_repeat(attempt_payload)
        due_now = self.puzzle_attempt_due_now(attempt_payload)
        mastered = bool(attempt_payload.get("mastered", False))
        review_state = self.normalize_puzzle_review_state(attempt_payload.get("review_state"))
        candidate_status_timestamp = (
            self.parse_timestamp(candidate_payload.get("updated_at", ""))
            or self.parse_timestamp(candidate_payload.get("created_at", ""))
            or datetime.min
        )
        attempt_timestamp = self._puzzle_attempt_sort_value(attempt_payload) if attempt_payload else datetime.min
        effective_status = base_status
        repeat_note = self.build_puzzle_repeat_note(candidate_payload, attempt_payload)
        reason = str(repeat_note.get("reason", "") or "").strip()
        coach_note = str(repeat_note.get("note", "") or "").strip()
        if repeat_needed:
            if not coach_note:
                coach_note = "This puzzle came back because you missed it before. Try to solve it cleanly this time."
        elif latest_status == "completed" and due_now and not mastered:
            coach_note = "This one is back for review. See if you can solve it cleanly again."
            reason = "Due for review"

        if latest_status == "completed":
            if base_status in {"queued", "training"} and candidate_status_timestamp > attempt_timestamp:
                effective_status = base_status
            else:
                effective_status = "candidate" if (repeat_needed or (due_now and not mastered)) else "done"
        elif latest_status == "skipped":
            if base_status in {"queued", "training"} and candidate_status_timestamp > attempt_timestamp:
                effective_status = base_status
            else:
                effective_status = "candidate"
        elif latest_status == "started" and base_status == "candidate":
            effective_status = "training"

        return {
            "repeat_needed": repeat_needed,
            "due_now": due_now,
            "review_state": review_state,
            "mastered": mastered,
            "effective_status": effective_status,
            "latest_attempt": attempt_payload,
            "latest_attempt_status": latest_status,
            "summary": self.build_puzzle_attempt_summary(attempt_payload),
            "reason": reason,
            "coach_note": coach_note,
            "repeat_note": repeat_note,
        }

    def get_or_create_active_puzzle_attempt(self, data, candidate, total_steps=0):
        payload = data if isinstance(data, dict) else self.default_chess_data()
        candidate_payload = dict(candidate or {}) if isinstance(candidate, dict) else {}
        candidate_id = str(candidate_payload.get("id", "") or "").strip()
        if not candidate_id:
            return {"attempt": None, "changed": False, "created": False}
        attempts = [
            item for item in (payload.get("puzzle_attempts", []) or [])
            if isinstance(item, dict)
            and str(item.get("candidate_id", "") or "").strip() == candidate_id
            and self.normalize_puzzle_attempt_status(item.get("status")) == "started"
        ]
        attempts.sort(key=self._puzzle_attempt_sort_value, reverse=True)
        changed = False
        active_attempt = None
        now = self.current_timestamp()
        if attempts:
            active_attempt = attempts[0]
            for extra_attempt in attempts[1:]:
                if self.normalize_puzzle_attempt_status(extra_attempt.get("status")) != "archived":
                    extra_attempt["status"] = "archived"
                    extra_attempt["updated_at"] = now
                    changed = True
            total_steps_value = max(0, int(total_steps or 0))
            if total_steps_value and int(active_attempt.get("total_steps", 0) or 0) != total_steps_value:
                active_attempt["total_steps"] = total_steps_value
                changed = True
            for field_name, field_value in (
                ("game_id", str(candidate_payload.get("game_id", "") or "").strip()),
                ("source_type", str(candidate_payload.get("source_type", "") or "").strip()),
            ):
                if field_value and str(active_attempt.get(field_name, "") or "").strip() != field_value:
                    active_attempt[field_name] = field_value
                    changed = True
            if changed:
                active_attempt["updated_at"] = now
            return {"attempt": dict(active_attempt), "changed": changed, "created": False}

        created_at = now
        total_steps_value = max(0, int(total_steps or 0))
        previous_attempt = self.find_previous_resolved_puzzle_attempt(payload, candidate_id=candidate_id)
        previous_review = self._copy_puzzle_review_fields(previous_attempt)
        new_attempt = {
            "id": self.build_puzzle_attempt_id(candidate_id, created_at),
            "candidate_id": candidate_id,
            "game_id": str(candidate_payload.get("game_id", "") or "").strip(),
            "source_type": str(candidate_payload.get("source_type", "") or "").strip(),
            "started_at": created_at,
            "completed_at": "",
            "status": "started",
            "wrong_count": 0,
            "correct_count": 0,
            "reveal_used": False,
            "hint_used": False,
            "engine_check_used": False,
            "critical_moment_check_used": False,
            "completed_clean": False,
            "needs_repeat": False,
            "final_step": 0,
            "total_steps": total_steps_value,
            "wrong_moves": [],
            "review_state": previous_review.get("review_state", "new") if previous_attempt else "new",
            "last_attempt_at": str(previous_review.get("last_attempt_at", previous_attempt.get("completed_at", "") if isinstance(previous_attempt, dict) else "") or "").strip(),
            "next_due_at": str(previous_review.get("next_due_at", "") or "").strip(),
            "clean_streak": max(0, int(previous_review.get("clean_streak", 0) or 0)),
            "miss_streak": max(0, int(previous_review.get("miss_streak", 0) or 0)),
            "times_seen": max(0, int(previous_review.get("times_seen", 0) or 0)) + 1,
            "times_clean": max(0, int(previous_review.get("times_clean", 0) or 0)),
            "times_missed": max(0, int(previous_review.get("times_missed", 0) or 0)),
            "mastered": bool(previous_review.get("mastered", False)),
            "created_at": created_at,
            "updated_at": created_at,
        }
        payload.setdefault("puzzle_attempts", []).append(new_attempt)
        return {"attempt": dict(new_attempt), "changed": True, "created": True}

    def record_puzzle_wrong_move(self, data, attempt_id, step_index=0, fen="", attempted_move_uci="", attempted_move_san="", expected_move_uci="", engine_move_uci=""):
        attempt = self.find_puzzle_attempt(data, attempt_id=attempt_id)
        if not isinstance(attempt, dict):
            return {"ok": False, "error": "Active puzzle attempt unavailable.", "attempt": None}
        wrong_moves = attempt.get("wrong_moves")
        if not isinstance(wrong_moves, list):
            wrong_moves = []
            attempt["wrong_moves"] = wrong_moves
        wrong_moves.append({
            "step_index": max(0, int(step_index or 0)),
            "fen": str(fen or "").strip(),
            "attempted_move_uci": str(attempted_move_uci or "").strip(),
            "attempted_move_san": str(attempted_move_san or "").strip(),
            "expected_move_uci": str(expected_move_uci or "").strip(),
            "engine_move_uci": str(engine_move_uci or "").strip(),
            "created_at": self.current_timestamp(),
        })
        attempt["wrong_count"] = max(0, int(attempt.get("wrong_count", 0) or 0)) + 1
        attempt["final_step"] = max(max(0, int(attempt.get("final_step", 0) or 0)), max(0, int(step_index or 0)))
        attempt["updated_at"] = self.current_timestamp()
        return {"ok": True, "error": "", "attempt": dict(attempt)}

    def record_puzzle_correct_move(self, data, attempt_id, step_index=0):
        attempt = self.find_puzzle_attempt(data, attempt_id=attempt_id)
        if not isinstance(attempt, dict):
            return {"ok": False, "error": "Active puzzle attempt unavailable.", "attempt": None}
        attempt["correct_count"] = max(0, int(attempt.get("correct_count", 0) or 0)) + 1
        attempt["final_step"] = max(max(0, int(attempt.get("final_step", 0) or 0)), max(0, int(step_index or 0)))
        attempt["updated_at"] = self.current_timestamp()
        return {"ok": True, "error": "", "attempt": dict(attempt)}

    def record_puzzle_reveal(self, data, attempt_id):
        attempt = self.find_puzzle_attempt(data, attempt_id=attempt_id)
        if not isinstance(attempt, dict):
            return {"ok": False, "error": "Active puzzle attempt unavailable.", "attempt": None}
        if not bool(attempt.get("reveal_used", False)):
            attempt["reveal_used"] = True
            attempt["updated_at"] = self.current_timestamp()
        return {"ok": True, "error": "", "attempt": dict(attempt)}

    def record_puzzle_engine_check(self, data, attempt_id):
        attempt = self.find_puzzle_attempt(data, attempt_id=attempt_id)
        if not isinstance(attempt, dict):
            return {"ok": False, "error": "Active puzzle attempt unavailable.", "attempt": None}
        if not bool(attempt.get("engine_check_used", False)):
            attempt["engine_check_used"] = True
            attempt["updated_at"] = self.current_timestamp()
        return {"ok": True, "error": "", "attempt": dict(attempt)}

    def record_puzzle_critical_moment_check(self, data, attempt_id):
        attempt = self.find_puzzle_attempt(data, attempt_id=attempt_id)
        if not isinstance(attempt, dict):
            return {"ok": False, "error": "Active puzzle attempt unavailable.", "attempt": None}
        if not bool(attempt.get("critical_moment_check_used", False)):
            attempt["critical_moment_check_used"] = True
            attempt["updated_at"] = self.current_timestamp()
        return {"ok": True, "error": "", "attempt": dict(attempt)}

    def skip_puzzle_attempt(self, data, attempt_id, final_step=0, total_steps=0):
        attempt = self.find_puzzle_attempt(data, attempt_id=attempt_id)
        if not isinstance(attempt, dict):
            return {"ok": False, "error": "Active puzzle attempt unavailable.", "attempt": None}
        review = self._copy_puzzle_review_fields(attempt)
        schedule = self.build_puzzle_review_schedule(clean_streak=0, needs_repeat=True, skipped=True)
        attempt["status"] = "skipped"
        attempt["completed_at"] = self.current_timestamp()
        attempt["completed_clean"] = False
        attempt["needs_repeat"] = True
        attempt["final_step"] = max(0, int(final_step or 0))
        attempt["total_steps"] = max(max(0, int(attempt.get("total_steps", 0) or 0)), max(0, int(total_steps or 0)))
        attempt["last_attempt_at"] = attempt["completed_at"]
        attempt["clean_streak"] = 0
        attempt["miss_streak"] = max(0, int(review.get("miss_streak", 0) or 0)) + 1
        attempt["times_seen"] = max(1, int(review.get("times_seen", 0) or 0))
        attempt["times_clean"] = max(0, int(review.get("times_clean", 0) or 0))
        attempt["times_missed"] = max(0, int(review.get("times_missed", 0) or 0)) + 1
        attempt["review_state"] = schedule["review_state"]
        attempt["next_due_at"] = schedule["next_due_at"]
        attempt["mastered"] = False
        attempt["updated_at"] = self.current_timestamp()
        if callable(self.candidate_progress_updater):
            self.candidate_progress_updater(
                data,
                str(attempt.get("candidate_id", "") or "").strip(),
                result="skipped",
                attempt=attempt,
                status_after="candidate",
            )
        return {"ok": True, "error": "", "attempt": dict(attempt)}

    def complete_puzzle_attempt(self, data, attempt_id, final_step=0, total_steps=0):
        attempt = self.find_puzzle_attempt(data, attempt_id=attempt_id)
        if not isinstance(attempt, dict):
            return {"ok": False, "error": "Active puzzle attempt unavailable.", "attempt": None}
        wrong_count = max(0, int(attempt.get("wrong_count", 0) or 0))
        reveal_used = bool(attempt.get("reveal_used", False))
        completed_clean = wrong_count == 0 and not reveal_used
        review = self._copy_puzzle_review_fields(attempt)
        next_clean_streak = max(0, int(review.get("clean_streak", 0) or 0)) + 1 if completed_clean else 0
        next_miss_streak = 0 if completed_clean else max(0, int(review.get("miss_streak", 0) or 0)) + 1
        schedule = self.build_puzzle_review_schedule(clean_streak=next_clean_streak, needs_repeat=not completed_clean)
        attempt["status"] = "completed"
        attempt["completed_at"] = self.current_timestamp()
        attempt["completed_clean"] = completed_clean
        attempt["needs_repeat"] = not completed_clean
        attempt["final_step"] = max(0, int(final_step or 0))
        attempt["total_steps"] = max(max(0, int(attempt.get("total_steps", 0) or 0)), max(0, int(total_steps or 0)))
        attempt["last_attempt_at"] = attempt["completed_at"]
        attempt["clean_streak"] = next_clean_streak
        attempt["miss_streak"] = next_miss_streak
        attempt["times_seen"] = max(1, int(review.get("times_seen", 0) or 0))
        attempt["times_clean"] = max(0, int(review.get("times_clean", 0) or 0)) + (1 if completed_clean else 0)
        attempt["times_missed"] = max(0, int(review.get("times_missed", 0) or 0)) + (0 if completed_clean else 1)
        attempt["review_state"] = schedule["review_state"]
        attempt["next_due_at"] = schedule["next_due_at"]
        attempt["mastered"] = bool(schedule["mastered"])
        attempt["updated_at"] = self.current_timestamp()
        if callable(self.candidate_progress_updater):
            self.candidate_progress_updater(
                data,
                str(attempt.get("candidate_id", "") or "").strip(),
                result="completed",
                attempt=attempt,
                status_after="done",
            )
        return {"ok": True, "error": "", "attempt": dict(attempt)}
