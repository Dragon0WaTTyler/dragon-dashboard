"""Chess runtime orchestration ownership."""

from datetime import datetime, timezone

from ..puzzles.progress import (
    normalize_auto_puzzle_progress as normalize_auto_puzzle_progress_payload,
    record_auto_puzzle_candidate_progress as record_auto_puzzle_candidate_progress_payload,
)
from ..services import LichessProgressService
from ..services.puzzle_attempt_service import configure_puzzle_attempt_service
from .chess_storage import configure_chess_storage


class ChessRuntime:
    def __init__(self):
        self.current_timestamp = None
        self.parse_timestamp = None
        self.default_chess_data = None
        self.safe_non_negative_int = None
        self.normalize_candidate_status = None
        self.format_move_label = None
        self.storage = None
        self.puzzle_attempt_service = None
        self.lichess_progress_service = None

    def configure(
        self,
        *,
        current_timestamp,
        parse_timestamp,
        default_chess_data,
        safe_non_negative_int,
        normalize_candidate_status,
        format_move_label,
        chess_data_path,
        load_json_file,
        save_json_file,
    ):
        self.current_timestamp = current_timestamp
        self.parse_timestamp = parse_timestamp
        self.default_chess_data = default_chess_data
        self.safe_non_negative_int = safe_non_negative_int
        self.normalize_candidate_status = normalize_candidate_status
        self.format_move_label = format_move_label
        self.puzzle_attempt_service = configure_puzzle_attempt_service(
            current_timestamp=current_timestamp,
            parse_timestamp=parse_timestamp,
            default_chess_data=default_chess_data,
            safe_non_negative_int=safe_non_negative_int,
            normalize_candidate_status=normalize_candidate_status,
            format_move_label=format_move_label,
        )
        self.lichess_progress_service = LichessProgressService(
            current_timestamp=current_timestamp,
            default_chess_data=default_chess_data,
        )
        self.storage = configure_chess_storage(
            chess_data_path=chess_data_path,
            load_json_file=load_json_file,
            save_json_file=save_json_file,
            current_timestamp=current_timestamp,
            coerce_lichess_progress_map=self.lichess_progress_service.coerce_lichess_progress_map,
        )
        self.puzzle_attempt_service.set_candidate_progress_updater(self.record_candidate_progress)
        return self

    def _require_service(self, service, name):
        if service is None:
            raise RuntimeError(f"Chess runtime is not configured: missing {name}.")
        return service

    def _puzzle_service(self):
        return self._require_service(self.puzzle_attempt_service, "puzzle_attempt_service")

    def _lichess_service(self):
        return self._require_service(self.lichess_progress_service, "lichess_progress_service")

    def normalize_puzzle_attempt_status(self, value):
        return self._puzzle_service().normalize_puzzle_attempt_status(value)

    def normalize_puzzle_review_state(self, value):
        return self._puzzle_service().normalize_puzzle_review_state(value)

    def build_attempt_id(self, candidate_id, started_at=""):
        return self._puzzle_service().build_puzzle_attempt_id(candidate_id, started_at=started_at)

    def safe_attempt_int(self, value, default=0):
        return self._puzzle_service().safe_puzzle_attempt_int(value, default=default)

    def safe_attempt_timestamp_value(self, value):
        return self._puzzle_service()._safe_attempt_timestamp_value(value)

    def copy_puzzle_review_fields(self, attempt):
        return self._puzzle_service()._copy_puzzle_review_fields(attempt)

    def build_review_schedule(self, clean_streak=0, needs_repeat=False, skipped=False):
        return self._puzzle_service().build_puzzle_review_schedule(
            clean_streak=clean_streak,
            needs_repeat=needs_repeat,
            skipped=skipped,
        )

    def find_previous_resolved_attempt(self, data, candidate_id="", exclude_attempt_id=""):
        return self._puzzle_service().find_previous_resolved_puzzle_attempt(
            data,
            candidate_id=candidate_id,
            exclude_attempt_id=exclude_attempt_id,
        )

    def attempt_due_now(self, attempt, now=None):
        return self._puzzle_service().puzzle_attempt_due_now(attempt, now=now)

    def find_attempt(self, data, attempt_id="", candidate_id="", status=None):
        return self._puzzle_service().find_puzzle_attempt(
            data,
            attempt_id=attempt_id,
            candidate_id=candidate_id,
            status=status,
        )

    def build_attempt_summary(self, attempt):
        return self._puzzle_service().build_puzzle_attempt_summary(attempt)

    def get_latest_attempt_map(self, data):
        return self._puzzle_service().get_latest_puzzle_attempt_map(data)

    def build_attempt_history_map(self, data):
        return self._puzzle_service().build_puzzle_attempt_history_map(data)

    def latest_attempt_needs_repeat(self, attempt):
        return self._puzzle_service().latest_puzzle_attempt_needs_repeat(attempt)

    def build_repeat_note(self, candidate, latest_attempt):
        return self._puzzle_service().build_puzzle_repeat_note(candidate, latest_attempt)

    def get_candidate_repeat_state(self, candidate, latest_attempt):
        return self._puzzle_service().get_candidate_repeat_state(candidate, latest_attempt)

    def get_or_create_active_attempt(self, data, candidate, total_steps=0):
        return self._puzzle_service().get_or_create_active_puzzle_attempt(
            data,
            candidate,
            total_steps=total_steps,
        )

    def normalize_auto_puzzle_progress(self, raw_state=None, history_entry=None):
        return normalize_auto_puzzle_progress_payload(
            raw_state,
            history_entry=history_entry,
            safe_non_negative_int=self.safe_non_negative_int,
            safe_attempt_timestamp_value=self.safe_attempt_timestamp_value,
        )

    def compute_auto_puzzle_rotation_penalty(self, progress, repeat_needed=False, due_now=False, mastered=False):
        payload = dict(progress or {}) if isinstance(progress, dict) else {}
        penalty = 0
        completed_count = self.safe_non_negative_int(payload.get("completed_count", 0), 0)
        clean_completed_count = self.safe_non_negative_int(payload.get("clean_completed_count", 0), 0)
        skipped_count = self.safe_non_negative_int(payload.get("skipped_count", 0), 0)
        difficult_attempt_count = self.safe_non_negative_int(payload.get("difficult_attempt_count", 0), 0)
        attempt_count = self.safe_non_negative_int(payload.get("attempt_count", 0), 0)
        recent_text = str(payload.get("last_completed_at", "") or payload.get("last_attempt_at", "") or "").strip()
        recent_at = self.safe_attempt_timestamp_value(recent_text)
        now = datetime.now(timezone.utc)
        if recent_at is not None:
            age_hours = max(0.0, (now - recent_at).total_seconds() / 3600.0)
            if age_hours < 1:
                penalty += 130
            elif age_hours < 6:
                penalty += 100
            elif age_hours < 24:
                penalty += 72
            elif age_hours < 72:
                penalty += 44
            elif age_hours < 168:
                penalty += 20
            else:
                penalty += 8
        penalty += min(completed_count, 6) * 8
        penalty += min(clean_completed_count, 4) * 5
        penalty += min(skipped_count, 4) * 4
        penalty += min(attempt_count, 6) * 2
        if mastered:
            penalty += 24
        if repeat_needed:
            penalty = max(12, int(round(penalty * 0.45)))
            penalty += min(difficult_attempt_count, 4) * 3
        elif due_now and not mastered:
            penalty = max(10, int(round(penalty * 0.65)))
        return max(0, int(penalty))

    def record_candidate_progress(self, data, candidate_id, result="", attempt=None, status_after=None):
        return record_auto_puzzle_candidate_progress_payload(
            data,
            candidate_id,
            result=result,
            attempt=attempt,
            status_after=status_after,
            default_chess_data=self.default_chess_data,
            current_timestamp=self.current_timestamp,
            normalize_candidate_status=self.normalize_candidate_status,
            build_puzzle_attempt_history_map=self.build_attempt_history_map,
            normalize_auto_puzzle_progress=self.normalize_auto_puzzle_progress,
            safe_non_negative_int=self.safe_non_negative_int,
        )

    def record_wrong_move(
        self,
        data,
        attempt_id,
        step_index=0,
        fen="",
        attempted_move_uci="",
        attempted_move_san="",
        expected_move_uci="",
        engine_move_uci="",
    ):
        return self._puzzle_service().record_puzzle_wrong_move(
            data,
            attempt_id,
            step_index=step_index,
            fen=fen,
            attempted_move_uci=attempted_move_uci,
            attempted_move_san=attempted_move_san,
            expected_move_uci=expected_move_uci,
            engine_move_uci=engine_move_uci,
        )

    def record_correct_move(self, data, attempt_id, step_index=0):
        return self._puzzle_service().record_puzzle_correct_move(data, attempt_id, step_index=step_index)

    def record_reveal(self, data, attempt_id):
        return self._puzzle_service().record_puzzle_reveal(data, attempt_id)

    def record_engine_check(self, data, attempt_id):
        return self._puzzle_service().record_puzzle_engine_check(data, attempt_id)

    def record_critical_moment_check(self, data, attempt_id):
        return self._puzzle_service().record_puzzle_critical_moment_check(data, attempt_id)

    def skip_attempt(self, data, attempt_id, final_step=0, total_steps=0):
        return self._puzzle_service().skip_puzzle_attempt(
            data,
            attempt_id,
            final_step=final_step,
            total_steps=total_steps,
        )

    def complete_attempt(self, data, attempt_id, final_step=0, total_steps=0):
        return self._puzzle_service().complete_puzzle_attempt(
            data,
            attempt_id,
            final_step=final_step,
            total_steps=total_steps,
        )

    def normalize_lichess_progress_status(self, value):
        return self._lichess_service().normalize_lichess_progress_status(value)

    def get_lichess_puzzle_progress_map(self, data):
        return self._lichess_service().get_lichess_puzzle_progress_map(data)

    def record_lichess_puzzle_started(self, data, puzzle_id=""):
        return self._lichess_service().record_lichess_puzzle_started(data, puzzle_id)

    def record_lichess_puzzle_wrong_move(self, data, puzzle_id="", attempted_move=""):
        return self._lichess_service().record_lichess_puzzle_wrong_move(
            data,
            puzzle_id,
            attempted_move=attempted_move,
        )

    def record_lichess_puzzle_reveal(self, data, puzzle_id=""):
        return self._lichess_service().record_lichess_puzzle_reveal(data, puzzle_id)

    def record_lichess_puzzle_skipped(self, data, puzzle_id=""):
        return self._lichess_service().record_lichess_puzzle_skipped(data, puzzle_id)

    def record_lichess_puzzle_complete(self, data, puzzle_id="", completed_clean=True):
        return self._lichess_service().record_lichess_puzzle_complete(
            data,
            puzzle_id,
            completed_clean=completed_clean,
        )

    def classify_lichess_progress_bucket(self, progress_entry):
        return self._lichess_service().classify_lichess_progress_bucket(progress_entry)

    def build_lichess_progress_snapshot(self, data, valid_items=None):
        return self._lichess_service().build_lichess_progress_snapshot(data, valid_items=valid_items)


CHESS_RUNTIME = ChessRuntime()

