from datetime import datetime, timedelta, timezone


def normalize_puzzle_review_state(value):
    normalized = str(value or "").strip().lower()
    if normalized in {"new", "repeat_due", "scheduled", "review_due", "mastered"}:
        return normalized
    return "new"


def build_puzzle_review_schedule(clean_streak=0, needs_repeat=False, skipped=False):
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


def latest_puzzle_attempt_needs_repeat(attempt):
    payload = dict(attempt or {}) if isinstance(attempt, dict) else {}
    if not payload:
        return False
    status_value = str(payload.get("status", "") or "").strip().lower()
    if status_value == "skipped":
        return True
    if bool(payload.get("needs_repeat", False)):
        return True
    if max(0, int(payload.get("wrong_count", 0) or 0)) > 0:
        return True
    if bool(payload.get("reveal_used", False)):
        return True
    return False


def puzzle_attempt_due_now(attempt, parse_timestamp, now=None):
    payload = dict(attempt or {}) if isinstance(attempt, dict) else {}
    if not payload:
        return False
    status_value = str(payload.get("status", "") or "").strip().lower()
    if status_value in {"started", "archived"}:
        return False
    if latest_puzzle_attempt_needs_repeat(payload):
        return True
    mastered = bool(payload.get("mastered", False))
    due_value = parse_timestamp(payload.get("next_due_at")) or None
    if due_value is None:
        return not mastered
    compare_now = now if isinstance(now, datetime) else datetime.now(timezone.utc)
    return due_value <= compare_now
