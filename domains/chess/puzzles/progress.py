from datetime import datetime

from .review import normalize_puzzle_review_state


def get_latest_puzzle_attempt_map(data, default_chess_data, puzzle_attempt_sort_value):
    payload = data if isinstance(data, dict) else default_chess_data()
    latest_map = {}
    for raw_attempt in payload.get("puzzle_attempts", []) or []:
        if not isinstance(raw_attempt, dict):
            continue
        candidate_id = str(raw_attempt.get("candidate_id", "") or "").strip()
        if not candidate_id:
            continue
        existing = latest_map.get(candidate_id)
        if existing is None or puzzle_attempt_sort_value(raw_attempt) >= puzzle_attempt_sort_value(existing):
            latest_map[candidate_id] = raw_attempt
    return latest_map


def build_puzzle_attempt_history_map(
    data,
    default_chess_data,
    normalize_puzzle_attempt_status,
    safe_attempt_timestamp_value,
    safe_non_negative_int,
    latest_puzzle_attempt_needs_repeat,
):
    payload = data if isinstance(data, dict) else default_chess_data()
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
        status_value = normalize_puzzle_attempt_status(raw_attempt.get("status"))
        completed_at = str(raw_attempt.get("completed_at", "") or "").strip()
        updated_at = str(raw_attempt.get("updated_at", "") or "").strip()
        created_at = str(raw_attempt.get("created_at", "") or "").strip()
        last_attempt_at = completed_at or updated_at or created_at
        previous_attempt_at = safe_attempt_timestamp_value(entry.get("last_attempt_at"))
        current_attempt_at = safe_attempt_timestamp_value(last_attempt_at)
        if current_attempt_at and (previous_attempt_at is None or current_attempt_at >= previous_attempt_at):
            entry["last_attempt_at"] = last_attempt_at
            entry["last_result"] = status_value
            entry["last_wrong_count"] = safe_non_negative_int(raw_attempt.get("wrong_count", 0), 0)
            entry["last_reveal_used"] = bool(raw_attempt.get("reveal_used", False))
        if status_value == "completed":
            entry["completed_count"] += 1
            if bool(raw_attempt.get("completed_clean", False)):
                entry["clean_completed_count"] += 1
        elif status_value == "skipped":
            entry["skipped_count"] += 1
        if status_value in {"completed", "skipped"} and latest_puzzle_attempt_needs_repeat(raw_attempt):
            entry["difficult_attempt_count"] += 1
        if status_value == "completed" and completed_at:
            previous_completed_at = safe_attempt_timestamp_value(entry.get("last_completed_at"))
            current_completed_at = safe_attempt_timestamp_value(completed_at)
            if current_completed_at and (previous_completed_at is None or current_completed_at >= previous_completed_at):
                entry["last_completed_at"] = completed_at
    return history_map


def normalize_auto_puzzle_progress(raw_state=None, history_entry=None, safe_non_negative_int=None, safe_attempt_timestamp_value=None):
    state = dict(raw_state or {}) if isinstance(raw_state, dict) else {}
    history = dict(history_entry or {}) if isinstance(history_entry, dict) else {}

    def latest_timestamp_text(*values):
        best_text = ""
        best_value = None
        for value in values:
            value_text = str(value or "").strip()
            parsed = safe_attempt_timestamp_value(value_text)
            if parsed is None:
                continue
            if best_value is None or parsed >= best_value:
                best_value = parsed
                best_text = value_text
        return best_text

    completed_count = max(
        safe_non_negative_int(state.get("completed_count", 0), 0),
        safe_non_negative_int(history.get("completed_count", 0), 0),
    )
    clean_completed_count = max(
        safe_non_negative_int(state.get("clean_completed_count", 0), 0),
        safe_non_negative_int(history.get("clean_completed_count", 0), 0),
    )
    skipped_count = max(
        safe_non_negative_int(state.get("skipped_count", 0), 0),
        safe_non_negative_int(history.get("skipped_count", 0), 0),
    )
    difficult_attempt_count = max(
        safe_non_negative_int(state.get("difficult_attempt_count", 0), 0),
        safe_non_negative_int(history.get("difficult_attempt_count", 0), 0),
    )
    attempt_count = max(
        safe_non_negative_int(state.get("attempt_count", 0), 0),
        safe_non_negative_int(history.get("attempt_count", 0), 0),
        completed_count + skipped_count,
    )
    last_attempt_at = latest_timestamp_text(state.get("last_attempt_at", ""), history.get("last_attempt_at", ""))
    last_completed_at = latest_timestamp_text(state.get("last_completed_at", ""), history.get("last_completed_at", ""))
    last_result = str(state.get("last_result", "") or history.get("last_result", "") or "").strip().lower()
    if last_result not in {"completed", "skipped", "started"}:
        last_result = ""
    return {
        "training_key": str(state.get("training_key", "") or history.get("training_key", "") or state.get("id", "") or "").strip(),
        "attempt_count": attempt_count,
        "completed_count": completed_count,
        "clean_completed_count": clean_completed_count,
        "skipped_count": skipped_count,
        "difficult_attempt_count": difficult_attempt_count,
        "last_attempt_at": last_attempt_at,
        "last_completed_at": last_completed_at,
        "last_result": last_result,
        "last_wrong_count": max(
            safe_non_negative_int(state.get("last_wrong_count", 0), 0),
            safe_non_negative_int(history.get("last_wrong_count", 0), 0),
        ),
        "last_reveal_used": bool(state.get("last_reveal_used", history.get("last_reveal_used", False))),
    }


def record_auto_puzzle_candidate_progress(
    data,
    candidate_id,
    *,
    result="",
    attempt=None,
    status_after=None,
    default_chess_data,
    current_timestamp,
    normalize_candidate_status,
    build_puzzle_attempt_history_map,
    normalize_auto_puzzle_progress,
    safe_non_negative_int,
):
    payload = data if isinstance(data, dict) else default_chess_data()
    target_id = str(candidate_id or "").strip()
    if not target_id:
        return {"changed": False, "item": None, "error": "Missing auto candidate id."}
    attempt_payload = dict(attempt or {}) if isinstance(attempt, dict) else {}
    status_value = normalize_candidate_status(status_after or ("done" if str(result or "").strip().lower() == "completed" else "candidate"))
    attempt_history = build_puzzle_attempt_history_map(payload).get(target_id, {})
    progress_fields = normalize_auto_puzzle_progress({
        "training_key": target_id,
        "last_result": str(result or "").strip().lower(),
        "last_completed_at": str(attempt_payload.get("completed_at", "") or "").strip(),
        "last_attempt_at": str(attempt_payload.get("completed_at", "") or attempt_payload.get("updated_at", "") or attempt_payload.get("created_at", "") or "").strip(),
        "last_wrong_count": safe_non_negative_int(attempt_payload.get("wrong_count", 0), 0),
        "last_reveal_used": bool(attempt_payload.get("reveal_used", False)),
    }, history_entry=attempt_history)
    for item in payload.get("auto_puzzle_candidates", []) or []:
        if not isinstance(item, dict):
            continue
        if str(item.get("id", "") or "").strip() != target_id:
            continue
        changed = False
        if normalize_candidate_status(item.get("status", "candidate")) != status_value:
            item["status"] = status_value
            changed = True
        for key, value in progress_fields.items():
            if item.get(key) != value:
                item[key] = value
                changed = True
        if changed:
            item["updated_at"] = current_timestamp()
        return {"changed": changed, "item": dict(item), "error": ""}
    new_item = {
        "id": target_id,
        "status": status_value,
        "saved_seed_id": "",
        "created_at": current_timestamp(),
        "updated_at": current_timestamp(),
    }
    new_item.update(progress_fields)
    payload.setdefault("auto_puzzle_candidates", []).append(new_item)
    return {"changed": True, "item": dict(new_item), "error": ""}


def get_candidate_repeat_state(
    candidate,
    latest_attempt,
    *,
    normalize_candidate_status,
    normalize_puzzle_attempt_status,
    latest_puzzle_attempt_needs_repeat,
    puzzle_attempt_due_now,
    parse_timestamp,
    puzzle_attempt_sort_value,
    build_puzzle_repeat_note,
    build_puzzle_attempt_summary,
):
    candidate_payload = dict(candidate or {}) if isinstance(candidate, dict) else {}
    attempt_payload = dict(latest_attempt or {}) if isinstance(latest_attempt, dict) else {}
    base_status = normalize_candidate_status(candidate_payload.get("status", "candidate")) if callable(normalize_candidate_status) else str(candidate_payload.get("status", "candidate") or "").strip().lower()
    if base_status == "archived":
        repeat_note = build_puzzle_repeat_note(candidate_payload, attempt_payload)
        return {
            "repeat_needed": False,
            "effective_status": "archived",
            "latest_attempt": attempt_payload,
            "latest_attempt_status": normalize_puzzle_attempt_status(attempt_payload.get("status")) if attempt_payload else "",
            "summary": build_puzzle_attempt_summary(attempt_payload),
            "reason": "",
            "coach_note": "",
            "repeat_note": repeat_note,
        }

    latest_status = normalize_puzzle_attempt_status(attempt_payload.get("status")) if attempt_payload else ""
    repeat_needed = latest_puzzle_attempt_needs_repeat(attempt_payload)
    due_now = puzzle_attempt_due_now(attempt_payload)
    mastered = bool(attempt_payload.get("mastered", False))
    review_state = normalize_puzzle_review_state(attempt_payload.get("review_state"))
    candidate_status_timestamp = (
        parse_timestamp(candidate_payload.get("updated_at", ""))
        or parse_timestamp(candidate_payload.get("created_at", ""))
        or datetime.min
    )
    attempt_timestamp = puzzle_attempt_sort_value(attempt_payload) if attempt_payload else datetime.min
    effective_status = base_status
    repeat_note = build_puzzle_repeat_note(candidate_payload, attempt_payload)
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
        "summary": build_puzzle_attempt_summary(attempt_payload),
        "reason": reason,
        "coach_note": coach_note,
        "repeat_note": repeat_note,
    }
