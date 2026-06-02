from __future__ import annotations

import copy
import re
from collections import Counter, defaultdict
from datetime import timezone


class ReadingRecipeOfDayService:
    """Build and persist a small deterministic Recipe of the Day snapshot."""

    RECIPE_VERSION = 1
    MAX_SELECTION = 7
    RECENT_WINDOW_HOURS = 24
    _STOPWORDS = {
        "about",
        "after",
        "again",
        "against",
        "also",
        "around",
        "before",
        "being",
        "between",
        "could",
        "from",
        "have",
        "into",
        "news",
        "not",
        "opinion",
        "other",
        "over",
        "people",
        "should",
        "since",
        "story",
        "that",
        "their",
        "there",
        "these",
        "they",
        "this",
        "those",
        "today",
        "topic",
        "update",
        "world",
        "your",
    }
    _INTEREST_KEYWORDS = {
        "ai",
        "analysis",
        "books",
        "business",
        "culture",
        "design",
        "economy",
        "flask",
        "history",
        "media",
        "morocco",
        "openai",
        "politics",
        "python",
        "research",
        "science",
        "technology",
        "trump",
        "war",
    }

    def __init__(
        self,
        *,
        app_logger,
        load_reading_data_cached,
        default_reading_data,
        reading_runtime_projection_service,
        normalize_reading_status,
        parse_timestamp,
        format_timestamp_label,
        normalize_reading_url,
        save_json_file,
        load_json_file,
        reading_recipe_of_day_path,
        datetime_module,
        monotonic,
    ):
        self.app_logger = app_logger
        self.load_reading_data_cached = load_reading_data_cached
        self.default_reading_data = default_reading_data
        self.reading_runtime_projection_service = reading_runtime_projection_service
        self.normalize_reading_status = normalize_reading_status
        self.parse_timestamp = parse_timestamp
        self.format_timestamp_label = format_timestamp_label
        self.normalize_reading_url = normalize_reading_url
        self.save_json_file = save_json_file
        self.load_json_file = load_json_file
        self.reading_recipe_of_day_path = reading_recipe_of_day_path
        self.datetime_module = datetime_module
        self.monotonic = monotonic

    def _now(self):
        return self.datetime_module.now(timezone.utc).astimezone()

    def _today_key(self):
        return self._now().date().isoformat()

    def _tokenize(self, value):
        raw = re.sub(r"[^A-Za-z0-9]+", " ", str(value or "").lower())
        tokens = []
        for token in raw.split():
            if len(token) < 3 or token.isdigit() or token in self._STOPWORDS:
                continue
            tokens.append(token)
        return tokens

    def _build_interest_keyword_profile(self, entries):
        counts = Counter()
        for entry in entries:
            entry = entry if isinstance(entry, dict) else {}
            for token in self._tokenize(" ".join([
                str(entry.get("title", "") or ""),
                str(entry.get("topic", "") or ""),
                str(entry.get("source", "") or ""),
            ])):
                counts[token] += 1
        keywords = []
        for token, count in counts.items():
            if count >= 2 or token in self._INTEREST_KEYWORDS:
                keywords.append((token, count))
        keywords.sort(key=lambda item: (-item[1], item[0]))
        return [token for token, _count in keywords[:24]]

    def _parse_entry_timestamp(self, entry):
        entry = entry if isinstance(entry, dict) else {}
        for field in ("published_at", "added_at", "imported_at"):
            timestamp = self.parse_timestamp(entry.get(field, ""))
            if timestamp:
                if timestamp.tzinfo is None:
                    timestamp = timestamp.replace(tzinfo=timezone.utc)
                return timestamp.astimezone()
        return None

    def _normalize_title_key(self, entry):
        entry = entry if isinstance(entry, dict) else {}
        title = re.sub(r"\s+", " ", str(entry.get("title", "") or "").strip().lower())
        title = re.sub(r"[^a-z0-9\s]+", "", title)
        title = re.sub(r"\s+", " ", title).strip()
        source = re.sub(r"\s+", " ", str(entry.get("source", "") or "").strip().lower())
        source = re.sub(r"[^a-z0-9\s]+", "", source)
        source = re.sub(r"\s+", " ", source).strip()
        if title and source:
            return f"title:{title}|source:{source}"
        if title:
            return f"title:{title}"
        return ""

    def _dedupe_keys(self, entry):
        entry = entry if isinstance(entry, dict) else {}
        keys = []
        for field in ("url", "original_url", "canonical_url"):
            normalized_url = self.normalize_reading_url(entry.get(field, "") or "")
            if normalized_url:
                keys.append(f"url:{normalized_url}")
        title_key = self._normalize_title_key(entry)
        if title_key:
            keys.append(title_key)
        if not keys:
            fallback = str(entry.get("id", "") or "").strip()
            if fallback:
                keys.append(f"id:{fallback.lower()}")
        return keys

    def _source_key_for_entry(self, entry, source=None):
        entry = entry if isinstance(entry, dict) else {}
        source = source if isinstance(source, dict) else {}
        source_id = str(source.get("id", "") or entry.get("source_id", "") or "").strip()
        if source_id:
            return f"id:{source_id.lower()}"
        source_name = str(source.get("name", "") or entry.get("source", "") or "").strip().lower()
        if source_name:
            return f"name:{re.sub(r'[^a-z0-9]+', '-', source_name).strip('-')}"
        return "unknown"

    def _source_quality(self, source):
        source = source if isinstance(source, dict) else {}
        score = 0
        tags = []
        if not source.get("active", True):
            return -12, ["source:inactive"]
        if str(source.get("url", "") or "").strip():
            score += 3
        status = str(source.get("last_sync_status", "") or "").strip().lower()
        imported_count = int(source.get("last_sync_imported_count", 0) or 0)
        raw_count = int(source.get("last_sync_raw_count", 0) or source.get("last_sync_count", 0) or 0)
        zero_import_streak = int(source.get("last_sync_zero_import_streak", 0) or 0)
        missing_key_count = int(source.get("last_sync_missing_key_count", 0) or 0)
        if status == "ok":
            score += 12
            tags.append("source:ok")
        elif status == "blocked_source":
            score -= 12
            tags.append("source:blocked")
        elif status == "error":
            score -= 8
            tags.append("source:error")
        elif status:
            score += 3
            tags.append(f"source:{status}")
        else:
            tags.append("source:quiet")
        if imported_count > 0:
            score += min(8, imported_count * 2)
        elif raw_count > 0:
            score += 2
        if zero_import_streak >= 3:
            score -= 3
        if missing_key_count:
            score -= 2
        last_synced_at = self.parse_timestamp(source.get("last_synced_at", ""))
        if last_synced_at:
            if last_synced_at.tzinfo is None:
                last_synced_at = last_synced_at.replace(tzinfo=timezone.utc)
            age_hours = max((self._now() - last_synced_at.astimezone()).total_seconds() / 3600.0, 0.0)
            if age_hours <= self.RECENT_WINDOW_HOURS:
                score += 4
        return max(-12, min(20, score)), tags

    def _freshness_score(self, entry_timestamp):
        if not entry_timestamp:
            return 0, ["freshness:unknown"], False, None
        age_hours = max((self._now() - entry_timestamp).total_seconds() / 3600.0, 0.0)
        if age_hours <= 1:
            score = 30
        elif age_hours <= 6:
            score = 26
        elif age_hours <= 12:
            score = 22
        elif age_hours <= 24:
            score = 18
        elif age_hours <= 72:
            score = 8
        else:
            score = 2
        if age_hours < 24:
            tag = f"fresh-{int(max(age_hours, 0.0))}h"
            recent = True
        else:
            tag = f"aged-{int(age_hours // 24)}d"
            recent = False
        return score, [tag], recent, age_hours

    def _status_score(self, status):
        normalized = self.normalize_reading_status(status)
        if normalized == "unread":
            return 14, ["unread"]
        if normalized == "reading":
            return 8, ["reading"]
        if normalized == "finished":
            return -10, ["finished"]
        if normalized == "archived":
            return -18, ["archived"]
        return 0, [normalized or "unknown"]

    def _interest_score(self, entry, keyword_profile):
        entry = entry if isinstance(entry, dict) else {}
        haystack = " ".join([
            str(entry.get("title", "") or ""),
            str(entry.get("topic", "") or ""),
            str(entry.get("source", "") or ""),
        ]).lower()
        matches = []
        for keyword in keyword_profile:
            keyword = str(keyword or "").strip().lower()
            if not keyword:
                continue
            if re.search(rf"(^|[^a-z0-9]){re.escape(keyword)}([^a-z0-9]|$)", haystack):
                matches.append(keyword)
        if not matches:
            return 0, []
        score = min(20, len(matches) * 5)
        tags = [f"interest:{keyword}" for keyword in matches[:3]]
        return score, tags

    def _selection_sort_key(self, candidate):
        candidate = candidate if isinstance(candidate, dict) else {}
        published_ts = candidate.get("published_timestamp")
        added_ts = candidate.get("added_timestamp")
        published_value = published_ts.timestamp() if published_ts else float("-inf")
        added_value = added_ts.timestamp() if added_ts else float("-inf")
        return (
            -int(candidate.get("score", 0) or 0),
            -int(candidate.get("freshness_score", 0) or 0),
            -int(candidate.get("source_quality_score", 0) or 0),
            -int(candidate.get("interest_score", 0) or 0),
            -int(candidate.get("star_score", 0) or 0),
            -self._status_priority(candidate.get("status", "")),
            -published_value,
            -added_value,
            str(candidate.get("source_key", "") or ""),
            str(candidate.get("title", "") or "").lower(),
            str(candidate.get("id", "") or ""),
        )

    def score_article_candidate(self, candidate, source=None, keyword_profile=None):
        candidate = candidate if isinstance(candidate, dict) else {}
        source = source if isinstance(source, dict) else {}
        keyword_profile = list(keyword_profile or [])
        entry_timestamp = self._parse_entry_timestamp(candidate)
        freshness_score, freshness_tags, is_recent, age_hours = self._freshness_score(entry_timestamp)
        source_score, source_tags = self._source_quality(source)
        status_score, status_tags = self._status_score(candidate.get("status", ""))
        interest_score, interest_tags = self._interest_score(candidate, keyword_profile)
        star_score = 4 if candidate.get("starred") else 0
        tags = []
        for tag in freshness_tags + source_tags + status_tags + interest_tags:
            if tag and tag not in tags:
                tags.append(tag)
        if star_score:
            tags.append("starred")
        score = freshness_score + source_score + status_score + interest_score + star_score
        score = max(0, min(100, int(round(score))))
        return {
            "score": score,
            "freshness_score": freshness_score,
            "source_quality_score": source_score,
            "status_score": status_score,
            "interest_score": interest_score,
            "star_score": star_score,
            "age_hours": age_hours,
            "is_recent": is_recent,
            "reason_tags": tags[:5],
        }

    def get_candidate_reason_tags(self, candidate):
        candidate = candidate if isinstance(candidate, dict) else {}
        tags = list(candidate.get("reason_tags", []) or [])
        if candidate.get("starred") and "starred" not in tags:
            tags.append("starred")
        if candidate.get("status") and candidate.get("status") not in tags:
            tags.append(str(candidate.get("status", "")).strip())
        return [tag for tag in tags if tag][:5]

    def load_recipe_snapshot(self):
        payload = self.load_json_file(self.reading_recipe_of_day_path, {})
        if not isinstance(payload, dict):
            payload = {}
        payload.setdefault("version", self.RECIPE_VERSION)
        recipes_by_date = payload.get("recipes_by_date", {})
        if not isinstance(recipes_by_date, dict):
            recipes_by_date = {}
        payload["recipes_by_date"] = recipes_by_date
        return payload

    def save_recipe_snapshot(self, payload):
        payload = payload if isinstance(payload, dict) else {}
        payload.setdefault("version", self.RECIPE_VERSION)
        payload.setdefault("recipes_by_date", {})
        self.save_json_file(self.reading_recipe_of_day_path, payload)
        return payload

    def _status_priority(self, status):
        normalized = self.normalize_reading_status(status)
        return {
            "unread": 4,
            "reading": 3,
            "finished": 1,
            "archived": 0,
        }.get(normalized, 2)

    def _candidate_rank_key(self, candidate):
        candidate = candidate if isinstance(candidate, dict) else {}
        published_ts = candidate.get("published_timestamp")
        added_ts = candidate.get("added_timestamp")
        return (
            int(candidate.get("score", 0) or 0),
            int(candidate.get("freshness_score", 0) or 0),
            int(candidate.get("source_quality_score", 0) or 0),
            int(candidate.get("interest_score", 0) or 0),
            int(candidate.get("star_score", 0) or 0),
            self._status_priority(candidate.get("status", "")),
            published_ts.timestamp() if published_ts else 0.0,
            added_ts.timestamp() if added_ts else 0.0,
            str(candidate.get("title", "") or "").lower(),
            str(candidate.get("source", "") or "").lower(),
            str(candidate.get("id", "") or ""),
        )

    def _serialize_candidate(self, candidate):
        candidate = candidate if isinstance(candidate, dict) else {}
        reason_tags = list(candidate.get("reason_tags", []) or [])
        selection_phase = str(candidate.get("recipe_phase", "") or "").strip()
        if selection_phase:
            phase_tag = "source:first-pass" if selection_phase == "source-first-pass" else "source:second-pass"
            if phase_tag not in reason_tags:
                reason_tags.append(phase_tag)
        return {
            "id": str(candidate.get("id", "") or "").strip(),
            "title": str(candidate.get("title", "") or "").strip(),
            "title_dir": str(candidate.get("title_dir", "") or "auto").strip() or "auto",
            "source": str(candidate.get("source", "") or "").strip() or "Unknown Source",
            "source_dir": str(candidate.get("source_dir", "") or "auto").strip() or "auto",
            "source_id": str(candidate.get("source_id", "") or "").strip(),
            "source_key": str(candidate.get("source_key", "") or "").strip(),
            "url": str(candidate.get("url", "") or "").strip(),
            "original_url": str(candidate.get("original_url", "") or "").strip(),
            "published_at": str(candidate.get("published_at", "") or "").strip(),
            "published_display": str(candidate.get("published_display", "") or "").strip(),
            "added_at": str(candidate.get("added_at", "") or "").strip(),
            "added_display": str(candidate.get("added_display", "") or "").strip(),
            "status": str(candidate.get("status", "") or "").strip(),
            "starred": bool(candidate.get("starred", False)),
            "topic": str(candidate.get("topic", "") or "").strip(),
            "topic_display": str(candidate.get("topic_display", "") or "").strip(),
            "category": str(candidate.get("category", "") or "").strip(),
            "score": int(candidate.get("score", 0) or 0),
            "freshness_score": int(candidate.get("freshness_score", 0) or 0),
            "source_quality_score": int(candidate.get("source_quality_score", 0) or 0),
            "status_score": int(candidate.get("status_score", 0) or 0),
            "interest_score": int(candidate.get("interest_score", 0) or 0),
            "star_score": int(candidate.get("star_score", 0) or 0),
            "age_hours": None if candidate.get("age_hours") is None else round(float(candidate.get("age_hours", 0.0) or 0.0), 2),
            "is_recent": bool(candidate.get("is_recent", False)),
            "recipe_phase": selection_phase,
            "reason_tags": reason_tags,
            "score_breakdown": dict(candidate.get("score_breakdown", {}) or {}),
        }

    def _select_candidates(self, entries, sources_by_id, sources_by_name):
        keyword_profile = self._build_interest_keyword_profile(entries)
        enriched = []
        seen_keys = {}
        for entry in entries:
            entry = entry if isinstance(entry, dict) else {}
            source_id = str(entry.get("source_id", "") or "").strip()
            source_name = str(entry.get("source", "") or "").strip()
            source = sources_by_id.get(source_id) or sources_by_name.get(source_name.lower()) or {}
            scores = self.score_article_candidate(entry, source=source, keyword_profile=keyword_profile)
            published_ts = self._parse_entry_timestamp(entry)
            added_ts = self.parse_timestamp(entry.get("added_at", "") or entry.get("imported_at", ""))
            if published_ts and published_ts.tzinfo is None:
                published_ts = published_ts.replace(tzinfo=timezone.utc)
            if added_ts and added_ts.tzinfo is None:
                added_ts = added_ts.replace(tzinfo=timezone.utc)
            candidate = {
                "id": str(entry.get("id", "") or "").strip(),
                "title": str(entry.get("title", "") or "").strip() or "Untitled article",
                "title_dir": str(entry.get("title_dir", "") or "").strip() or "auto",
                "source": source_name or "Unknown Source",
                "source_dir": str(entry.get("source_dir", "") or "").strip() or "auto",
                "source_id": source_id,
                "source_key": self._source_key_for_entry(entry, source=source),
                "url": str(entry.get("url", "") or "").strip(),
                "original_url": str(entry.get("original_url", "") or entry.get("url", "") or "").strip(),
                "published_at": str(entry.get("published_at", "") or "").strip(),
                "published_timestamp": published_ts,
                "published_display": str(entry.get("published_display", "") or "").strip() or self.format_timestamp_label(str(entry.get("published_at", "") or ""), default=""),
                "added_at": str(entry.get("added_at", "") or "").strip(),
                "added_timestamp": added_ts,
                "added_display": str(entry.get("added_display", "") or "").strip() or self.format_timestamp_label(str(entry.get("added_at", "") or ""), default=""),
                "status": self.normalize_reading_status(entry.get("status", "")),
                "starred": bool(entry.get("starred", False)),
                "topic": str(entry.get("topic", "") or "").strip(),
                "topic_display": str(entry.get("topic_display", "") or "").strip(),
                "category": str(entry.get("category", "") or "").strip(),
                "reason_tags": [],
                "score_breakdown": {},
            }
            candidate.update(scores)
            candidate["reason_tags"] = self.get_candidate_reason_tags(candidate)
            candidate["score_breakdown"] = {
                "freshness": candidate["freshness_score"],
                "source_quality": candidate["source_quality_score"],
                "status": candidate["status_score"],
                "interest": candidate["interest_score"],
                "star": candidate["star_score"],
            }
            candidate["sort_key"] = self._candidate_rank_key(candidate)
            candidate["selection_key"] = self._selection_sort_key(candidate)
            enriched.append(candidate)

        deduped = []
        for candidate in enriched:
            candidate_keys = self._dedupe_keys(candidate)
            chosen_key = None
            for key in candidate_keys:
                if key in seen_keys:
                    chosen_key = key
                    break
            if chosen_key is None:
                chosen_key = candidate_keys[0] if candidate_keys else ""
            if chosen_key and chosen_key in seen_keys:
                existing_index = seen_keys[chosen_key]
                existing = deduped[existing_index]
                if candidate["sort_key"] > existing["sort_key"]:
                    deduped[existing_index] = candidate
                    for key in self._dedupe_keys(existing):
                        seen_keys[key] = existing_index
                    for key in candidate_keys:
                        seen_keys[key] = existing_index
                continue
            deduped.append(candidate)
            current_index = len(deduped) - 1
            for key in candidate_keys:
                seen_keys[key] = current_index

        active_candidates = [candidate for candidate in deduped if candidate["status"] not in {"archived", "finished"}]
        recent_active = [candidate for candidate in active_candidates if candidate["is_recent"]]
        if len(recent_active) >= self.MAX_SELECTION:
            pool = recent_active
        elif len(active_candidates) >= self.MAX_SELECTION:
            pool = active_candidates
        else:
            pool = active_candidates + [candidate for candidate in deduped if candidate["status"] in {"archived", "finished"}]

        grouped = defaultdict(list)
        for candidate in pool:
            grouped[candidate["source_key"]].append(candidate)
        for group in grouped.values():
            group.sort(key=lambda candidate: candidate["selection_key"])

        source_order = sorted(
            grouped.items(),
            key=lambda item: (
                item[1][0]["selection_key"],
                item[0],
            ),
        )

        selected = []
        selected_counts = defaultdict(int)
        for source_key, group in source_order:
            if len(selected) >= self.MAX_SELECTION:
                break
            best_candidate = group[0]
            best_candidate["recipe_phase"] = "source-first-pass"
            selected.append(best_candidate)
            selected_counts[source_key] += 1

        remaining = []
        for source_key, group in grouped.items():
            for candidate in group[1:]:
                remaining.append(candidate)
        remaining.sort(key=lambda candidate: candidate["selection_key"])
        for candidate in remaining:
            if len(selected) >= self.MAX_SELECTION:
                break
            source_key = candidate["source_key"]
            if selected_counts[source_key] >= 2:
                continue
            candidate["recipe_phase"] = "source-second-pass"
            selected.append(candidate)
            selected_counts[source_key] += 1

        for index, candidate in enumerate(selected):
            candidate["recipe_index"] = index
        return {
            "selected_articles": [self._serialize_candidate(candidate) for candidate in selected],
            "candidate_count": len(deduped),
            "active_candidate_count": len(active_candidates),
            "recent_active_count": len(recent_active),
            "source_count": len(grouped),
            "keyword_profile": keyword_profile,
        }

    def build_today_recipe(self, force=False):
        started_at = self.monotonic()
        snapshot = self.load_recipe_snapshot()
        today_key = self._today_key()
        recipes_by_date = snapshot.setdefault("recipes_by_date", {})
        if not force and isinstance(recipes_by_date.get(today_key), dict):
            recipe = copy.deepcopy(recipes_by_date[today_key])
            recipe["reused_existing_snapshot"] = True
            recipe.setdefault("date", today_key)
            recipe.setdefault("generated_at_display", self.format_timestamp_label(recipe.get("generated_at", ""), default=""))
            self.app_logger.info(
                "reading_recipe reuse elapsed_ms=%.1f date=%s selected=%s path=%s",
                (self.monotonic() - started_at) * 1000,
                today_key,
                len(recipe.get("selected_articles", []) or []),
                self.reading_recipe_of_day_path,
            )
            return recipe

        data = self.load_reading_data_cached()
        if not isinstance(data, dict):
            data = self.default_reading_data()
        projection = self.reading_runtime_projection_service.build_projection(data, context_label="recipe")
        sources = [dict(source) for source in projection.sources]
        entries = [dict(entry) for entry in projection.lightweight_entries]
        sources_by_id = {str(source.get("id", "") or "").strip(): source for source in sources if str(source.get("id", "") or "").strip()}
        sources_by_name = {str(source.get("name", "") or "").strip().lower(): source for source in sources if str(source.get("name", "") or "").strip()}
        selection = self._select_candidates(entries, sources_by_id, sources_by_name)
        generated_at = self._now().isoformat(timespec="seconds")
        recipe = {
            "version": self.RECIPE_VERSION,
            "date": today_key,
            "generated_at": generated_at,
            "generated_at_display": self.format_timestamp_label(generated_at, default=""),
            "recipe_snapshot_path": str(self.reading_recipe_of_day_path),
            "candidate_count": int(selection["candidate_count"]),
            "active_candidate_count": int(selection["active_candidate_count"]),
            "recent_active_count": int(selection["recent_active_count"]),
            "selected_count": len(selection["selected_articles"]),
            "reused_existing_snapshot": False,
            "selected_articles": selection["selected_articles"],
            "source_count": len(sources),
            "source_names": [str(source.get("name", "") or "").strip() for source in sources if str(source.get("name", "") or "").strip()],
            "generated_from": "reading_data.json",
        }
        recipes_by_date[today_key] = recipe
        snapshot["version"] = self.RECIPE_VERSION
        snapshot["latest_recipe_date"] = today_key
        snapshot["recipes_by_date"] = recipes_by_date
        self.save_recipe_snapshot(snapshot)
        self.app_logger.info(
            "reading_recipe build elapsed_ms=%.1f date=%s candidates=%s active_candidates=%s recent_active=%s selected=%s path=%s force=%s",
            (self.monotonic() - started_at) * 1000,
            today_key,
            recipe["candidate_count"],
            recipe["active_candidate_count"],
            recipe["recent_active_count"],
            recipe["selected_count"],
            self.reading_recipe_of_day_path,
            bool(force),
        )
        return copy.deepcopy(recipe)
