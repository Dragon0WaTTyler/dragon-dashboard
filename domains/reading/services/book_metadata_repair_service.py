import difflib
import re
import unicodedata


class BookMetadataRepairService:
    def __init__(
        self,
        *,
        fetch_books_entries,
        open_library_source,
        google_books_source,
        default_limit=10,
        max_limit=25,
    ):
        self.fetch_books_entries = fetch_books_entries
        self.open_library_source = open_library_source
        self.google_books_source = google_books_source
        self.default_limit = max(1, int(default_limit or 10))
        self.max_limit = max(self.default_limit, int(max_limit or self.default_limit))

    def preview(self, payload=None):
        request_payload = payload if isinstance(payload, dict) else {}
        fetched = self.fetch_books_entries()
        entries = [dict(entry) for entry in (fetched.get("entries", []) or []) if isinstance(entry, dict)]
        warnings = []
        books_error = str(fetched.get("error") or "").strip()
        if books_error:
            warnings.append(f"Books source warning: {books_error}")

        selected_entries = self._select_entries(entries, request_payload, warnings)
        items = [self._preview_entry(entry) for entry in selected_entries]
        return {
            "ok": True,
            "dry_run": True,
            "count": len(items),
            "items": items,
            "warnings": warnings,
        }

    def _select_entries(self, entries, payload, warnings):
        entry_ids = payload.get("entry_ids", [])
        limit = self._coerce_limit(payload.get("limit"))
        if isinstance(entry_ids, list) and entry_ids:
            wanted = {str(item or "").strip() for item in entry_ids if str(item or "").strip()}
            selected = [entry for entry in entries if str(entry.get("id") or "").strip() in wanted]
            missing_ids = sorted(wanted - {str(entry.get("id") or "").strip() for entry in selected})
            if missing_ids:
                warnings.append(f"Some requested books were not found: {', '.join(missing_ids[:5])}")
            return selected[:limit]
        if bool(payload.get("all")):
            return list(entries[:limit])
        warnings.append("No books selected. Provide entry_ids or set all=true.")
        return []

    def _coerce_limit(self, value):
        try:
            coerced = int(value or self.default_limit)
        except (TypeError, ValueError):
            coerced = self.default_limit
        return max(1, min(coerced, self.max_limit))

    def _preview_entry(self, entry):
        current = self._current_payload(entry)
        warnings = []
        source_warnings = []

        open_library_candidates = self._safe_search(self.open_library_source, entry, source_warnings)
        open_library_best = self._best_candidate(entry, open_library_candidates)
        open_library_confidence = str(open_library_best.get("confidence") or "no_match").strip()

        if open_library_confidence in {"high", "medium"}:
            best_match = open_library_best
            if best_match.get("source") == self.open_library_source.source_name and not best_match.get("description"):
                best_match = self._maybe_enrich_open_library(best_match)
                best_match = self._score_candidate(entry, best_match)
        else:
            google_books_candidates = self._safe_search(self.google_books_source, entry, source_warnings)
            combined_candidates = list(open_library_candidates) + list(google_books_candidates)
            if combined_candidates:
                best_match = self._best_candidate(entry, combined_candidates)
                if best_match.get("source") == self.open_library_source.source_name and not best_match.get("description"):
                    best_match = self._maybe_enrich_open_library(best_match)
                    best_match = self._score_candidate(entry, best_match)
            else:
                best_match = self._empty_match()

        warnings.extend(source_warnings)
        warnings.extend(self._mismatch_warnings(entry, best_match))
        proposed_changes = self._proposed_changes(entry, best_match)
        return {
            "entry_id": str(entry.get("id") or "").strip(),
            "current": current,
            "best_match": self._best_match_payload(best_match),
            "proposed_changes": proposed_changes,
            "warnings": warnings,
        }

    def _maybe_enrich_open_library(self, candidate):
        if str((candidate or {}).get("source") or "").strip() != self.open_library_source.source_name:
            return candidate
        try:
            return self.open_library_source.enrich_candidate(candidate)
        except Exception:
            return candidate

    def _safe_search(self, source, entry, warnings):
        current = self._current_payload(entry)
        try:
            source_candidates = source.search(
                title=current["title"],
                author=current["author"],
            )
        except Exception as exc:
            warnings.append(f"{source.source_name} lookup failed: {type(exc).__name__}")
            return []
        candidates = []
        for candidate in source_candidates:
            normalized_candidate = self._score_candidate(entry, dict(candidate or {}))
            candidates.append(normalized_candidate)
        return candidates

    def _best_candidate(self, entry, candidates):
        if not candidates:
            return self._empty_match()
        return max(
            candidates,
            key=lambda item: float(item.get("score", 0.0) or 0.0),
        )

    def _current_payload(self, entry):
        return {
            "title": str(entry.get("title") or "").strip(),
            "author": str(entry.get("authors_display") or "").strip(),
            "published_year": "",
            "isbn": "",
            "cover": str(entry.get("cover_url") or "").strip(),
            "description": "",
            "subjects": [],
        }

    def _empty_match(self):
        return {
            "source": "none",
            "external_id": "",
            "title": "",
            "authors": [],
            "published_year": "",
            "isbn": "",
            "cover": "",
            "description": "",
            "subjects": [],
            "confidence": "no_match",
            "score": 0.0,
            "reasons": ["No suitable external metadata match found."],
        }

    def _score_candidate(self, entry, candidate):
        title_ratio = self._string_similarity(entry.get("title", ""), candidate.get("title", ""))
        author_ratio = self._author_similarity(entry.get("authors", []) or entry.get("authors_display", ""), candidate.get("authors", []))
        reasons = []
        score = 0.0

        current_isbn = ""
        candidate_isbn = str(candidate.get("isbn") or "").strip()
        if current_isbn and candidate_isbn and self._normalize_isbn(current_isbn) == self._normalize_isbn(candidate_isbn):
            score = 1.0
            reasons.append("Exact ISBN match.")
        else:
            score = (title_ratio * 0.7) + (author_ratio * 0.3)
            if title_ratio >= 0.95:
                reasons.append("Strong title match.")
            elif title_ratio >= 0.82:
                reasons.append("Close title match.")
            elif title_ratio >= 0.68:
                reasons.append("Loose title match.")
            if author_ratio >= 0.9:
                reasons.append("Strong author match.")
            elif author_ratio >= 0.55:
                reasons.append("Partial author match.")
            if candidate.get("cover"):
                score += 0.02
                reasons.append("Candidate includes a cover.")

        confidence = "no_match"
        if current_isbn and candidate_isbn and self._normalize_isbn(current_isbn) == self._normalize_isbn(candidate_isbn):
            confidence = "high"
        elif title_ratio >= 0.9 and author_ratio >= 0.85:
            confidence = "high"
        elif title_ratio >= 0.88 and author_ratio >= 0.45:
            confidence = "medium"
        elif title_ratio >= 0.72:
            confidence = "low"

        payload = dict(candidate or {})
        payload["confidence"] = confidence
        payload["score"] = round(score, 4) if confidence != "no_match" else 0.0
        payload["reasons"] = reasons or ["Candidate did not clear the match threshold."]
        return payload

    def _mismatch_warnings(self, entry, best_match):
        warnings = []
        if str(best_match.get("confidence") or "").strip() == "no_match":
            return warnings
        candidate_title = str(best_match.get("title") or "").strip()
        current_title = str(entry.get("title") or "").strip()
        candidate_authors = [str(item or "").strip() for item in (best_match.get("authors", []) or []) if str(item or "").strip()]
        current_authors = [str(item or "").strip() for item in (entry.get("authors", []) or []) if str(item or "").strip()]
        title_ratio = self._string_similarity(current_title, candidate_title)
        author_ratio = self._author_similarity(current_authors, candidate_authors)
        if candidate_title and title_ratio < 0.9:
            warnings.append("Title differs from the current Notion value. V0 will not propose a title change.")
        if candidate_authors and author_ratio < 0.9:
            warnings.append("Author differs from the current Notion value. V0 will not propose an author change.")
        return warnings

    def _proposed_changes(self, entry, best_match):
        if str(best_match.get("confidence") or "").strip() == "no_match":
            return []
        current = self._current_payload(entry)
        fields = [
            ("cover", "cover"),
            ("published_year", "published_year"),
            ("isbn", "isbn"),
            ("description", "description"),
            ("subjects", "subjects"),
        ]
        changes = []
        for current_key, candidate_key in fields:
            current_value = current.get(current_key)
            suggested_value = best_match.get(candidate_key)
            if current_key == "subjects":
                current_value = list(current_value or [])
                suggested_value = [str(item or "").strip() for item in (suggested_value or []) if str(item or "").strip()]
                if not suggested_value or current_value == suggested_value:
                    continue
            else:
                current_value = str(current_value or "").strip()
                suggested_value = str(suggested_value or "").strip()
                if not suggested_value or current_value == suggested_value:
                    continue
                if current_key == "cover" and current_value and not self._cover_is_temporary(current_value):
                    continue
            changes.append(
                {
                    "field": current_key,
                    "current": current_value,
                    "suggested": suggested_value,
                    "source": str(best_match.get("source") or "none"),
                    "confidence": str(best_match.get("confidence") or "no_match"),
                }
            )
        return changes

    def _cover_is_temporary(self, url):
        text = str(url or "").strip().lower()
        if not text:
            return False
        markers = (
            "x-amz-expires",
            "x-amz-signature",
            "x-amz-credential",
            "x-amz-security-token",
            "x-amz-date",
            "prod-files-secure",
            "amazonaws.com",
            "notion.com",
            "notion-static.com",
        )
        return any(marker in text for marker in markers)

    def _best_match_payload(self, candidate):
        payload = dict(candidate or {})
        return {
            "source": str(payload.get("source") or "none"),
            "external_id": str(payload.get("external_id") or "").strip(),
            "title": str(payload.get("title") or "").strip(),
            "authors": [str(item or "").strip() for item in (payload.get("authors", []) or []) if str(item or "").strip()],
            "published_year": str(payload.get("published_year") or "").strip(),
            "isbn": str(payload.get("isbn") or "").strip(),
            "cover": str(payload.get("cover") or "").strip(),
            "description": str(payload.get("description") or "").strip(),
            "subjects": [str(item or "").strip() for item in (payload.get("subjects", []) or []) if str(item or "").strip()],
            "confidence": str(payload.get("confidence") or "no_match"),
            "score": float(payload.get("score", 0.0) or 0.0),
            "reasons": [str(item or "").strip() for item in (payload.get("reasons", []) or []) if str(item or "").strip()],
        }

    def _string_similarity(self, left, right):
        left_normalized = self._normalize_text(left)
        right_normalized = self._normalize_text(right)
        if not left_normalized or not right_normalized:
            return 0.0
        if left_normalized == right_normalized:
            return 1.0
        return difflib.SequenceMatcher(None, left_normalized, right_normalized).ratio()

    def _author_similarity(self, current_authors, candidate_authors):
        current_values = self._author_list(current_authors)
        candidate_values = self._author_list(candidate_authors)
        if not current_values or not candidate_values:
            return 0.0
        best = 0.0
        for current_author in current_values:
            for candidate_author in candidate_values:
                ratio = self._string_similarity(current_author, candidate_author)
                if ratio > best:
                    best = ratio
        current_tokens = {token for author in current_values for token in self._normalize_text(author).split(" ") if token}
        candidate_tokens = {token for author in candidate_values for token in self._normalize_text(author).split(" ") if token}
        if current_tokens and candidate_tokens:
            overlap = len(current_tokens & candidate_tokens) / max(1, len(current_tokens | candidate_tokens))
            best = max(best, overlap)
        return best

    def _author_list(self, value):
        if isinstance(value, (list, tuple)):
            return [str(item or "").strip() for item in value if str(item or "").strip()]
        text = str(value or "").strip()
        if not text:
            return []
        return [part.strip() for part in re.split(r"\s*(?:,|;|/|&|\band\b)\s*", text, flags=re.IGNORECASE) if part.strip()]

    def _normalize_text(self, value):
        text = unicodedata.normalize("NFKD", str(value or ""))
        text = "".join(ch for ch in text if not unicodedata.combining(ch))
        text = re.sub(r"[\u064b-\u065f\u0670\u06d6-\u06ed]", "", text)
        text = text.lower()
        text = re.sub(r"[^\w\s\u0600-\u06ff]", " ", text, flags=re.UNICODE)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def _normalize_isbn(self, value):
        return re.sub(r"[^0-9Xx]", "", str(value or "")).upper()
