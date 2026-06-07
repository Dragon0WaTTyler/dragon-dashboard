import re
import urllib.parse


class OpenLibraryMetadataSource:
    source_name = "open_library"

    def __init__(self, *, requests_module, timeout_seconds=6):
        self.requests_module = requests_module
        self.timeout_seconds = timeout_seconds

    def search(self, *, title="", author=""):
        query_parts = []
        if str(title or "").strip():
            query_parts.append(f"title={urllib.parse.quote(str(title).strip())}")
        if str(author or "").strip():
            query_parts.append(f"author={urllib.parse.quote(str(author).strip())}")
        if not query_parts:
            return []
        url = "https://openlibrary.org/search.json?" + "&".join(query_parts) + "&limit=5"
        response = self.requests_module.get(url, timeout=self.timeout_seconds)
        response.raise_for_status()
        payload = response.json() or {}
        candidates = []
        for doc in payload.get("docs", []) or []:
            candidate = self.parse_candidate(doc)
            if candidate:
                candidates.append(candidate)
        return candidates

    def enrich_candidate(self, candidate):
        payload = dict(candidate or {})
        work_key = str(payload.get("work_key") or "").strip()
        if not work_key:
            return payload
        try:
            response = self.requests_module.get(
                f"https://openlibrary.org{work_key}.json",
                timeout=self.timeout_seconds,
            )
            response.raise_for_status()
            work_payload = response.json() or {}
        except Exception:
            return payload
        description = self._description_from_payload(work_payload)
        subjects = self._subjects_from_payload(work_payload)
        if description and not payload.get("description"):
            payload["description"] = description
        if subjects and not payload.get("subjects"):
            payload["subjects"] = subjects
        return payload

    def parse_candidate(self, doc):
        if not isinstance(doc, dict):
            return {}
        title = str(doc.get("title") or "").strip()
        if not title:
            return {}
        authors = [str(item or "").strip() for item in (doc.get("author_name", []) or []) if str(item or "").strip()]
        published_year = ""
        first_publish_year = doc.get("first_publish_year")
        if first_publish_year not in (None, ""):
            published_year = str(first_publish_year).strip()
        isbn_value = ""
        isbns = [str(item or "").strip() for item in (doc.get("isbn", []) or []) if str(item or "").strip()]
        if isbns:
            isbn_value = self._prefer_isbn(isbns)
        cover_value = ""
        cover_id = doc.get("cover_i")
        if cover_id not in (None, ""):
            cover_value = f"https://covers.openlibrary.org/b/id/{cover_id}-L.jpg"
        elif str(doc.get("cover_edition_key") or "").strip():
            cover_value = (
                "https://covers.openlibrary.org/b/olid/"
                f"{urllib.parse.quote(str(doc.get('cover_edition_key')).strip())}-L.jpg"
            )
        subjects = [str(item or "").strip() for item in (doc.get("subject", []) or []) if str(item or "").strip()]
        subjects = subjects[:10]
        work_key = str(doc.get("key") or "").strip()
        return {
            "source": self.source_name,
            "external_id": self._external_id_from_key(work_key),
            "work_key": work_key,
            "title": title,
            "authors": authors,
            "published_year": published_year,
            "isbn": isbn_value,
            "cover": cover_value,
            "description": "",
            "subjects": subjects,
            "raw": {
                "edition_count": int(doc.get("edition_count", 0) or 0),
                "language": list(doc.get("language", []) or [])[:5],
            },
        }

    def _external_id_from_key(self, value):
        text = str(value or "").strip()
        if not text:
            return ""
        return text.rsplit("/", 1)[-1]

    def _prefer_isbn(self, isbns):
        isbn13 = next((item for item in isbns if re.fullmatch(r"\d{13}", item)), "")
        if isbn13:
            return isbn13
        isbn10 = next((item for item in isbns if re.fullmatch(r"[\dXx]{10}", item)), "")
        if isbn10:
            return isbn10.upper()
        return isbns[0]

    def _description_from_payload(self, payload):
        value = (payload or {}).get("description")
        if isinstance(value, dict):
            return str(value.get("value") or "").strip()
        return str(value or "").strip()

    def _subjects_from_payload(self, payload):
        values = []
        for item in (payload or {}).get("subjects", []) or []:
            text = str(item or "").strip()
            if text:
                values.append(text)
        return values[:10]
