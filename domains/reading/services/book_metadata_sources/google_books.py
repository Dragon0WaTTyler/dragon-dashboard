import re
import urllib.parse


class GoogleBooksMetadataSource:
    source_name = "google_books"

    def __init__(self, *, requests_module, timeout_seconds=6):
        self.requests_module = requests_module
        self.timeout_seconds = timeout_seconds

    def search(self, *, title="", author=""):
        query_terms = []
        if str(title or "").strip():
            query_terms.append(f"intitle:{str(title).strip()}")
        if str(author or "").strip():
            query_terms.append(f"inauthor:{str(author).strip()}")
        if not query_terms:
            return []
        query = " ".join(query_terms)
        url = f"https://www.googleapis.com/books/v1/volumes?q={urllib.parse.quote(query)}&maxResults=5"
        response = self.requests_module.get(url, timeout=self.timeout_seconds)
        response.raise_for_status()
        payload = response.json() or {}
        candidates = []
        for item in payload.get("items", []) or []:
            candidate = self.parse_candidate(item)
            if candidate:
                candidates.append(candidate)
        return candidates

    def parse_candidate(self, item):
        if not isinstance(item, dict):
            return {}
        volume_info = item.get("volumeInfo", {}) or {}
        title = str(volume_info.get("title") or "").strip()
        if not title:
            return {}
        authors = [str(author or "").strip() for author in (volume_info.get("authors", []) or []) if str(author or "").strip()]
        identifiers = volume_info.get("industryIdentifiers", []) or []
        isbn_value = self._preferred_identifier(identifiers)
        image_links = volume_info.get("imageLinks", {}) or {}
        published_year = self._published_year(volume_info.get("publishedDate"))
        return {
            "source": self.source_name,
            "external_id": str(item.get("id") or "").strip(),
            "title": title,
            "authors": authors,
            "published_year": published_year,
            "isbn": isbn_value,
            "cover": str(image_links.get("thumbnail") or image_links.get("smallThumbnail") or "").strip(),
            "description": str(volume_info.get("description") or "").strip(),
            "subjects": [str(subject or "").strip() for subject in (volume_info.get("categories", []) or []) if str(subject or "").strip()][:10],
            "raw": {
                "page_count": int(volume_info.get("pageCount", 0) or 0),
                "language": str(volume_info.get("language") or "").strip(),
            },
        }

    def _preferred_identifier(self, identifiers):
        values = []
        for item in identifiers:
            if not isinstance(item, dict):
                continue
            identifier = str(item.get("identifier") or "").strip()
            if identifier:
                values.append(identifier)
        isbn13 = next((item for item in values if re.fullmatch(r"\d{13}", item)), "")
        if isbn13:
            return isbn13
        isbn10 = next((item for item in values if re.fullmatch(r"[\dXx]{10}", item)), "")
        if isbn10:
            return isbn10.upper()
        return values[0] if values else ""

    def _published_year(self, value):
        text = str(value or "").strip()
        match = re.search(r"\d{4}", text)
        return match.group(0) if match else ""
