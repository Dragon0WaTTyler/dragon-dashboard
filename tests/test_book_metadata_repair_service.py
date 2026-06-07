import unittest
from unittest.mock import Mock, patch

import app as dragon_app
from domains.reading.services.book_metadata_repair_service import BookMetadataRepairService
from domains.reading.services.book_metadata_sources.google_books import GoogleBooksMetadataSource
from domains.reading.services.book_metadata_sources.open_library import OpenLibraryMetadataSource


class _Response:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class AdapterParsingTests(unittest.TestCase):
    def test_open_library_candidate_parsing(self):
        source = OpenLibraryMetadataSource(requests_module=Mock())
        candidate = source.parse_candidate(
            {
                "key": "/works/OL123W",
                "title": "Le Rouge et le Noir",
                "author_name": ["Stendhal"],
                "first_publish_year": 1830,
                "isbn": ["1234567890", "9781234567897"],
                "cover_i": 42,
                "subject": ["French fiction", "Classics"],
            }
        )

        self.assertEqual(candidate["source"], "open_library")
        self.assertEqual(candidate["external_id"], "OL123W")
        self.assertEqual(candidate["title"], "Le Rouge et le Noir")
        self.assertEqual(candidate["authors"], ["Stendhal"])
        self.assertEqual(candidate["published_year"], "1830")
        self.assertEqual(candidate["isbn"], "9781234567897")
        self.assertIn("/b/id/42-L.jpg", candidate["cover"])
        self.assertEqual(candidate["subjects"], ["French fiction", "Classics"])

    def test_google_books_candidate_parsing(self):
        source = GoogleBooksMetadataSource(requests_module=Mock())
        candidate = source.parse_candidate(
            {
                "id": "g1",
                "volumeInfo": {
                    "title": "The Trial",
                    "authors": ["Franz Kafka"],
                    "publishedDate": "1925-04-26",
                    "industryIdentifiers": [{"identifier": "9780142437651"}],
                    "description": "A novel.",
                    "categories": ["Fiction", "Classics"],
                    "imageLinks": {"thumbnail": "https://example.test/cover.jpg"},
                    "pageCount": 255,
                },
            }
        )

        self.assertEqual(candidate["source"], "google_books")
        self.assertEqual(candidate["external_id"], "g1")
        self.assertEqual(candidate["title"], "The Trial")
        self.assertEqual(candidate["authors"], ["Franz Kafka"])
        self.assertEqual(candidate["published_year"], "1925")
        self.assertEqual(candidate["isbn"], "9780142437651")
        self.assertEqual(candidate["cover"], "https://example.test/cover.jpg")
        self.assertEqual(candidate["description"], "A novel.")
        self.assertEqual(candidate["subjects"], ["Fiction", "Classics"])


class RepairServiceTests(unittest.TestCase):
    def _build_service(self, *, entries, open_candidates=None, google_candidates=None, open_exception=None, google_exception=None):
        open_source = Mock()
        open_source.source_name = "open_library"
        google_source = Mock()
        google_source.source_name = "google_books"
        open_source.search = Mock(return_value=list(open_candidates or []))
        google_source.search = Mock(return_value=list(google_candidates or []))
        open_source.enrich_candidate = Mock(side_effect=lambda candidate: dict(candidate))
        if open_exception is not None:
            open_source.search.side_effect = open_exception
        if google_exception is not None:
            google_source.search.side_effect = google_exception
        return BookMetadataRepairService(
            fetch_books_entries=Mock(return_value={"entries": entries, "error": ""}),
            open_library_source=open_source,
            google_books_source=google_source,
            default_limit=10,
            max_limit=25,
        ), open_source, google_source

    def test_confidence_high_for_strong_title_and_author(self):
        service, _open_source, _google_source = self._build_service(
            entries=[{"id": "b1", "title": "The Trial", "authors": ["Franz Kafka"], "authors_display": "Franz Kafka", "cover_url": ""}],
            open_candidates=[{"source": "open_library", "external_id": "ol1", "title": "The Trial", "authors": ["Franz Kafka"], "published_year": "1925", "isbn": "", "cover": "", "description": "", "subjects": []}],
        )

        result = service.preview({"entry_ids": ["b1"]})

        self.assertEqual(result["items"][0]["best_match"]["confidence"], "high")

    def test_high_confidence_open_library_prevents_google_fallback(self):
        service, open_source, google_source = self._build_service(
            entries=[{"id": "b1", "title": "The Trial", "authors": ["Franz Kafka"], "authors_display": "Franz Kafka", "cover_url": ""}],
            open_candidates=[{"source": "open_library", "external_id": "ol1", "title": "The Trial", "authors": ["Franz Kafka"], "published_year": "1925", "isbn": "", "cover": "", "description": "", "subjects": []}],
            google_candidates=[{"source": "google_books", "external_id": "g1", "title": "The Trial", "authors": ["Franz Kafka"], "published_year": "1925", "isbn": "", "cover": "", "description": "", "subjects": []}],
        )

        service.preview({"entry_ids": ["b1"]})

        open_source.search.assert_called_once()
        google_source.search.assert_not_called()

    def test_confidence_medium_low_and_no_match_cases(self):
        service, open_source, google_source = self._build_service(
            entries=[
                {"id": "m1", "title": "The Trial", "authors": ["Franz Kafka"], "authors_display": "Franz Kafka", "cover_url": ""},
                {"id": "l1", "title": "The Trial", "authors": ["Franz Kafka"], "authors_display": "Franz Kafka", "cover_url": ""},
                {"id": "n1", "title": "The Trial", "authors": ["Franz Kafka"], "authors_display": "Franz Kafka", "cover_url": ""},
            ],
            open_candidates=[{"source": "open_library", "external_id": "ol1", "title": "The Trial Story", "authors": ["Franz Kafka"], "published_year": "", "isbn": "", "cover": "", "description": "", "subjects": []}],
            google_candidates=[{"source": "google_books", "external_id": "g1", "title": "The Trial Story", "authors": ["Franz Kafka"], "published_year": "", "isbn": "", "cover": "", "description": "", "subjects": []}],
        )
        result = service.preview({"entry_ids": ["m1"]})

        self.assertEqual(result["items"][0]["best_match"]["confidence"], "low")
        open_source.search.assert_called_once()
        google_source.search.assert_called_once()

    def test_no_match_open_library_still_runs_google_fallback(self):
        service, open_source, google_source = self._build_service(
            entries=[{"id": "n1", "title": "The Trial", "authors": ["Franz Kafka"], "authors_display": "Franz Kafka", "cover_url": ""}],
            open_candidates=[{"source": "open_library", "external_id": "ol1", "title": "Completely Different", "authors": ["Other"], "published_year": "", "isbn": "", "cover": "", "description": "", "subjects": []}],
            google_candidates=[{"source": "google_books", "external_id": "g1", "title": "The Trial", "authors": ["Franz Kafka"], "published_year": "1925", "isbn": "", "cover": "", "description": "", "subjects": []}],
        )

        result = service.preview({"entry_ids": ["n1"]})

        self.assertNotEqual(result["items"][0]["best_match"]["source"], "none")
        open_source.search.assert_called_once()
        google_source.search.assert_called_once()

        low_score = service._score_candidate(
            {"title": "The Trial", "authors": ["Franz Kafka"]},
            {"source": "google_books", "title": "The Trial Story", "authors": ["Someone Else"], "published_year": "", "isbn": "", "cover": "", "description": "", "subjects": []},
        )
        no_match = service._score_candidate(
            {"title": "The Trial", "authors": ["Franz Kafka"]},
            {"source": "google_books", "title": "Completely Different", "authors": ["Other"], "published_year": "", "isbn": "", "cover": "", "description": "", "subjects": []},
        )

        self.assertEqual(low_score["confidence"], "low")
        self.assertEqual(no_match["confidence"], "no_match")

    def test_preview_returns_safe_proposed_fields_only(self):
        service, _open_source, _google_source = self._build_service(
            entries=[{"id": "b1", "title": "The Trial", "authors": ["Franz Kafka"], "authors_display": "Franz Kafka", "cover_url": ""}],
            open_candidates=[{
                "source": "open_library",
                "external_id": "ol1",
                "title": "The Trial",
                "authors": ["Franz Kafka"],
                "published_year": "1925",
                "isbn": "9780142437651",
                "cover": "https://example.test/cover.jpg",
                "description": "A novel.",
                "subjects": ["Fiction"],
            }],
        )

        result = service.preview({"entry_ids": ["b1"]})
        fields = [item["field"] for item in result["items"][0]["proposed_changes"]]

        self.assertEqual(fields, ["cover", "published_year", "isbn", "description", "subjects"])

    def test_existing_stable_cover_is_not_replaced(self):
        service, _open_source, _google_source = self._build_service(
            entries=[{"id": "b1", "title": "The Trial", "authors": ["Franz Kafka"], "authors_display": "Franz Kafka", "cover_url": "https://example.test/stable-cover.jpg"}],
            open_candidates=[{
                "source": "open_library",
                "external_id": "ol1",
                "title": "The Trial",
                "authors": ["Franz Kafka"],
                "published_year": "1925",
                "isbn": "",
                "cover": "https://covers.openlibrary.org/b/id/42-L.jpg",
                "description": "",
                "subjects": [],
            }],
        )

        result = service.preview({"entry_ids": ["b1"]})
        fields = [item["field"] for item in result["items"][0]["proposed_changes"]]

        self.assertNotIn("cover", fields)

    def test_empty_cover_can_receive_suggested_cover(self):
        service, _open_source, _google_source = self._build_service(
            entries=[{"id": "b1", "title": "The Trial", "authors": ["Franz Kafka"], "authors_display": "Franz Kafka", "cover_url": ""}],
            open_candidates=[{
                "source": "open_library",
                "external_id": "ol1",
                "title": "The Trial",
                "authors": ["Franz Kafka"],
                "published_year": "1925",
                "isbn": "",
                "cover": "https://covers.openlibrary.org/b/id/42-L.jpg",
                "description": "",
                "subjects": [],
            }],
        )

        result = service.preview({"entry_ids": ["b1"]})
        proposed = {item["field"]: item for item in result["items"][0]["proposed_changes"]}

        self.assertIn("cover", proposed)

    def test_temporary_notion_signed_cover_can_receive_suggested_cover(self):
        service, _open_source, _google_source = self._build_service(
            entries=[{"id": "b1", "title": "The Trial", "authors": ["Franz Kafka"], "authors_display": "Franz Kafka", "cover_url": "https://prod-files-secure.s3.amazonaws.com/example.jpg?X-Amz-Expires=3600&X-Amz-Signature=abc"}],
            open_candidates=[{
                "source": "open_library",
                "external_id": "ol1",
                "title": "The Trial",
                "authors": ["Franz Kafka"],
                "published_year": "1925",
                "isbn": "",
                "cover": "https://covers.openlibrary.org/b/id/42-L.jpg",
                "description": "",
                "subjects": [],
            }],
        )

        result = service.preview({"entry_ids": ["b1"]})
        fields = [item["field"] for item in result["items"][0]["proposed_changes"]]

        self.assertIn("cover", fields)

    def test_title_author_mismatches_create_warnings_not_proposed_changes(self):
        service, _open_source, _google_source = self._build_service(
            entries=[{"id": "b1", "title": "The Trial", "authors": ["Franz Kafka"], "authors_display": "Franz Kafka", "cover_url": ""}],
            open_candidates=[{
                "source": "open_library",
                "external_id": "ol1",
                "title": "The Trial Story",
                "authors": ["Max Brod"],
                "published_year": "1926",
                "isbn": "",
                "cover": "",
                "description": "",
                "subjects": [],
            }],
        )

        result = service.preview({"entry_ids": ["b1"]})
        warnings = result["items"][0]["warnings"]
        proposed_fields = [item["field"] for item in result["items"][0]["proposed_changes"]]

        self.assertTrue(any("Title differs" in warning for warning in warnings))
        self.assertTrue(any("Author differs" in warning for warning in warnings))
        self.assertNotIn("title", proposed_fields)
        self.assertNotIn("author", proposed_fields)

    def test_network_failure_returns_safe_warning_and_ok_response(self):
        service, _open_source, _google_source = self._build_service(
            entries=[{"id": "b1", "title": "The Trial", "authors": ["Franz Kafka"], "authors_display": "Franz Kafka", "cover_url": ""}],
            open_exception=RuntimeError("network down"),
            google_exception=RuntimeError("network down"),
        )

        result = service.preview({"entry_ids": ["b1"]})

        self.assertTrue(result["ok"])
        self.assertEqual(result["items"][0]["best_match"]["confidence"], "no_match")
        self.assertTrue(any("lookup failed" in warning for warning in result["items"][0]["warnings"]))


class PreviewRouteTests(unittest.TestCase):
    def setUp(self):
        dragon_app.app.config["TESTING"] = True
        self.client = dragon_app.app.test_client()

    def test_preview_route_does_not_call_notion_write_helpers(self):
        mock_service = Mock()
        mock_service.preview.return_value = {"ok": True, "dry_run": True, "count": 1, "items": [], "warnings": []}
        with patch.object(dragon_app, "_get_book_metadata_repair_service", return_value=mock_service), patch.object(
            dragon_app,
            "update_notion_page_properties",
        ) as update_mock, patch.object(
            dragon_app,
            "create_notion_database_page",
        ) as create_mock:
            response = self.client.post("/api/books/metadata-repair/preview", json={"entry_ids": ["b1"]})

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.get_json()["ok"])
        mock_service.preview.assert_called_once()
        update_mock.assert_not_called()
        create_mock.assert_not_called()


class BooksMetadataRepairAdminPageTests(unittest.TestCase):
    def setUp(self):
        dragon_app.app.config["TESTING"] = True
        self.client = dragon_app.app.test_client()

    def test_admin_page_renders_preview_warning(self):
        response = self.client.get("/books/admin/metadata-repair")

        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn("Books Metadata Repair", html)
        self.assertIn("Preview only — no Notion changes will be made.", html)
        self.assertIn("Preview Books", html)
        self.assertNotIn("Apply", html)
        self.assertNotIn("Write back", html)

    def test_admin_page_posts_preview_results_without_write_helpers(self):
        mock_service = Mock()
        mock_service.preview.return_value = {
            "ok": True,
            "dry_run": True,
            "count": 1,
            "items": [
                {
                    "entry_id": "b1",
                    "current": {"title": "The Trial", "author": "Franz Kafka"},
                    "best_match": {"source": "open_library", "confidence": "high", "score": 0.9876},
                    "proposed_changes": [
                        {"field": "description", "current": "", "suggested": "A novel.", "source": "open_library", "confidence": "high"},
                    ],
                    "warnings": ["Title differs from the current Notion value. V0 will not propose a title change."],
                }
            ],
            "warnings": [],
        }
        with patch.object(dragon_app, "_get_book_metadata_repair_service", return_value=mock_service), patch.object(
            dragon_app,
            "update_notion_page_properties",
        ) as update_mock, patch.object(
            dragon_app,
            "create_notion_database_page",
        ) as create_mock:
            response = self.client.post("/books/admin/metadata-repair", data={"limit": "3"})

        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn("Preview Results", html)
        self.assertIn("The Trial", html)
        self.assertIn("Franz Kafka", html)
        self.assertIn("open_library", html)
        self.assertIn("high", html)
        self.assertNotIn("Apply", html)
        update_mock.assert_not_called()
        create_mock.assert_not_called()
        mock_service.preview.assert_called_once_with({"all": True, "limit": 3})


if __name__ == "__main__":
    unittest.main()
