import unittest
from pathlib import Path


class CanonicalSnapshotSyncV0Tests(unittest.TestCase):
    def test_document_exists_and_mentions_contract_terms(self):
        doc_path = Path("docs/CANONICAL_SNAPSHOT_SYNC_V0.md").resolve()
        text = doc_path.read_text(encoding="utf-8")

        required_terms = [
            "canonical synced state",
            "local runtime state",
            "reading_data.json",
            "youtube_latest_snapshot.json",
            ".env",
            "youtube_token.json",
        ]

        self.assertTrue(doc_path.exists())
        lowered = text.lower()
        for term in required_terms:
            self.assertIn(term, lowered)


if __name__ == "__main__":
    unittest.main()
