import unittest
from pathlib import Path


class RefreshFoundationCheckpointTests(unittest.TestCase):
    def test_checkpoint_doc_exists_and_marks_the_current_contract(self):
        doc_path = Path(__file__).resolve().parents[1] / "docs" / "REFRESH_FOUNDATION_CHECKPOINT.md"
        content = doc_path.read_text(encoding="utf-8")

        self.assertTrue(doc_path.exists())
        self.assertIn("domains/shared/refresh", content)
        self.assertIn("Articles/Reading", content)
        self.assertIn("YouTube/PocketTube", content)
        self.assertIn("refresh_status", content)
        self.assertIn("Pull Latest Articles", content)
        self.assertIn("Opening the PocketTube page must not trigger sync", content)
        self.assertIn("Refresh describes state", content)


if __name__ == "__main__":
    unittest.main()
