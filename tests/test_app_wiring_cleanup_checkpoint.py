import unittest
from pathlib import Path


class AppWiringCleanupCheckpointTests(unittest.TestCase):
    def test_checkpoint_document_exists_and_mentions_key_terms(self):
        doc_path = Path("docs/APP_WIRING_CLEANUP_V0.md")
        self.assertTrue(doc_path.exists(), f"Missing checkpoint document: {doc_path}")

        content = doc_path.read_text(encoding="utf-8")

        expected_phrases = [
            "dragon/wiring.py",
            "app.py remains the Flask route/orchestration layer",
            "lazy singleton wrappers",
            "constructor wiring",
            "not route extraction",
            "runtime behavior must remain unchanged",
        ]

        for phrase in expected_phrases:
            self.assertIn(phrase, content)


if __name__ == "__main__":
    unittest.main()
