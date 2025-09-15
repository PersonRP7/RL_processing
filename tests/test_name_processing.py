"""
tests.test_name_processing

Unit tests for the NameProcessingService class in services.name_processing.

Tests include:
- Happy path: proper merging of full names.
- Handling of unpaired entries.
- Detection and raising of InvalidInputError for malformed JSON.
"""

import json
import tempfile
import unittest
from typing import Dict
from services.name_processing import NameProcessingService, InvalidInputError


def write_temp_json(data: Dict) -> str:
    """
    Helper to write a dictionary to a temporary JSON file.

    Args:
        data (Dict): JSON-serializable data to write.

    Returns:
        str: Path to the temporary JSON file.
    """
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
    with open(tmp.name, "w", encoding="utf-8") as f:
        json.dump(data, f)
    return tmp.name


class TestNameProcessingService(unittest.TestCase):
    """
    Unit tests for the NameProcessingService.

    Tests the streaming NDJSON conversion, sorting, merging of full and unpaired
    names, and error handling for invalid JSON input.
    """

    def setUp(self) -> None:
        """
        Set up a temporary directory and service instance for each test.
        """
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.service = NameProcessingService(base_tmp=self.tmp_dir.name)

    def tearDown(self):
        """
        Clean up the temporary directory after each test.
        """
        self.tmp_dir.cleanup()

    def test_happy_path_full_names(self) -> None:
        """
        Test normal behavior: input JSON with matching first and last names
        should produce correctly merged 'full_names' in the output.
        """
        data = {
            "first_names": [["Alice", 1], ["Bob", 2]],
            "last_names": [["Smith", 1], ["Jones", 2]],
        }
        path = write_temp_json(data)

        output = b"".join(self.service.convert_to_ndjson_stream(path))
        text = output.decode("utf-8")

        self.assertIn('"full_names": [', text)
        self.assertIn('{"first": "Alice", "last": "Smith", "id": 1}', text)
        self.assertIn('{"first": "Bob", "last": "Jones", "id": 2}', text)
        self.assertIn('"unpaired": [', text)
        self.assertIn("Pipeline finished", text)

    def test_unpaired_entries(self):
        """
        Test behavior when some first or last names have no matching pair.
        Unpaired entries should appear in the 'unpaired' array.
        """
        data = {
            "first_names": [["Alice", 1], ["Bob", 3]],
            "last_names": [["Smith", 2], ["Jones", 4]],
        }
        path = write_temp_json(data)

        output = b"".join(self.service.convert_to_ndjson_stream(path))
        text = output.decode("utf-8")

        self.assertIn('"full_names": [', text)
        self.assertIn('"unpaired": [', text)
        self.assertIn('{"first": "Alice", "id": 1}', text)
        self.assertIn('{"last": "Smith", "id": 2}', text)

    def test_invalid_json_raises_error(self):
        """
        Test that malformed JSON input raises an InvalidInputError.
        """
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
        with open(tmp.name, "w", encoding="utf-8") as f:
            # Malformed JSON
            f.write('{"first_names": [["Alice", 1]], "last_names": [')

        with self.assertRaises(InvalidInputError):
            list(self.service.convert_to_ndjson_stream(tmp.name))


if __name__ == "__main__":
    unittest.main()