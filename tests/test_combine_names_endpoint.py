"""
Unit tests for the /combine-names endpoint in main.py.

Covers:
- Successful streaming response
- TempfileSaveError handling (500 response)
- InvalidInputError handling (400 response)
"""

import unittest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock

from main import app
from utils.io_utils import TempfileSaveError
from services.name_processing import InvalidInputError


class TestCombineNamesEndpoint(unittest.TestCase):
    """
    Unit tests for the combine_names controller.
    """

    def setUp(self) -> None:
        """
        Create a test client for the FastAPI app.
        """
        self.client = TestClient(app)

    @patch("main.save_request_to_tempfile")
    @patch("main.NameProcessingService")
    def test_successful_request_returns_streaming_response(
        self, mock_service_cls: MagicMock, mock_save: MagicMock
    ) -> None:
        """
        Test that a valid request returns a StreamingResponse with NDJSON chunks.
        """
        mock_save.return_value = "/tmp/fakefile.json"

        # Mock service generator
        def fake_gen(_):
            yield b'{"combined":"John Doe"}\n'

        mock_service = mock_service_cls.return_value
        mock_service.convert_to_ndjson_stream.side_effect = lambda path: fake_gen(path)

        response = self.client.post("/combine-names", data=b'{"first":"John","last":"Doe"}')

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["content-type"], "application/x-ndjson")
        self.assertIn("John Doe", response.text)

    @patch("main.save_request_to_tempfile")
    def test_tempfile_save_error_returns_500(self, mock_save: MagicMock) -> None:
        """
        Test that TempfileSaveError during request save returns a 500 response.
        """
        mock_save.side_effect = TempfileSaveError("failed to save")

        response = self.client.post("/combine-names", data=b'invalid')

        self.assertEqual(response.status_code, 500)
        self.assertIn("failed to save", response.text)

    @patch("main.save_request_to_tempfile")
    @patch("main.NameProcessingService")
    def test_invalid_input_error_returns_400(
        self, mock_service_cls: MagicMock, mock_save: MagicMock
    ) -> None:
        """
        Test that InvalidInputError from the service returns a 400 response.
        """
        mock_save.return_value = "/tmp/fakefile.json"

        mock_service = mock_service_cls.return_value
        mock_service.convert_to_ndjson_stream.side_effect = InvalidInputError(
            message="bad input",
            raw_error="details"
        )

        response = self.client.post("/combine-names", data=b'invalid json')

        self.assertEqual(response.status_code, 400)
        self.assertIn("bad input", response.text)


if __name__ == "__main__":
    unittest.main()
