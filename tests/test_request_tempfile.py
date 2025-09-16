"""
Unit tests for the io_utils module.

This test suite validates the behavior of:
- save_request_to_tempfile: ensuring it properly writes streamed request data to a tempfile.
- TempfileSaveError: ensuring it is raised on underlying IO errors.

The tests cover:
1. Successful saving of streamed request data to a temporary file.
2. Raising of TempfileSaveError when an exception occurs during file write.
"""

import os
import tempfile
import unittest
import asyncio

from utils.io_utils import save_request_to_tempfile, TempfileSaveError


class MockRequest:
    """
    Mock implementation of FastAPI's Request for testing.

    Attributes:
        chunks (list[bytes]): A list of byte chunks to simulate request streaming.
    """

    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = chunks

    async def stream(self):
        """
        Simulate an async stream of request body chunks.

        Yields:
            bytes: Chunk of request body.
        """
        for chunk in self._chunks:
            yield chunk


class TestSaveRequestToTempfile(unittest.TestCase):
    """
    Test suite for the save_request_to_tempfile utility function.
    """

    def test_save_request_success(self) -> None:
        """
        Test that request body is successfully saved to a temporary file.

        Asserts:
            - The returned file path exists.
            - The file contents match the streamed request body.
        """
        mock_request = MockRequest([b'{"key": "value"}'])
        path = asyncio.run(save_request_to_tempfile(mock_request))

        try:
            self.assertTrue(os.path.exists(path))
            with open(path, "rb") as f:
                content = f.read()
            self.assertEqual(content, b'{"key": "value"}')
        finally:
            os.remove(path)

    def test_save_request_failure_raises_tempfile_save_error(self) -> None:
        """
        Test that TempfileSaveError is raised if writing fails.

        Simulates failure by patching tempfile.NamedTemporaryFile
        to raise an exception immediately.
        """
        # Simulate signature compatibility
        def bad_tempfile(*args, **kwargs):
            raise IOError("disk full")

        mock_request = MockRequest([b"some data"])

        # Patch NamedTemporaryFile temporarily
        original_tempfile = tempfile.NamedTemporaryFile
        tempfile.NamedTemporaryFile = bad_tempfile

        try:
            with self.assertRaises(TempfileSaveError) as cm:
                asyncio.run(save_request_to_tempfile(mock_request))

            self.assertIn("Failed to save request body", str(cm.exception))
            self.assertIsInstance(cm.exception.original_exception, IOError)
        finally:
            tempfile.NamedTemporaryFile = original_tempfile

    def test_save_request_failure_write_error(self) -> None:
        """
        Test that TempfileSaveError is raised if writing to the temp file fails mid-stream.

        Uses a custom temp file object that raises an exception on write.
        """
        class BadTempFile:
            # Simulate signature compatibility
            def __init__(self, *args, **kwargs):
                self.closed = False
            def write(self, chunk):
                raise IOError("write error")
            def close(self):
                self.closed = True

        mock_request = MockRequest([b"chunk1", b"chunk2"])

        original_tempfile = tempfile.NamedTemporaryFile
        tempfile.NamedTemporaryFile = lambda *a, **kw: BadTempFile()

        try:
            with self.assertRaises(TempfileSaveError) as cm:
                asyncio.run(save_request_to_tempfile(mock_request))

            self.assertIn("Failed to save request body", str(cm.exception))
            self.assertIsInstance(cm.exception.original_exception, IOError)
        finally:
            tempfile.NamedTemporaryFile = original_tempfile


if __name__ == "__main__":
    unittest.main()
