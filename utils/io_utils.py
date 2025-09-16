"""IO utils

Custom error type and a utility mechanism for storing
streamed request data to a tempfile
"""

import tempfile
from fastapi import Request

class TempfileSaveError(Exception):
    """
    Raised when saving a FastAPI request body to a temporary file fails.

    Attributes:
        message (str): Human-readable error message.
        original_exception (Exception | None): Underlying exception, if any.
    """
    def __init__(self, message: str, original_exception: Exception | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.original_exception = original_exception

async def save_request_to_tempfile(request: Request, suffix: str = ".json") -> str:
    """
    Save the body of a streaming FastAPI request to a temporary file.

    Args:
        request (Request): Incoming FastAPI request with streaming body.
        suffix (str, optional): File suffix (default: ".json").

    Returns:
        str: Path to the saved temporary file on disk.

    Raises:
        TempfileSaveError: If writing to the temporary file fails.
    """
    try:
        temp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
        try:
            async for chunk in request.stream():
                temp.write(chunk)
        finally:
            temp.close()
        return temp.name
    except Exception as e:
        raise TempfileSaveError("Failed to save request body to temporary file", e)
