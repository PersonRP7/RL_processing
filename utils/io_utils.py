"""IO utils

Custom error type and a utility mechanism for storing
streamed request data to a tempfile
"""

import tempfile
import ijson
from fastapi import Request
from services.name_processing import InvalidInputError
from validators.input_validator import streaming_validator

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
    try:
        temp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
        validator = streaming_validator()

        try:
            async for chunk in request.stream():
                temp.write(chunk)
            temp.flush()

            # Re-open the file for validation (streaming validation)
            with open(temp.name, "rb") as f:
                validator.validate(f)

        finally:
            temp.close()

        return temp.name

    except InvalidInputError:
        raise  # propagate 400-level validation error
    except Exception as e:
        raise TempfileSaveError("Failed to save request body to temporary file", e)
