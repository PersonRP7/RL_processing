"""IO utils

Custom error type and a utility mechanism for storing
streamed request data to a tempfile
"""

import tempfile
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
    """
    Save the body of a streaming FastAPI request to a temporary file
    while simultaneously performing incremental JSON validation.

    This function is designed for large JSON payloads, allowing the request
    body to be validated in a streaming fashion without loading the entire
    content into memory. Each chunk of the request body is written to a
    temporary file and fed into a streaming validator, which checks for
    structural correctness and ensures that required top-level keys
    ('first_names' or 'last_names') and their items conform to the expected format.

    Steps performed:
        1. Create a temporary file with a specified suffix.
        2. Instantiate a streaming JSON validator.
        3. Iterate asynchronously over the incoming request chunks:
            - Write each chunk to the temporary file.
            - Feed each chunk to the streaming validator.
        4. After all chunks are received:
            - Flush remaining data to disk.
            - Call `validator.finish()` to validate any remaining partial data.
        5. Close the temporary file and return its path.

    Args:
        request (Request): The incoming FastAPI request object containing
                           a streaming JSON payload.
        suffix (str, optional): File suffix for the temporary file (default ".json").

    Returns:
        str: Path to the temporary file containing the request body.

    Raises:
        InvalidInputError: Raised if the JSON payload is malformed or
                           violates validation rules (e.g., missing required
                           top-level keys, invalid array items). This should
                           result in an HTTP 400 response at the endpoint.
        TempfileSaveError: Raised if there is any other failure during
                           writing to the temporary file (e.g., I/O errors).
    """
    try:
        temp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
        validator = streaming_validator()

        try:
            async for chunk in request.stream():
                temp.write(chunk)
                validator.feed(chunk)
            temp.flush()
            validator.finish()  # Ensure remaining data is valid
        finally:
            temp.close()

        return temp.name

    except InvalidInputError:
        raise  # propagate 400-level validation error
    except Exception as e:
        raise TempfileSaveError("Failed to save request body to temporary file", e)