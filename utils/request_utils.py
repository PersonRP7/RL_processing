from fastapi import Request, Response
from services.name_processing import InvalidInputError
from logging import Logger
from utils.io_utils import TempfileSaveError, save_request_to_tempfile

async def save_request_with_handling(
    request: Request,
    logger: Logger,
) -> str | Response:
    """
    Save a streaming request body to a temporary file with built-in error handling.

    This wraps `save_request_to_tempfile`, catching validation and I/O errors,
    and returning an HTTP Response if an error occurs.

    Args:
        request (Request): The incoming FastAPI request object.
        logger (logging.Logger): A configured logger instance to use for logging
            validation or I/O errors.

    Returns:
        str | Response:
            - On success: The path to the saved tempfile.
            - On failure: A `Response` object with an appropriate error status code
              and message, ready to return from the controller.
    """
    try:
        return await save_request_to_tempfile(request)
    except InvalidInputError as e:
        logger.error(
            "Invalid input while saving request body: %s",
            e.raw_error or e.message,
            exc_info=True,
        )
        return Response(content=e.message, status_code=e.status_code)
    except TempfileSaveError as e:
        logger.error(
            "Internal error while saving request body: %s",
            e.original_exception,
            exc_info=True,
        )
        return Response(content=e.message, status_code=500)