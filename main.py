"""Application main entrypoint

Contains the routes and their controllers.
"""

import logging
from fastapi import FastAPI, Request, Depends, Response
from fastapi.responses import StreamingResponse
from services.name_processing import NameProcessingService, InvalidInputError
from logging_utils.config import setup_logging
from utils.io_utils import save_request_to_tempfile, TempfileSaveError
from utils.generator_utils import sync_gen_to_async_gen, stream_sync_service
from anyio import to_thread

logger = setup_logging()

logger = logging.getLogger(__name__)
app = FastAPI()

def get_name_service() -> NameProcessingService:
    """
    Dependency provider for the NameProcessingService.

    Returns:
        NameProcessingService: Instance of the name processing service.
    """
    return NameProcessingService()


@app.post("/combine-names")
async def combine_names(
    request: Request,
    service: NameProcessingService = Depends(get_name_service),
)-> Response:
    """
    Stream request data to a tempfile.
    Start the name processing service.

    Args:
        request (Request): Incoming FastAPI request with JSON payload.
        service (NameProcessingService): Service instance for name processing.

    Returns:
        StreamingResponse | Response: A streaming NDJSON response on success,
        or a plain Response with an error message on failure.
    """
    # Step 1: Save the incoming body to a tempfile
    try:
        temp_path = await save_request_to_tempfile(request)
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
            exc_info=True
            )
        return Response(content=e.message, status_code=500)

    # Step 2: Process JSON in an async generator
    return await stream_sync_service(service.convert_to_ndjson_stream, temp_path)