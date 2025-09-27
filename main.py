"""Application main entrypoint

Contains the routes and their controllers.
"""

import logging
from fastapi import FastAPI, Request, Depends, Response
from fastapi.responses import StreamingResponse
from services.name_processing import NameProcessingService, InvalidInputError
from logging_utils.config import setup_logging
from utils.io_utils import save_request_to_tempfile, TempfileSaveError
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

    def sync_gen():
        return service.convert_to_ndjson_stream(temp_path)

    def next_item(gen):
        try:
            return next(gen)
        except StopIteration:
            return None

    async def async_gen():
        gen = await to_thread.run_sync(sync_gen)
        while True:
            chunk = await to_thread.run_sync(next_item, gen)
            if chunk is None:
                break
            yield chunk

    return StreamingResponse(async_gen(), media_type="application/x-ndjson")