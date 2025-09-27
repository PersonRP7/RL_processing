"""Application main entrypoint

Contains the routes and their controllers.
"""
import logging
from fastapi import FastAPI, Request, Depends, Response
from services.name_processing import NameProcessingService
from logging_utils.config import setup_logging
from utils.generator_utils import stream_sync_service
from utils.request_utils import save_request_with_handling

setup_logging()
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
    # Step 1: Save the request body to a tempfile
    temp_path_or_response = await save_request_with_handling(request, logger=logger)
    if isinstance(temp_path_or_response, Response):
        # Early return if saving or validation failed
        return temp_path_or_response

    # Step 2: Process JSON in an async generator
    return await stream_sync_service(
        service.convert_to_ndjson_stream,
        temp_path_or_response
        )