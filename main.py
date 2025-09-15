import tempfile
import logging
from fastapi import FastAPI, Request, Depends, HTTPException, Response
from fastapi.responses import StreamingResponse
from services.name_processing import NameProcessingService, InvalidInputError
from logging_utils.config import setup_logging

logger = setup_logging()

logger = logging.getLogger(__name__)
app = FastAPI()

# Create one service instance (could later be swapped for a different implementation)
def get_name_service():
    return NameProcessingService()

@app.post("/combine-names")
async def combine_names(
    request: Request,
    service: NameProcessingService = Depends(get_name_service)
):
    # Save incoming file to a temp JSON file
    temp = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
    async for chunk in request.stream():
        temp.write(chunk)
    temp.close()

    try:
        gen = service.convert_to_ndjson_stream(temp.name)

        first_chunk = next(gen)

        # Wrap the generator with the first chunk re-injected
        # in order to display the error response status
        def safe_gen():
            yield first_chunk
            yield from gen

        return StreamingResponse(safe_gen(), media_type="application/x-ndjson")

    except InvalidInputError as e:
         logger.error(
             "Invalid input for /combine-names: %s",
             e.raw_error,
             exc_info=True
         )
         return Response(content=e.message, status_code=e.status_code)