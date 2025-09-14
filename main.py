from fastapi import FastAPI, Request, Depends
from fastapi.responses import StreamingResponse
import tempfile

from services.name_processing import NameProcessingService

app = FastAPI()

# Create one service instance (could later be swapped for a different implementation)
def get_name_service():
    return NameProcessingService()


@app.post("/upload-json-stream")
async def upload_json_stream(
    request: Request,
    service: NameProcessingService = Depends(get_name_service)
):
    # Save incoming file to a temp JSON file
    temp = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
    async for chunk in request.stream():
        temp.write(chunk)
    temp.close()

    # Return streaming response from the service
    return StreamingResponse(
        service.convert_to_ndjson_stream(temp.name),
        media_type="application/x-ndjson"
    )