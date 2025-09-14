from fastapi import FastAPI, Request
import tempfile
from fastapi.responses import JSONResponse, StreamingResponse
from services.name_processing import convert_to_ndjson_stream

app = FastAPI()

@app.post("/upload-json-stream")
async def upload_json_stream(request: Request):
    temp = tempfile.NamedTemporaryFile(delete=False, suffix=".json")

    async for chunk in request.stream():
        temp.write(chunk)
    temp.close()

    # Each request gets its own unique directory
    unique_dir = tempfile.mkdtemp(prefix="ndjson_")

    return StreamingResponse(
        convert_to_ndjson_stream(temp.name, output_dir=unique_dir),
        media_type="application/x-ndjson"
    )