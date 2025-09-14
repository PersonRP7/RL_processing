from fastapi import FastAPI, Request
import tempfile
from fastapi.responses import JSONResponse, StreamingResponse
from services.name_processing import convert_to_ndjson_stream

app = FastAPI()


@app.post("/upload-json-stream")
async def upload_json_stream(request: Request):
    # Save incoming file to temp first (still chunked, avoids memory blowup)
    temp = tempfile.NamedTemporaryFile(delete=False, suffix=".json")

    async for chunk in request.stream():
        temp.write(chunk)

    temp.close()

    # Return a StreamingResponse that streams NDJSON back to client
    return StreamingResponse(
        convert_to_ndjson_stream(temp.name),
        media_type="application/x-ndjson"
    )