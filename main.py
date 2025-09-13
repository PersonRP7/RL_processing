from fastapi import FastAPI, Request
import tempfile
from fastapi.responses import JSONResponse

app = FastAPI()

@app.post("/upload-json")
async def upload_json(request: Request):
    # Step 1: Save uploaded JSON to a temporary file in chunks
    temp = tempfile.NamedTemporaryFile(delete=False, suffix=".json")

    async for chunk in request.stream():
        temp.write(chunk)

    temp.close()

    return JSONResponse({"message": "Upload successful", "temp_file": temp.name})