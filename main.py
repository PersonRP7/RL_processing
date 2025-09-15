from fastapi import FastAPI, Request, Depends, HTTPException, Response
from fastapi.responses import StreamingResponse
import tempfile

from services.name_processing import NameProcessingService, InvalidInputError

app = FastAPI()

# Create one service instance (could later be swapped for a different implementation)
def get_name_service():
    return NameProcessingService()


# @app.post("/combine-names")
# async def upload_json_stream(
#     request: Request,
#     service: NameProcessingService = Depends(get_name_service)
# ):
#     # Save incoming file to a temp JSON file
#     temp = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
#     async for chunk in request.stream():
#         temp.write(chunk)
#     temp.close()

#     # Return streaming response from the service
#     return StreamingResponse(
#         service.convert_to_ndjson_stream(temp.name),
#         media_type="application/x-ndjson"
#     )

# @app.post("/combine-names")
# async def combine_names(
#     request: Request,
#     service: NameProcessingService = Depends(get_name_service)
# ):
#     temp = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
#     async for chunk in request.stream():
#         temp.write(chunk)
#     temp.close()

#     try:
#         return StreamingResponse(
#             service.convert_to_ndjson_stream(temp.name),
#             media_type="application/x-ndjson"
#         )
#     except InvalidInputError as e:
#         raise HTTPException(status_code=e.status_code, detail=e.message)

# @app.post("/combine-names")
# async def combine_names(
#     request: Request,
#     service: NameProcessingService = Depends(get_name_service)
# ):
#     temp = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
#     async for chunk in request.stream():
#         temp.write(chunk)
#     temp.close()

#     try:
#         return StreamingResponse(
#             service.convert_to_ndjson_stream(temp.name),
#             media_type="application/x-ndjson"
#         )
#     except InvalidInputError as e:
#         # here you only expose the safe message to the client
#         raise HTTPException(status_code=e.status_code, detail=e.message)

# @app.post("/combine-names")
# async def combine_names(
#     request: Request,
#     service: NameProcessingService = Depends(get_name_service)
# ):
#     # Save incoming file to a temp JSON file
#     temp = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
#     async for chunk in request.stream():
#         temp.write(chunk)
#     temp.close()

#     try:
#         # Create the generator first to catch early errors
#         gen = service.convert_to_ndjson_stream(temp.name)
#         return StreamingResponse(gen, media_type="application/x-ndjson")

#     except InvalidInputError as e:
#         # Here you map service exception -> HTTP response
#         raise HTTPException(status_code=e.status_code, detail=e.message)

# Initial working version
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
        # raise HTTPException(status_code=e.status_code, detail=e.message)
        # return Response(content = "400 error", status_code = 400)
         return Response(content=e.message, status_code=e.status_code)