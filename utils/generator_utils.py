from anyio import to_thread
from fastapi.responses import StreamingResponse
from typing import Callable, AsyncGenerator, Generator

async def sync_gen_to_async_gen(sync_gen: Generator) -> AsyncGenerator:
    """
    Wrap a blocking sync generator into an async generator that can be used
    in FastAPI without blocking the event loop.
    """
    while True:
        item = await to_thread.run_sync(lambda: next(sync_gen, None))
        if item is None:
            break
        yield item

async def stream_sync_service(
    service_method: Callable[[str], Generator],
    temp_path: str,
) -> StreamingResponse:
    """
    Wrap a blocking sync service method that returns a generator
    into a StreamingResponse with async iteration.
    """
    def sync_gen():
        return service_method(temp_path)

    gen = await to_thread.run_sync(sync_gen)
    async_gen = sync_gen_to_async_gen(gen)
    return StreamingResponse(async_gen, media_type="application/x-ndjson")