""" Async generator utilities.

This module provides helper functions that make it possible to integrate synchronous
(blocking) Python generators into an asynchronous FastAPI application
without blocking the worker event loop.
The core use case is streaming large responses
(e.g., NDJSON) from a service method that produces results
via a normal Python generator.

By offloading generator iteration to a worker thread and re-exposing it as an async generator,
these helpers allow FastAPI's `StreamingResponse`
to stream data incrementally while preserving non-blocking behavior.
"""
from anyio import to_thread
from fastapi.responses import StreamingResponse
from typing import Callable, AsyncGenerator, Generator

async def sync_gen_to_async_gen(sync_gen: Generator) -> AsyncGenerator:
    """
    Convert a blocking sync generator into an async generator.

    Iteration of the sync generator is executed in a
    worker thread using `anyio.to_thread.run_sync`,
    ensuring the main FastAPI event loop remains responsive.
    Each value produced by the sync generator
    is yielded asynchronously to the caller.

    Args:
        sync_gen (Generator):
            A Python generator object that
            produces items synchronously (blocking).

    Yields:
        Any:
            Items produced by the underlying sync generator,
            re-exposed in an async context.
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
    Create a StreamingResponse from a sync service method that returns a generator.

    This function bridges the gap between a blocking service method
    (which yields results synchronously) and FastAPI's asynchronous
    streaming mechanism.
    The service method is invoked in a worker thread,
    its generator is wrapped via `sync_gen_to_async_gen`,
    and the resulting async generator is streamed back to the client as NDJSON.

    Args:
        service_method (Callable[[str], Generator]):
            A blocking service function that
            takes the path to a temporary file and returns a
            generator yielding NDJSON chunks.

        temp_path (str):
            Path to a temporary file containing
            request data to be processed by the service.
    """
    def sync_gen():
        return service_method(temp_path)

    gen = await to_thread.run_sync(sync_gen)
    async_gen = sync_gen_to_async_gen(gen)
    return StreamingResponse(async_gen, media_type="application/x-ndjson")