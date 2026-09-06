import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from .consumers import BINDINGS
from .core.config import settings
from .events import consume


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    consumer = asyncio.create_task(consume(BINDINGS))
    yield
    consumer.cancel()
    await asyncio.gather(consumer, return_exceptions=True)


app = FastAPI(title="Atlas {{cookiecutter.service_name}} API", version="0.1.0", lifespan=lifespan)


@app.get("/api/v1/{{cookiecutter.service_name}}/healthz")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": settings.service_name}


@app.get("/healthz", include_in_schema=False)
async def internal_health() -> dict[str, str]:
    return await health()

