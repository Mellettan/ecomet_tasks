from contextlib import asynccontextmanager
from typing import Annotated

import asyncpg
import uvicorn
from fastapi import APIRouter, FastAPI, Depends, Request
from config import settings


async def get_pg_connection(request: Request) -> asyncpg.Connection:
    pool: asyncpg.Pool = request.app.state.pool

    async with pool.acquire() as connection:
        yield connection


async def get_db_version(
    conn: Annotated[asyncpg.Connection, Depends(get_pg_connection)],
):
    return await conn.fetchval("SELECT version()")


def register_routes(app: FastAPI):
    router = APIRouter(prefix="/api")
    router.add_api_route(path="/db_version", endpoint=get_db_version, methods=["GET"])
    app.include_router(router)


@asynccontextmanager
async def lifespan(app: FastAPI):
    pool = await asyncpg.create_pool(dsn=settings.POSTGRES_DSN, min_size=2, max_size=10)
    app.state.pool = pool  # noqa

    yield

    await pool.close()


def create_app() -> FastAPI:
    app = FastAPI(title="e-Comet", lifespan=lifespan)
    register_routes(app)
    return app


if __name__ == "__main__":
    uvicorn.run("main:create_app", factory=True)
