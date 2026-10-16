from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from payments.api import payments_router
from payments.core.logging import configure_logging
from payments.db.session import dispose_engine, init_engine

configure_logging()


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncGenerator[None]:
    """
    Управление ресурсами API-процесса.

    Брокер RabbitMQ здесь НЕ стартует. Публикация событий, обязанность
    отдельного процесса outbox-publisher. API пишет только в PostgreSQL,
    поэтому N реплик API не множат циклы публикации и не конкурируют
    за строки outbox.

    Единственный ресурс API, пул соединений с БД. Он создаётся до yield
    и освобождается после, гарантированно (даже при исключении в lifespan).
    """
    init_engine()
    try:
        yield
    finally:
        await dispose_engine()


app = FastAPI(
    title="Payments Service",
    version="0.1.0",
    description="Асинхронный сервис процессинга платежей",
    lifespan=lifespan,
)
app.include_router(payments_router)
