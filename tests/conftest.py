import asyncio
import atexit
import os
from collections.abc import AsyncIterator, Iterator
from contextlib import suppress
from typing import Any

import aio_pika
import pytest
from alembic import command
from alembic.config import Config as AlembicConfig
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from testcontainers.community.postgres import PostgresContainer
from testcontainers.community.rabbitmq import RabbitMqContainer

from payments.db.session import (
    dispose_engine,
    get_engine,
    get_session_factory,
    init_engine,
)
from payments.main import app

# Session bootstrap.
#
# pytest_configure вызывается до сбора тестовых модулей, единственное
# место, где можно поднять контейнеры и выставить env так, чтобы
# module level объекты в topology.py (RETRY_STAGES, retry_queues) уже
# захватили тестовые TTL, а не дефолтные 5000/10000 мс.
#

_containers: dict[str, Any] = {}


def _unit_only(config: pytest.Config) -> bool:
    """
    Проверяет, что запущены только unit-тесты.

    Проверяем через маркер -m unit или через явный путь tests/unit.
    Подстрочный поиск unit в пути отброшен, tests/unit_and_integration
    тоже подходил бы, что неверно.
    """
    args = config.args

    if "-m" in args:
        idx = args.index("-m")
        if idx + 1 < len(args) and args[idx + 1] == "unit":
            return True

    paths = [p for p in args if p and not p.startswith("-")]
    if not paths:
        return False

    return all(p.rstrip("/").endswith("tests/unit") or p.rstrip("/") == "tests/unit" for p in paths)


def _apply_env(
    *,
    database_url: str,
    rabbitmq_url: str,
    api_key: str,
    retry_1_ms: int,
) -> None:
    os.environ["DATABASE_URL"] = database_url
    os.environ["RABBITMQ_URL"] = rabbitmq_url
    os.environ["API_KEY"] = api_key
    os.environ["RETRY_BASE_DELAY_MS"] = str(retry_1_ms)
    os.environ["RETRY_MULTIPLIER"] = "2.0"
    os.environ["RETRY_MAX_DELAY_MS"] = "60000"
    os.environ["GATEWAY_MIN_LATENCY"] = "0.01"
    os.environ["GATEWAY_MAX_LATENCY"] = "0.05"
    os.environ["WEBHOOK_TIMEOUT"] = "1.0"
    os.environ["PROCESS_MAX_ATTEMPTS"] = "3"


def _apply_fake_env() -> None:
    """Заглушки для случаев, когда реальная БД не нужна (unit, collect-only)."""
    _apply_env(
        database_url="postgresql+asyncpg://x:x@localhost:5432/x",
        rabbitmq_url="amqp://guest:guest@localhost:5672/",
        api_key="test-key",
        retry_1_ms=200,
    )


def _start_containers_and_migrate() -> None:
    pg = PostgresContainer("postgres:16-alpine", driver="asyncpg")
    pg.start()
    atexit.register(pg.stop)

    rmq = RabbitMqContainer("rabbitmq:3.13-alpine")
    rmq.start()
    atexit.register(rmq.stop)

    _containers["pg"] = pg
    _containers["rmq"] = rmq

    # Собираем URI из host/port вручную, работает на любом контейнере.
    rmq_host = rmq.get_container_host_ip()
    rmq_port = rmq.get_exposed_port(5672)
    rabbitmq_url = f"amqp://guest:guest@{rmq_host}:{rmq_port}/"

    _apply_env(
        database_url=pg.get_connection_url(),
        rabbitmq_url=rabbitmq_url,
        api_key="test-key",
        retry_1_ms=200,
    )

    cfg = AlembicConfig("alembic.ini")
    cfg.set_main_option("sqlalchemy.url", pg.get_connection_url())
    command.upgrade(cfg, "head")


def pytest_configure(config: pytest.Config) -> None:
    # env нужен всегда: импорт payments.* вызывает get_settings().
    if config.option.collectonly or _unit_only(config):
        _apply_fake_env()
        return

    _start_containers_and_migrate()


# Фикстуры
@pytest.fixture(scope="session")
def _engine_ready() -> Iterator[None]:
    """
    Инициализирует движок один раз на всю сессию и освобождает в конце.

    FastAPI lifespan в ASGITransport не запускается, поэтому init_engine
    делаем явно. dispose_engine вызываем при выходе из сессии, иначе
    при повторных запусках в одной БД копятся idle connections.
    """

    init_engine()
    yield
    asyncio.run(dispose_engine())


@pytest.fixture(autouse=True)
async def _clean_db(request: pytest.FixtureRequest) -> AsyncIterator[None]:
    """
    TRUNCATE для integration тестов. Для unit, no op.

    Unit тесты не должны зависеть от БД, маркер integration активирует
    реальную очистку. Это изолирует быстрые unit-тесты от инфраструктуры.
    """
    if "integration" not in request.keywords:
        yield
        return

    init_engine()

    async def _truncate() -> None:
        async with get_engine().begin() as conn:
            await conn.execute(text("TRUNCATE payments, outbox RESTART IDENTITY CASCADE"))

    await _truncate()
    yield
    await _truncate()


@pytest.fixture
async def db_session() -> AsyncIterator[Any]:
    """Async-сессия для прямого доступа к БД из тестов."""

    init_engine()
    async with get_session_factory()() as session:
        yield session


@pytest.fixture
async def api_client() -> AsyncIterator[Any]:
    """
    HTTP-клиент к FastAPI через ASGITransport. Без сети и uvicorn.

    ASGITransport не запускает lifespan приложения. У нас в lifespan
    только init_engine/dispose_engine, init_engine вызывается здесь,
    dispose делает session scoped _engine_ready. Если в lifespan появятся
    другие ресурсы (брокер, кэш), потребуется asgi-lifespan.LifespanManager.
    """

    init_engine()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client


@pytest.fixture
async def clean_rabbitmq() -> AsyncIterator[None]:
    """
    Удаляет очереди перед и после теста.

    Каждое удаление, на отдельном канале. RabbitMQ при попытке удалить
    несуществующую очередь отвечает 404 NOT_FOUND и закрывает канал. RobustChannel восстановится не мгновенно,
    и следующий delete на том же канале упадёт с ChannelInvalidStateError. Изоляция каналов устраняет этот класс ошибок.

    Exchange не трогается: topology объявляется через _ensure_topology
    в тестах и через declare_topology в продакшене.
    """
    from payments.messaging.topology import dlq, payments_queue, retry_queues

    queue_names = [
        payments_queue.name,
        dlq.name,
        *(q.name for q in retry_queues),
    ]

    async def _drop_queues() -> None:
        conn = await aio_pika.connect_robust(os.environ["RABBITMQ_URL"])
        try:
            for name in queue_names:
                channel = await conn.channel()
                try:
                    with suppress(aio_pika.exceptions.ChannelNotFoundEntity):
                        await channel.queue_delete(name)
                finally:
                    await channel.close()
        finally:
            await conn.close()

    await _drop_queues()
    yield
    await _drop_queues()
