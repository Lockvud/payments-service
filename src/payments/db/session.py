from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from payments.core.config import get_settings

_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def init_engine() -> None:
    """
    Создаёт движок и фабрику сессий. Вызывается один раз при старте процесса:
    - из FastAPI lifespan (API-процесс),
    - из consumer on_startup,
    - из outbox_publisher.main().

    Идемпотентна при последовательном вызове. Не рассчитана на конкурентный
    вызов из нескольких потоков, подразумевается, что старт процесса
    однопоточный и происходит до начала обработки запросов.

    Если процесс обратился к get_session_factory без init_engine, падаем
    с явной ошибкой, а не строим движок неявно: это ловит ошибки
    инициализации на старте, а не в середине обработки запроса.
    """
    global _engine, _session_factory
    if _engine is not None:
        return

    engine = create_async_engine(
        get_settings().database_url,
        pool_pre_ping=True,
        # 5 + 5 overflow = до 10 соединений на один процесс.
        # Реальный лимит зависит от количества реплик: при R реплик
        # каждого из трёх сервисов суммарный потолок, R * 3 * 10.
        # Для тестового задания R=1, то есть до 30 соединений, что
        # укладывается в дефолтный max_connections=100 у PostgreSQL.
        pool_size=5,
        max_overflow=5,
    )
    _engine = engine
    _session_factory = async_sessionmaker(
        engine,
        expire_on_commit=False,
        autoflush=False,
    )


async def dispose_engine() -> None:
    """Закрывает пул соединений. Вызывается при остановке процесса."""
    global _engine, _session_factory
    try:
        if _engine is not None:
            await _engine.dispose()
    finally:
        # Сбрасываем глобалы даже если dispose() бросил, иначе повторный
        # init_engine вернёт мёртвый движок.
        _engine = None
        _session_factory = None


def get_engine() -> AsyncEngine:
    """
    Возвращает активный движок. Используется в тестах для TRUNCATE
    и в воркерах для доступа к пулу.
    """
    if _engine is None:
        raise RuntimeError("engine is not initialised; call init_engine() during application startup")
    return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    if _session_factory is None:
        raise RuntimeError("engine is not initialised; call init_engine() during application startup")
    return _session_factory


@asynccontextmanager
async def session_scope() -> AsyncGenerator[AsyncSession]:
    """
    Контекст менеджер сессии для воркеров (consumer, outbox publisher)
    и для FastAPI зависимости.

    Commit не выполняется автоматически, границы транзакций определяет
    сервисный слой. Для платёжной логики это критично, charge, обновление
    статуса и webhook, это разные фазы с разными требованиями к
    атомарности и порядку. Неявный commit в конце запроса смешал бы их.

    Если сервис завершится без явного commit и без исключения, изменения
    не сохранятся, то есть SQLAlchemy откатит открытую транзакцию при закрытии
    сессии. Это осознанный компромисс в пользу явности.
    """
    factory = get_session_factory()
    async with factory() as session:
        yield session


async def get_session() -> AsyncGenerator[AsyncSession]:
    """
    FastAPI зависимость: одна сессия на запрос.
    """
    async with session_scope() as session:
        yield session
