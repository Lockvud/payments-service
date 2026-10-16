import asyncio
import contextlib
import logging
import signal
from datetime import UTC, datetime

from faststream.rabbit import RabbitBroker
from pydantic import ValidationError
from sqlalchemy import select

from payments.core.config import get_settings
from payments.core.logging import configure_logging
from payments.db.session import dispose_engine, get_session_factory, init_engine
from payments.messaging.broker import get_broker
from payments.messaging.publisher import publish_event
from payments.messaging.topology import (
    PAYMENT_ROUTING_KEY,
    declare_topology,
    payments_exchange,
)
from payments.models.outbox import Outbox, OutboxStatus
from payments.schemas.events import PaymentEvent

logger = logging.getLogger(__name__)


async def _drain_once(broker: RabbitBroker, batch_size: int) -> int:
    """
    Забирает пачку pending событий и публикует их.

    Возвращает количество реально опубликованных событий, а не размер
    пачки. Это критично для run_forever: если вся пачка состоит из
    невалидных payload (Poison Pill), функция вернёт 0, и цикл уйдёт
    в sleep вместо busy loop.

    Невалидные строки остаются в PENDING и будут перечитываться на
    следующей итерации. В проде добавил бы
    отдельный статус failed + метрику.
    """
    factory = get_session_factory()
    async with factory() as session, session.begin():
        stmt = (
            select(Outbox)
            .where(Outbox.status == OutboxStatus.PENDING)
            .order_by(Outbox.created_at)
            .limit(batch_size)
            .with_for_update(skip_locked=True)
        )
        rows = list((await session.scalars(stmt)).all())
        if not rows:
            return 0

        now = datetime.now(tz=UTC)
        published = 0
        for row in rows:
            try:
                event = PaymentEvent.model_validate(row.payload)
            except ValidationError:
                logger.error(
                    "invalid outbox payload id=%s aggregate_id=%s, skipping",
                    row.id,
                    row.aggregate_id,
                    exc_info=True,
                )
                continue

            await publish_event(
                broker,
                event,
                exchange=payments_exchange,
                routing_key=PAYMENT_ROUTING_KEY,
            )
            row.status = OutboxStatus.PUBLISHED
            row.published_at = now
            published += 1

        return published


async def run_forever(broker: RabbitBroker, stop: asyncio.Event) -> None:
    """Цикл публикации. При пустой пачке ждёт outbox_poll_interval секунд."""
    settings = get_settings()

    while not stop.is_set():
        try:
            processed = await _drain_once(
                broker,
                settings.outbox_batch_size,
            )
        except Exception:
            logger.exception("outbox publisher iteration failed")
            processed = 0

        if processed == 0:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(
                    stop.wait(),
                    timeout=settings.outbox_poll_interval,
                )


async def _close_broker(broker: RabbitBroker) -> None:
    """Останавливает брокер, не прерывая освобождение ресурсов БД."""
    try:
        await broker.stop()
    except Exception:
        logger.exception("failed to stop broker cleanly")


async def _start_broker_with_retry(
    broker: RabbitBroker,
    *,
    attempts: int = 10,
    delay: float = 2.0,
) -> None:
    """
    Устойчивый старт брокера. RabbitMQ может ещё принимать соединения,
    даже если контейнер уже healthy. Ретраим с фиксированной задержкой.
    """
    last_exc: BaseException | None = None
    for attempt in range(1, attempts + 1):
        try:
            await broker.start()
            return
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            logger.warning("broker.start attempt %d/%d failed: %s", attempt, attempts, exc)
            if attempt < attempts:
                await asyncio.sleep(delay)
    raise RuntimeError(f"broker.start failed after {attempts} attempts") from last_exc


async def main() -> None:
    """
    Точка входа Outbox Publisher. Целевая среда, Linux-контейнер,
    loop.add_signal_handler не работает на Windows event loop.
    """
    configure_logging()
    logger.info("outbox publisher starting")

    init_engine()
    try:
        broker = get_broker()
        await _start_broker_with_retry(broker)

        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)

        try:
            await declare_topology(broker)
            await run_forever(broker, stop)
        finally:
            logger.info("outbox publisher stopping")
            await _close_broker(broker)
    finally:
        await dispose_engine()


if __name__ == "__main__":
    asyncio.run(main())
