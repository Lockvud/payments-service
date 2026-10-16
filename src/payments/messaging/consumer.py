import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from types import TracebackType

from faststream import AckPolicy, FastStream
from faststream.rabbit import RabbitBroker

from payments.core.config import get_settings
from payments.core.logging import configure_logging
from payments.db.session import dispose_engine, init_engine, session_scope
from payments.messaging.broker import get_broker
from payments.messaging.publisher import publish_event
from payments.messaging.topology import (
    DLQ_ROUTING_KEY,
    RETRY_STAGES,
    assert_topology_consistent,
    declare_topology,
    dlx_exchange,
    payments_exchange,
    payments_queue,
)
from payments.schemas.events import PaymentEvent
from payments.services.payments import PaymentService
from payments.services.webhooks import UnsafeWebhookUrlError

configure_logging()
logger = logging.getLogger(__name__)

settings = get_settings()
broker: RabbitBroker = get_broker()


async def _on_startup() -> None:
    """
    Проверка инвариантов
    и инициализация пула БД.
    """
    assert_topology_consistent(settings.process_max_attempts)
    init_engine()


async def _on_shutdown() -> None:
    await dispose_engine()


app = FastStream(
    broker,
    on_startup=[_on_startup],
    on_shutdown=[_on_shutdown],
)


@app.after_startup
async def _on_after_startup() -> None:
    """
    after_startup выполняется после подключения брокера, единственное
    место, где можно безопасно объявлять exchanges/queues/bindings.
    """
    await declare_topology(broker)


@asynccontextmanager
async def _service() -> AsyncGenerator[PaymentService]:
    async with session_scope() as session:
        yield PaymentService(session)


def _exc_info(
    exc: BaseException,
) -> tuple[type[BaseException], BaseException, TracebackType | None]:
    return (type(exc), exc, exc.__traceback__)


async def _send_to_dlq(event: PaymentEvent, exc: BaseException | None) -> None:
    logger.error(
        "payment=%s attempt=%d -> DLQ",
        event.payment_id,
        event.attempt,
        exc_info=_exc_info(exc) if exc is not None else None,
    )
    await publish_event(broker, event, exchange=dlx_exchange, routing_key=DLQ_ROUTING_KEY)


async def _schedule_retry(event: PaymentEvent) -> None:
    next_attempt = event.attempt + 1
    stage = RETRY_STAGES.get(next_attempt)
    if stage is None:
        raise RuntimeError(f"no retry stage for attempt={next_attempt}")

    exchange, routing_key = stage
    logger.warning(
        "payment=%s attempt %d/%d failed, scheduling retry via %s",
        event.payment_id,
        event.attempt,
        settings.process_max_attempts,
        routing_key,
    )
    await publish_event(
        broker,
        event.next_attempt(),
        exchange=exchange,
        routing_key=routing_key,
    )


async def _route_after_failure(
    event: PaymentEvent,
    exc: BaseException,
    *,
    retryable: bool = True,
) -> None:
    if not retryable or event.attempt >= settings.process_max_attempts:
        await _send_to_dlq(event, exc)
        return

    await _schedule_retry(event)


@broker.subscriber(
    payments_queue,
    payments_exchange,
    ack_policy=AckPolicy.REJECT_ON_ERROR,
)
async def handle_payment(event: PaymentEvent) -> None:
    try:
        async with _service() as service:
            await service.process(event.payment_id)
    except UnsafeWebhookUrlError as exc:
        await _route_after_failure(event, exc, retryable=False)
    except Exception as exc:  # noqa: BLE001
        await _route_after_failure(event, exc)
