import asyncio
import json
import os
from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import aio_pika
import pytest
from faststream.rabbit import RabbitBroker
from sqlalchemy.ext.asyncio import AsyncSession

from payments.core.config import get_settings
from payments.messaging.broker import get_broker
from payments.messaging.consumer import broker as consumer_broker
from payments.messaging.topology import (
    PAYMENT_ROUTING_KEY,
    declare_topology,
    payments_exchange,
    payments_queue,
)
from payments.models.payment import Currency, Payment, PaymentStatus
from payments.schemas.events import PaymentEvent
from payments.services.payments import PaymentService


async def _wait_for_consumer(queue_name: str, timeout: float) -> None:
    """
    Ждёт, пока на очереди появится хотя бы один активный consumer.

    Заменяет фиксированный sleep(1.0), FastStream запускает подписчиков
    фоновыми тасками и не даёт хук готово. Единственный надёжный
    сигнал, RabbitMQ Management API (consumer_count), который aio-pika
    отдаёт в declaration_result пассивного declare_queue.

    consumer_count имеет тип int | None: None означает, что RabbitMQ
    не вернул это поле.
    """
    conn = await aio_pika.connect_robust(os.environ["RABBITMQ_URL"])
    try:
        channel = await conn.channel()
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            queue = await channel.get_queue(queue_name)
            if (queue.declaration_result.consumer_count or 0) >= 1:
                return
            await asyncio.sleep(0.1)

        raise AssertionError(f"no active consumer on {queue_name} within {timeout}s")
    finally:
        await conn.close()


async def _wait_for_dlq_message(timeout: float) -> str:
    """
    Ждёт появления сообщения в payments.dlq, возвращает тело.

    Ack выполняется после успешного чтения. Для диагностики в
    assertion сообщении указывается тело; логи консьюмера содержат
    полный traceback. Clean_rabbitmq всё равно удалит очередь между тестами, поэтому, если сообщение не подтвердить,
    следующий тест увидит грязное состояние.
    """
    conn = await aio_pika.connect_robust(os.environ["RABBITMQ_URL"])
    try:
        channel = await conn.channel()
        queue = await channel.get_queue("payments.dlq")

        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            message = await queue.get(timeout=1.0, fail=False)
            if message is None:
                continue
            body = message.body.decode()
            await message.ack()
            return body

        raise AssertionError("no message in DLQ within timeout")
    finally:
        await conn.close()


async def _ensure_topology() -> None:
    """
    Объявляет topology через временный брокер.

    FastStream подписчик при вызове start() декларирует только свою очередь,payments.q. Очереди повторных попыток retry
    и мёртвых писем (DLQ) не привязываются к подписчикам, их нужно объявлять отдельно. Используется тот же
    declare_topology, что и в consumer.py, поэтому аргументы очередей
    гарантированно совпадают, PRECONDITION_FAILED при несовпадении x-message-ttl исключён.
    """

    temp = RabbitBroker(str(get_settings().rabbitmq_url))
    await temp.start()
    try:
        await declare_topology(temp)
    finally:
        await temp.stop()


def _payments_queue_name() -> str:
    """Имя основной очереди, единый источник правды в topology.py."""

    return payments_queue.name


@pytest.mark.integration
@pytest.mark.asyncio
async def test_retry_exhausts_and_lands_in_dlq(clean_rabbitmq: None, db_session: AsyncSession) -> None:

    payment = Payment(
        id=uuid4(),
        amount=10,
        currency=Currency.RUB,
        description="",
        meta={},
        webhook_url="https://example.com/hook",
        idempotency_key=f"dlq-{uuid4()}",
        status=PaymentStatus.PENDING,
        created_at=datetime.now(tz=UTC),
    )
    db_session.add(payment)
    await db_session.commit()

    # В рамках sanity‑проверки файл consumer.py и соответствующие тесты используют один и тот же
    # экземпляр брокера сообщений
    assert consumer_broker is get_broker()

    # Топология: exchange, payments.q, retry-очереди, DLQ.
    # TTL retry-очередей заданы в conftest._apply_env до импорта topology:
    # 200 / 400 мс. Тест не ждёт реальные 5 / 10 секунд.
    await _ensure_topology()

    event = PaymentEvent(
        payment_id=payment.id,
        occurred_at=datetime.now(tz=UTC),
        attempt=1,
    )

    # Запускаем подписчиков
    await consumer_broker.start()
    try:
        await _wait_for_consumer(_payments_queue_name(), timeout=5.0)

        with patch.object(
            PaymentService,
            "process",
            new=AsyncMock(side_effect=RuntimeError("infra down")),
        ) as mock_process:
            await consumer_broker.publish(
                event,
                exchange=payments_exchange,
                routing_key=PAYMENT_ROUTING_KEY,
                persist=True,
            )

            body = await _wait_for_dlq_message(timeout=20.0)

        # call_count == 3, фактические вызовы обработчика, а не только
        # данные в payload. attempt=3 в DLQ без трёх вызовов process
        # означал бы, что цепочка retry сломана.
        assert mock_process.call_count == 3, f"expected 3 process() calls, got {mock_process.call_count}"
    finally:
        await consumer_broker.stop()

    data = json.loads(body)
    assert data["payment_id"] == str(payment.id)
    assert data["attempt"] == 3
