import os
from unittest.mock import AsyncMock, patch

import aio_pika
import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from payments.messaging.broker import get_broker
from payments.messaging.topology import declare_topology
from payments.models.outbox import Outbox, OutboxStatus
from payments.schemas.events import PaymentEvent
from payments.workers import outbox_publisher
from payments.workers.outbox_publisher import _drain_once

API_KEY = "test-key"


def _payload() -> dict[str, object]:
    return {
        "amount": "10.00",
        "currency": "RUB",
        "description": "",
        "metadata": {},
        "webhook_url": "https://example.com/hook",
    }


async def _create_payment(api_client: AsyncClient, key: str) -> str:
    response = await api_client.post(
        "/api/v1/payments",
        headers={"X-API-Key": API_KEY, "Idempotency-Key": key},
        json=_payload(),
    )
    assert response.status_code == 202, response.text
    payment_id: str = response.json()["payment_id"]
    return payment_id


# Доставка в RabbitMQ
@pytest.mark.integration
@pytest.mark.asyncio
async def test_outbox_event_is_published(api_client: AsyncClient, clean_rabbitmq: None) -> None:
    """
    Проверяет _drain_once() как единицу работы, забирает pending строку,
    публикует событие с корректной структурой, помечает строку published.

    Это не тест жизненного цикла Outbox Publisher как отдельного процесса.
    Тот проверяется в E2E с docker compose. Сервис outbox-publisher
    запускается со своим broker, читает БД, публикует. Здесь мы вызываем
    функцию напрямую, быстро, детерминированно, без поднятия процесса.
    """
    payment_id = await _create_payment(api_client, "outbox-1")

    broker = get_broker()
    async with broker:
        await declare_topology(broker)
        processed = await _drain_once(broker, batch_size=10)
        assert processed == 1

    conn = await aio_pika.connect_robust(os.environ["RABBITMQ_URL"])
    try:
        channel = await conn.channel()
        queue = await channel.get_queue("payments.q")

        # Один таймаут на чтение, без внешнего asyncio.wait_for
        # queue.get(timeout) сам управляет ожиданием.
        message = await queue.get(timeout=5.0, fail=False)
        assert message is not None, "expected message in payments.q"

        # Валидируем структуру события через Pydantic, проверяет
        # и типы полей, и enum, и attempt. Проверка вхождения payment_id
        # в сырой body, слишком слабый контракт
        event = PaymentEvent.model_validate_json(message.body)
        assert str(event.payment_id) == payment_id
        assert event.event_type == "payment.created"
        assert event.attempt == 1
        await message.ack()

        # Второго сообщения быть не должно, _drain_once публикует ровно
        # один раз на строку.
        duplicate = await queue.get(timeout=0.5, fail=False)
        assert duplicate is None, "expected exactly one message in payments.q"
    finally:
        await conn.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_outbox_marked_published(
    api_client: AsyncClient,
    clean_rabbitmq: None,
    db_session: AsyncSession,
) -> None:
    await _create_payment(api_client, "outbox-2")

    broker = get_broker()
    async with broker:
        await declare_topology(broker)
        await _drain_once(broker, batch_size=10)

    rows = list((await db_session.scalars(select(Outbox))).all())
    assert len(rows) == 1
    assert rows[0].status is OutboxStatus.PUBLISHED
    assert rows[0].published_at is not None


# Сохранность Outbox при сбое публикации
@pytest.mark.integration
@pytest.mark.asyncio
async def test_outbox_remains_pending_on_publish_failure(
    api_client: AsyncClient,
    clean_rabbitmq: None,
    db_session: AsyncSession,
) -> None:
    """
    Если публикация падает, строка не должна помечаться published.

    _drain_once оборачивает публикацию и UPDATE в одну транзакцию.
    Исключение при publish_event откатывает транзакцию целиком, статус
    остаётся pending. Это ключевой инвариант Outbox Pattern, потеря
    события исключена, повторная попытка произойдёт на следующей итерации.
    """
    await _create_payment(api_client, "outbox-3")

    broker = get_broker()
    async with broker:
        await declare_topology(broker)

        # Патчим в модуле outbox_publisher, а не в publisher, там импорт
        # зафиксирован в момент загрузки модуля. Два контекстных менеджера
        # объединены в один with, чтобы не нарушать правило ruff SIM117.

        with (
            patch.object(
                outbox_publisher,
                "publish_event",
                new=AsyncMock(side_effect=RuntimeError("broker unavailable")),
            ),
            pytest.raises(RuntimeError, match="broker unavailable"),
        ):
            await _drain_once(broker, batch_size=10)

    # Транзакция откатилась, статус остался pending, published_at пуст.
    rows = list((await db_session.scalars(select(Outbox))).all())
    assert len(rows) == 1
    assert rows[0].status is OutboxStatus.PENDING
    assert rows[0].published_at is None
