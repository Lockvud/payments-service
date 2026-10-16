from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from payments.messaging import consumer
from payments.schemas.events import PaymentEvent


def _event(attempt: int, payment_id: UUID | None = None) -> PaymentEvent:
    return PaymentEvent(
        payment_id=payment_id or uuid4(),
        occurred_at=datetime.now(tz=UTC),
        attempt=attempt,
    )


# Маршрутизация по attempt


# publish_event вызывается с event как ПОЗИЦИОННЫМ аргументом:
# publish_event(broker, event, exchange, routing_key)
# Поэтому в call_args он в args[1], а exchange/routing_key, в kwargs.
@pytest.mark.asyncio
async def test_attempt_1_schedules_first_retry() -> None:

    event = _event(attempt=1)
    with patch.object(consumer, "publish_event", new=AsyncMock()) as publish:
        await consumer._route_after_failure(event, RuntimeError("boom"))

    # Ровно один publish, иначе сообщение продублируется в очереди.
    assert publish.call_count == 1
    args, kwargs = publish.call_args
    forwarded: PaymentEvent = args[1]
    assert kwargs["exchange"].name == "payments.retry.200ms"
    assert kwargs["routing_key"] == "payment.retry.200ms"
    assert forwarded.attempt == 2
    assert forwarded.payment_id == event.payment_id


@pytest.mark.asyncio
async def test_attempt_2_schedules_second_retry() -> None:

    event = _event(attempt=2)
    with patch.object(consumer, "publish_event", new=AsyncMock()) as publish:
        await consumer._route_after_failure(event, RuntimeError("boom"))

    assert publish.call_count == 1
    args, kwargs = publish.call_args
    forwarded: PaymentEvent = args[1]
    assert kwargs["exchange"].name == "payments.retry.400ms"
    assert kwargs["routing_key"] == "payment.retry.400ms"
    assert forwarded.attempt == 3
    assert forwarded.payment_id == event.payment_id


@pytest.mark.asyncio
async def test_attempt_3_sends_to_dlq() -> None:

    event = _event(attempt=3)
    with patch.object(consumer, "publish_event", new=AsyncMock()) as publish:
        await consumer._route_after_failure(event, RuntimeError("boom"))

    assert publish.call_count == 1
    args, kwargs = publish.call_args
    forwarded: PaymentEvent = args[1]
    assert kwargs["exchange"].name == "payments.dlx"
    assert kwargs["routing_key"] == "payment.dead"
    # attempt остаётся 3, исчерпали попытки, дальше не растёт.
    assert forwarded.attempt == 3
    assert forwarded.payment_id == event.payment_id


# Non-retryable, сразу DLQ, без промежуточных стадий
@pytest.mark.asyncio
async def test_non_retryable_goes_straight_to_dlq() -> None:

    event = _event(attempt=1)
    with patch.object(consumer, "publish_event", new=AsyncMock()) as publish:
        await consumer._route_after_failure(event, RuntimeError("nope"), retryable=False)

    assert publish.call_count == 1
    args, kwargs = publish.call_args
    forwarded: PaymentEvent = args[1]
    assert kwargs["exchange"].name == "payments.dlx"
    assert kwargs["routing_key"] == "payment.dead"
    # attempt не увеличивается, retry-стадии пропускаются.
    assert forwarded.attempt == 1
    assert forwarded.payment_id == event.payment_id


@pytest.mark.asyncio
async def test_non_retryable_after_partial_retry() -> None:
    """Non retryable на attempt=2 не идёт в retry, сразу в DLQ."""

    event = _event(attempt=2)
    with patch.object(consumer, "publish_event", new=AsyncMock()) as publish:
        await consumer._route_after_failure(event, RuntimeError("nope"), retryable=False)

    args, kwargs = publish.call_args
    forwarded: PaymentEvent = args[1]
    assert kwargs["exchange"].name == "payments.dlx"
    assert forwarded.attempt == 2


# Ошибка публикации
@pytest.mark.asyncio
async def test_publish_failure_propagates() -> None:
    """
    Если publish_event падает, _route_after_failure пробрасывает исключение.

    handle_payment в consumer.py не перехватывает исключение из
    _route_after_failure. FastStream с ack_policy=REJECT_ON_ERROR вызовет
    reject(requeue=False), и исходное сообщение уйдёт в payments.dlq через
    safety-net DLX на payments_queue. Потери нет.
    """

    event = _event(attempt=1)
    with (
        patch.object(
            consumer,
            "publish_event",
            new=AsyncMock(side_effect=RuntimeError("broker down")),
        ),
        pytest.raises(RuntimeError, match="broker down"),
    ):
        await consumer._route_after_failure(event, RuntimeError("original"))


@pytest.mark.asyncio
async def test_publish_failure_to_dlq_propagates() -> None:
    """Симметрично: сбой публикации в DLQ на attempt=3 тоже пробрасывается."""

    event = _event(attempt=3)
    with (
        patch.object(
            consumer,
            "publish_event",
            new=AsyncMock(side_effect=RuntimeError("broker down")),
        ),
        pytest.raises(RuntimeError, match="broker down"),
    ):
        await consumer._route_after_failure(event, RuntimeError("original"))


# Контракт PaymentEvent.attempt
def test_attempt_below_minimum_rejected() -> None:
    """attempt=0 не проходит Pydantic-валидацию: ge=1."""
    with pytest.raises(ValidationError):
        _event(attempt=0)


def test_attempt_negative_rejected() -> None:
    with pytest.raises(ValidationError):
        _event(attempt=-1)


def test_attempt_above_ceiling_rejected() -> None:
    """attempt=11 не проходит валидацию: le=MAX_ATTEMPT_CEILING."""
    with pytest.raises(ValidationError):
        _event(attempt=11)


def test_attempt_boundary_values_accepted() -> None:
    """Границы валидного диапазона принимаются."""
    assert _event(attempt=1).attempt == 1
    assert _event(attempt=10).attempt == 10
