from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from payments.models.payment import Currency, Payment, PaymentStatus
from payments.services.payments import PaymentService
from payments.services.webhooks import WebhookDeliveryError


def _make_payment(**overrides: object) -> Payment:
    defaults: dict[str, object] = {
        "id": uuid4(),
        "amount": Decimal("100.00"),
        "currency": Currency.RUB,
        "description": "",
        "meta": {},
        "webhook_url": "https://example.com/hook",
        "idempotency_key": "k",
        "status": PaymentStatus.PENDING,
        "created_at": datetime.now(tz=UTC),
        "processed_at": None,
    }
    defaults.update(overrides)
    return Payment(**defaults)


@pytest.fixture
def call_log() -> list[str]:
    """
    Общий список для проверки порядка операций.

    Порядок критичен. Сначала charge, далее commit (выход из begin) и webhook.
    Если webhook уйдёт до commit, клиент получит уведомление о платеже,
    которого ещё нет в БД.
    """
    return []


@pytest.fixture
def session(call_log: list[str]) -> AsyncMock:
    """
    AsyncMock с корректным session.begin() и записью порядка в call_log.

    AsyncSession.begin(), синхронный вызов, возвращающий объект с
    __aenter__/__aexit__. Обычный AsyncMock делает begin() корутиной,
    и async with падает.
    """
    s = AsyncMock()
    transaction = MagicMock()

    async def _enter(*_args: object) -> None:
        call_log.append("begin_enter")

    async def _exit(*_args: object) -> None:
        call_log.append("begin_exit")

    transaction.__aenter__ = AsyncMock(side_effect=_enter)
    transaction.__aexit__ = AsyncMock(side_effect=_exit)
    s.begin = MagicMock(return_value=transaction)
    return s


@pytest.fixture
def gateway(call_log: list[str]) -> AsyncMock:
    """AsyncMock с записью charge в call_log. Return value задаёт тест."""
    g = AsyncMock()

    async def _charge(*args: object, **_kwargs: object) -> bool:
        call_log.append("charge")
        return g.charge.return_value  # type: ignore[no-any-return]

    g.charge.side_effect = _charge
    return g


@pytest.fixture
def notifier(call_log: list[str]) -> AsyncMock:
    n = AsyncMock()

    async def _deliver(*args: object, **_kwargs: object) -> None:
        call_log.append("deliver")

    n.deliver.side_effect = _deliver
    return n


def _stub_get_for_update(service: PaymentService, payment: Payment | None) -> None:
    """Заменяет repository.get_for_update на возврат заданного платежа."""
    service._payments.get_for_update = AsyncMock(return_value=payment)  # type: ignore[method-assign]


# Успешная обработка
@pytest.mark.asyncio
async def test_process_calls_charge_when_pending(
    session: AsyncMock,
    gateway: AsyncMock,
    notifier: AsyncMock,
    call_log: list[str],
) -> None:
    payment = _make_payment(status=PaymentStatus.PENDING)
    gateway.charge.return_value = True

    service = PaymentService(session, gateway=gateway, notifier=notifier)
    _stub_get_for_update(service, payment)

    await service.process(payment.id)

    gateway.charge.assert_awaited_once_with(payment.amount, payment.currency)
    assert call_log == ["begin_enter", "charge", "begin_exit", "deliver"]
    assert payment.status is PaymentStatus.SUCCEEDED
    assert payment.processed_at is not None


@pytest.mark.asyncio
async def test_process_marks_failed_on_business_decline(
    session: AsyncMock,
    gateway: AsyncMock,
    notifier: AsyncMock,
) -> None:
    """charge() == False, бизнес-отказ. Статус FAILED, webhook всё равно уходит."""
    payment = _make_payment(status=PaymentStatus.PENDING)
    gateway.charge.return_value = False

    service = PaymentService(session, gateway=gateway, notifier=notifier)
    _stub_get_for_update(service, payment)

    await service.process(payment.id)

    gateway.charge.assert_awaited_once()
    notifier.deliver.assert_awaited_once()
    assert payment.status is PaymentStatus.FAILED
    assert payment.processed_at is not None


# Терминальные статусы, charge не вызывается повторно
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "terminal_status",
    [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED],
)
async def test_process_skips_charge_when_terminal(
    session: AsyncMock,
    gateway: AsyncMock,
    notifier: AsyncMock,
    terminal_status: PaymentStatus,
) -> None:
    """
    Retry сообщения после неудачной доставки webhook не должен вызывать
    charge повторно. Это защита от двойного списания. Касается любого
    терминального статуса, включая FAILED.
    """
    payment = _make_payment(
        status=terminal_status,
        processed_at=datetime.now(tz=UTC),
    )

    service = PaymentService(session, gateway=gateway, notifier=notifier)
    _stub_get_for_update(service, payment)

    await service.process(payment.id)

    gateway.charge.assert_not_awaited()
    notifier.deliver.assert_awaited_once()


@pytest.mark.asyncio
async def test_process_drops_missing_payment(
    session: AsyncMock,
    gateway: AsyncMock,
    notifier: AsyncMock,
) -> None:
    service = PaymentService(session, gateway=gateway, notifier=notifier)
    _stub_get_for_update(service, None)

    await service.process(uuid4())

    gateway.charge.assert_not_awaited()
    notifier.deliver.assert_not_awaited()


# Ошибки
@pytest.mark.asyncio
async def test_process_propagates_webhook_error(
    session: AsyncMock,
    gateway: AsyncMock,
    notifier: AsyncMock,
    call_log: list[str],
) -> None:
    """
    Ошибка webhook поднимается наружу, её обрабатывает consumer через
    retry/DLQ.

    Порядок в call_log доказывает, begin_exit (commit) выполнен до
    deliver. К моменту падения webhook'а статус зафиксирован в БД.
    На уровне unit мы видим только exit из контекста, фактическую
    фиксацию проверяет интеграционный тест (когда появится реальная БД).

    side_effect из фикстуры notifier заменяем на свою функцию, которая
    И пишет "deliver" в call_log, И бросает WebhookDeliveryError. Прямое
    присваивание WebhookDeliveryError затирает запись в call_log,
    теряем проверку порядка.
    """

    payment = _make_payment(status=PaymentStatus.PENDING)
    gateway.charge.return_value = True

    async def _deliver_and_raise(*_args: object, **_kwargs: object) -> None:
        call_log.append("deliver")
        raise WebhookDeliveryError("boom")

    notifier.deliver.side_effect = _deliver_and_raise

    service = PaymentService(session, gateway=gateway, notifier=notifier)
    _stub_get_for_update(service, payment)

    with pytest.raises(WebhookDeliveryError):
        await service.process(payment.id)

    # Порядок: commit (begin_exit) произошёл до попытки webhook.
    assert call_log == ["begin_enter", "charge", "begin_exit", "deliver"]
    assert payment.status is PaymentStatus.SUCCEEDED
    assert payment.processed_at is not None


@pytest.mark.asyncio
async def test_process_propagates_gateway_error(
    session: AsyncMock,
    gateway: AsyncMock,
    notifier: AsyncMock,
    call_log: list[str],
) -> None:
    """
    Исключение из gateway.charge не должно превращаться в SUCCEEDED.

    Сценарий такой: инфраструктурная ошибка (таймаут, недоступность PSP).
    Статус остаётся PENDING, исключение уходит наружу для retry через
    RabbitMQ. Webhook не отправляется, нечего уведомлять.
    """
    payment = _make_payment(status=PaymentStatus.PENDING)
    gateway.charge.side_effect = RuntimeError("psp timeout")

    service = PaymentService(session, gateway=gateway, notifier=notifier)
    _stub_get_for_update(service, payment)

    with pytest.raises(RuntimeError, match="psp timeout"):
        await service.process(payment.id)

    assert payment.status is PaymentStatus.PENDING
    assert payment.processed_at is None
    notifier.deliver.assert_not_awaited()
    assert "deliver" not in call_log
    assert "begin_enter" in call_log
