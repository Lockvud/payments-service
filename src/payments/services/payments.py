import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from payments.models.payment import Currency, Payment, PaymentStatus
from payments.repositories.outbox import OutboxRepository
from payments.repositories.payments import PaymentRepository
from payments.services.gateway import PaymentGateway
from payments.services.webhooks import WebhookNotifier

logger = logging.getLogger(__name__)

PAYMENT_CREATED_EVENT = "payment.created"
_IDEMPOTENCY_CONSTRAINT = "uq_payments_idempotency_key"


def _is_idempotency_violation(exc: IntegrityError) -> bool:
    """
    Проверяет, что IntegrityError вызван нарушением уникальности
    idempotency_key, а не другим ограничением (NOT NULL, CHECK, FK).

    asyncpg выставляет constraint_name в orig-ошибке. Fallback на строковый
    поиск, для случаев, когда драйвер не предоставляет атрибут.
    """
    orig = getattr(exc, "orig", None)
    if orig is None:
        return False
    constraint = getattr(orig, "constraint_name", None)
    if constraint == _IDEMPOTENCY_CONSTRAINT:
        return True
    return _IDEMPOTENCY_CONSTRAINT in str(orig)


class PaymentService:
    """
    Бизнес-логика платежа.

    create(), атомарная запись payment + outbox, коммит внутри.
    Вызывающий код не должен оборачивать в собственную транзакцию.
    get(), только чтение; process(), обработка одного платежа консьюмером.
    Использует SELECT FOR UPDATE для защиты от параллельной обработки одного платежа двумя консьюмерами.

    Контракт идемпотентности create():
    - при повторе с тем же idempotency_key возвращается ранее созданный платёж;
    - гонка двух одновременных запросов разрешается через уникальный индекс
      и IntegrityError на одном из них. Ошибка может вылететь как на flush()
      (внутри PaymentRepository.add), так и на commit(), оба случая должны
      приводить к requery, поэтому try оборачивает весь блок записи;
    - тело повторного запроса игнорируется.
    """

    def __init__(
        self,
        session: AsyncSession,
        gateway: PaymentGateway | None = None,
        notifier: WebhookNotifier | None = None,
    ) -> None:
        self._session = session
        self._payments = PaymentRepository(session)
        self._outbox = OutboxRepository(session)
        self._gateway = gateway or PaymentGateway()
        self._notifier = notifier or WebhookNotifier()

    async def get(self, payment_id: UUID) -> Payment | None:
        return await self._payments.get(payment_id)

    async def create(
        self,
        *,
        amount: Decimal,
        currency: Currency,
        description: str,
        metadata: dict[str, Any],
        webhook_url: str,
        idempotency_key: str,
    ) -> Payment:
        existing = await self._payments.get_by_idempotency_key(idempotency_key)
        if existing is not None:
            # idempotency_key в логи не пишется, клиентский ввод, потенциально
            # чувствительный. По payment_id достаточно для корреляции.
            logger.info("idempotent replay payment=%s", existing.id)
            return existing

        payment = Payment(
            id=uuid4(),
            amount=amount,
            currency=currency,
            description=description,
            meta=metadata,
            webhook_url=webhook_url,
            idempotency_key=idempotency_key,
            status=PaymentStatus.PENDING,
        )

        # try охватывает и flush() внутри add(), и commit().
        #
        # Почему не только commit: PaymentRepository.add() вызывает flush(),
        # чтобы получить payment.id для outbox. Именно flush() первым
        # обращается к уникальному индексу uq_payments_idempotency_key и
        # получает IntegrityError при гонке двух параллельных запросов.
        # Если try стоит только вокруг commit, IntegrityError из flush
        # уходит наружу как необработанный, вместо того чтобы превратиться
        # в идемпотентный ответ с существующим платежом.
        try:
            await self._payments.add(payment)
            await self._outbox.add(
                aggregate_id=payment.id,
                event_type=PAYMENT_CREATED_EVENT,
                payload={
                    "payment_id": str(payment.id),
                    "event_type": PAYMENT_CREATED_EVENT,
                    "occurred_at": datetime.now(tz=UTC).isoformat(),
                    "attempt": 1,
                },
            )
            await self._session.commit()
        except IntegrityError as exc:
            await self._session.rollback()
            if not _is_idempotency_violation(exc):
                raise

            existing = await self._payments.get_by_idempotency_key(idempotency_key)
            if existing is None:
                raise

            logger.warning("idempotency race resolved, returning payment=%s", existing.id)
            return existing

        await self._session.refresh(payment)
        return payment

    async def process(self, payment_id: UUID) -> None:
        """
        Обработка платежа. Вызывается из консьюемра.

        Порядок шагов:
        1. SELECT FOR UPDATE, блокируем строку до конца транзакции.
           Параллельный консьюмет, попытавшийся обработать тот же платёж,
           дождётся нашего коммита и увидит терминальный статус.
        2. Если PENDING, charge, терминальный статус, коммит
        3. После коммита, webhook. Ошибка доставки бросает
           WebhookDeliveryError и обрабатывается retry-логикой consumer'а.

        Исключения означают, что сообщение должно быть ретрайнуто.
        Бизнес-отказ шлюза, это не исключение, а FAILED + webhook.

        Между успешным charge() и commit()
        есть окно, в котором падение процесса приведёт к повторному вызову
        charge() при redelivery. Закрывается идемпотентностью на стороне
        реального PSP.
        """
        async with self._session.begin():
            payment = await self._payments.get_for_update(payment_id)

            if payment is None:
                logger.warning("payment=%s not found, dropping event", payment_id)
                return

            if payment.status is PaymentStatus.PENDING:
                success = await self._gateway.charge(payment.amount, payment.currency)
                payment.status = PaymentStatus.SUCCEEDED if success else PaymentStatus.FAILED
                payment.processed_at = datetime.now(tz=UTC)
            else:
                logger.info(
                    "payment=%s already terminal (%s), skipping charge",
                    payment.id,
                    payment.status.value,
                )

        await self._notifier.deliver(
            payment.webhook_url,
            {
                "payment_id": str(payment.id),
                "status": payment.status.value,
                "amount": str(payment.amount),
                "currency": payment.currency.value,
                "processed_at": (payment.processed_at.isoformat() if payment.processed_at else None),
            },
        )
