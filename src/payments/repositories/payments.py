from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from payments.models.payment import Payment


class PaymentRepository:
    """Доступ к таблице payments. Никакой бизнес-логики."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, payment_id: UUID) -> Payment | None:
        return await self._session.get(Payment, payment_id)

    async def get_for_update(self, payment_id: UUID) -> Payment | None:
        """
        Блокирующее чтение для обработки платежа.

        SELECT FOR UPDATE блокирует конкурентную обработку той же строки
        до завершения текущей транзакции.

        После commit первого consumer второй получит актуальный статус
        платежа и сможет пропустить повторный вызов шлюза.

        Блокировка удерживается до конца транзакции, поэтому вызов шлюза
        происходит в том же транзакционном контексте.
        """
        stmt = select(Payment).where(Payment.id == payment_id).with_for_update()
        result = await self._session.scalars(stmt)
        return result.first()

    async def get_by_idempotency_key(self, key: str) -> Payment | None:
        stmt = select(Payment).where(Payment.idempotency_key == key)
        result = await self._session.scalars(stmt)
        return result.first()

    async def add(self, payment: Payment) -> Payment:
        self._session.add(payment)
        await self._session.flush()
        return payment
