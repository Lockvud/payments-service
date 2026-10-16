from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from payments.models.outbox import Outbox


class OutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(
        self,
        *,
        aggregate_id: UUID,
        event_type: str,
        payload: dict[str, Any],
    ) -> Outbox:
        entry = Outbox(
            aggregate_id=aggregate_id,
            event_type=event_type,
            payload=payload,
        )
        self._session.add(entry)
        await self._session.flush()
        return entry
