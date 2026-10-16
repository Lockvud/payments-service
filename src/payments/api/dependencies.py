import secrets
from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from payments.core.config import get_settings
from payments.db.session import get_session
from payments.services.payments import PaymentService


async def require_api_key(
    x_api_key: Annotated[str | None, Header(alias="X-API-Key")] = None,
) -> None:
    """
    Проверка статического API-ключа.

    Заголовок объявлен опциональным намеренно, если бы он был обязательным
    Header(), FastAPI валидировал бы его до вызова функции и возвращал
    422 при отсутствии. Семантически корректный ответ для отсутствующей
    аутентификации 401.

    secrets.compare_digest, constant-time сравнение, исключает timing-атаку.
    """
    if x_api_key is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing API key",
            headers={"WWW-Authenticate": "X-API-Key"},
        )

    expected = get_settings().api_key
    if not secrets.compare_digest(x_api_key, expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
            headers={"WWW-Authenticate": "X-API-Key"},
        )


async def get_payment_service(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> AsyncIterator[PaymentService]:
    """
    PaymentService с текущей сессией.

    Сессия не коммитит автоматически, границы транзакций определяет
    сервисный слой. Для POST это критично, PaymentService.create коммитит
    ровно один раз, атомарно сохраняя payment и outbox.
    """
    yield PaymentService(session)


PaymentServiceDep = Annotated[PaymentService, Depends(get_payment_service)]
