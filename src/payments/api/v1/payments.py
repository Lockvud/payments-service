from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, status
from pydantic import StringConstraints

from payments.api.dependencies import (
    PaymentServiceDep,
    require_api_key,
)
from payments.models.payment import Payment
from payments.schemas.payment import PaymentCreate, PaymentCreated, PaymentRead

# Idempotency-Key: обрезаем пробелы по краям, требуем непустую строку,
# ограничиваем длину полем idempotency_key в БД (String(255)).
# Внутренние пробелы сохраняются, это допустимый ключ.
IdempotencyKey = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=255),
]

IdempotencyKeyHeader = Annotated[
    IdempotencyKey,
    Header(
        alias="Idempotency-Key",
        description=(
            "Уникальный ключ для защиты от дублей. Повторный запрос с тем же "
            "ключом вернёт ранее созданный платёж с тем же payment_id. "
            "Тело повторного запроса игнорируется."
        ),
    ),
]

router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
    dependencies=[Depends(require_api_key)],
)


def _to_read(payment: Payment) -> PaymentRead:
    """
    Явный маппинг ORM к Pydantic.

    from_attributes=True здесь не используется намеренно, не потому, что
    не сработает, а потому что явный маппинг даёт контроль над тем, какие
    поля ORM попадают в публичный ответ. При добавлении новых колонок
    в Payment они не утекут в API автоматически, пока не будут явно
    добавлены здесь.

    Разные имена (payment.meta к schema.metadata) при желании решались бы
    через validation_alias, но это не тот случай, явный маппинг читается
    линейно и не требует держать в голове настройки Pydantic.
    """
    return PaymentRead(
        id=payment.id,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=payment.meta,
        status=payment.status,
        idempotency_key=payment.idempotency_key,
        webhook_url=payment.webhook_url,
        created_at=payment.created_at,
        processed_at=payment.processed_at,
    )


def _to_created(payment: Payment) -> PaymentCreated:
    return PaymentCreated(
        payment_id=payment.id,
        status=payment.status,
        created_at=payment.created_at,
    )


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentCreated,
    summary="Create a payment",
)
async def create_payment(
    payload: PaymentCreate,
    idempotency_key: IdempotencyKeyHeader,
    service: PaymentServiceDep,
) -> PaymentCreated:
    """
    Создаёт платёж и outbox-событие в одной транзакции.

    Ответ 202 Accepted, запрос принят, но списание выполняется
    асинхронно, отдельным consumer'ом, читающим события из RabbitMQ.
    Поле status в ответе отражает фактическое начальное состояние,
    pending, не succeeded.

    Идемпотентность:
    - при повторе с тем же Idempotency-Key возвращается существующий платёж
      с тем же payment_id, status, created_at;
    - тело повторного запроса игнорируется, если оно валидно, совпадение
      ключа означает "это тот же запрос", повторная валидация тела не делается;
    - гонка двух одновременных запросов разрешается на уровне БД. Второй
      получает IntegrityError на уникальном индексе idempotency_key,
      откатывает транзакцию и перечитывает существующую запись.

    Важное ограничение в том, что FastAPI валидирует PaymentCreate до вызова этой
    функции. Если тело невалидно (не тот тип, отсутствуют поля, нарушены
    ограничения) вернётся 422, независимо от того, что Idempotency-Key
    совпадает с существующим платежом. Это осознанное поведение, возврат
    существующего платежа на мусорное тело маскировал бы ошибку клиента.
    Так же работают Stripe, Adyen и Square.

    Публикация в RabbitMQ здесь не делается. API пишет только в PostgreSQL;
    отдельный процесс outbox-publisher читает pending-события и публикует их.
    """
    payment = await service.create(
        amount=payload.amount,
        currency=payload.currency,
        description=payload.description,
        metadata=payload.metadata,
        webhook_url=str(payload.webhook_url),
        idempotency_key=idempotency_key,
    )
    return _to_created(payment)


@router.get(
    "/{payment_id}",
    response_model=PaymentRead,
    summary="Get payment details",
)
async def get_payment(
    payment_id: UUID,
    service: PaymentServiceDep,
) -> PaymentRead:
    """
    Детали платежа.

    Возвращает 404, если платежа нет. Некорректный UUID отсекается
    автоматически на уровне FastAPI-валидации, 422 Unprocessable Entity.

    В ответе присутствуют idempotency_key и webhook_url эти оба поля, это
    клиентский ввод из исходного запроса, не серверные секреты. Возврат
    позволяет клиенту сверить, что его запрос был обработан с ожидаемым
    ключом и адресом webhook.
    """
    payment = await service.get(payment_id)
    if payment is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Payment not found",
        )
    return _to_read(payment)
