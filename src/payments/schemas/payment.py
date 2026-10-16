import json
from datetime import datetime
from decimal import Decimal
from typing import Annotated
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    UrlConstraints,
    field_validator,
)
from pydantic_core import Url

from payments.models.payment import Currency, PaymentStatus

Amount = Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2)]

# HttpUrl по умолчанию max_length=2083, а колонка webhook_url String(2048).
# Без явного ограничения URL длиной 2050 символов пройдёт валидацию Pydantic
# и упадёт на INSERT с StringDataRightTruncation.
WebhookUrl = Annotated[
    Url,
    UrlConstraints(allowed_schemes=["http", "https"], max_length=2048),
]

MAX_METADATA_BYTES = 16 * 1024


def _validate_metadata_size(value: dict[str, JsonValue]) -> dict[str, JsonValue]:
    """
    Ограничивает размер metadata при сериализации в JSON.

    JsonValue гарантирует, что значение сериализуемо (никаких Decimal,
    datetime, UUID). Здесь только защита от клиент прислал 100 МБ,
    JSONB в PostgreSQL такое примет, но это создаст нагрузку на сеть,
    память и бэкапы.
    """
    encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    if len(encoded.encode("utf-8")) > MAX_METADATA_BYTES:
        raise ValueError(
            f"metadata exceeds {MAX_METADATA_BYTES} bytes when serialised (got {len(encoded.encode('utf-8'))})"
        )
    return value


class PaymentCreate(BaseModel):
    """Тело запроса POST /api/v1/payments."""

    model_config = ConfigDict(extra="forbid")

    amount: Amount
    currency: Currency
    description: str = Field(default="", max_length=1024)
    metadata: dict[str, JsonValue] = Field(default_factory=dict)
    webhook_url: WebhookUrl

    _check_metadata = field_validator("metadata")(_validate_metadata_size)


class PaymentCreated(BaseModel):
    """Ответ 202 на создание платежа."""

    payment_id: UUID
    status: PaymentStatus
    created_at: datetime


class PaymentRead(BaseModel):
    """Детальная информация о платеже."""

    id: UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, JsonValue]
    status: PaymentStatus
    idempotency_key: str
    webhook_url: WebhookUrl
    created_at: datetime
    processed_at: datetime | None
