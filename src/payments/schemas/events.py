from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

# Потолок для защиты от некорректных входных данных
# Реальный бизнес-максимум попыток, settings.process_max_attempts
# Схема гарантирует лишь, что значение в разумных пределах
MAX_ATTEMPT_CEILING = 10


class PaymentEvent(BaseModel):
    """
    Сообщение, проходящее через RabbitMQ

    attempt это номер текущей обработки, начиная с 1. Хранится в payload явно,
    чтобы не зависеть от заголовка x-death и однозначно определять момент
    перевода в DLQ. Верхняя граница (MAX_ATTEMPT_CEILING), защитный потолок
    против мусорных сообщений, а не максимум попыток он определяется
    в Settings.process_max_attempts
    """

    model_config = ConfigDict(extra="ignore", frozen=True)

    payment_id: UUID
    event_type: Literal["payment.created"] = "payment.created"
    occurred_at: datetime
    attempt: int = Field(default=1, ge=1, le=MAX_ATTEMPT_CEILING)

    def next_attempt(self) -> PaymentEvent:
        """
        Возвращает копию с attempt+1, прошедшую полную валидацию.

        model_copy(update) не валидирует обновлённые значения, поэтому
        используем model_validate при attempt=MAX_ATTEMPT_CEILING метод
        упадёт с ValidationError, а не молча создаст некорректный объект
        """
        return PaymentEvent.model_validate({**self.model_dump(), "attempt": self.attempt + 1})
