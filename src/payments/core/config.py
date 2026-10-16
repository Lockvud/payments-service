from functools import lru_cache
from math import isfinite

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    api_key: str = Field(min_length=1)

    database_url: str = Field(min_length=1)
    rabbitmq_url: str = Field(min_length=1)

    # Outbox publisher
    outbox_poll_interval: float = Field(default=1.0, gt=0, le=60.0)
    outbox_batch_size: int = Field(default=100, ge=1, le=10_000)

    # Retry
    # Retry-стадии вычисляются из этих параметров автоматически.
    # Количество retry-очередей = process_max_attempts - 1.
    # TTL каждой очереди: min(base * multiplier ** i, cap), i = 0..N-2.
    # Пример при base=5000, multiplier=2.0, max_attempts=3: 5000, 10000.
    process_max_attempts: int = Field(default=3, ge=2, le=10)
    retry_base_delay_ms: int = Field(default=5_000, ge=100, le=3_600_000)
    retry_multiplier: float = Field(default=2.0, ge=1.0, le=10.0)
    retry_max_delay_ms: int = Field(default=300_000, ge=1_000, le=86_400_000)

    # Эмулятор платёжного шлюза
    gateway_min_latency: float = Field(default=2.0, ge=0.0, le=60.0)
    gateway_max_latency: float = Field(default=5.0, ge=0.0, le=60.0)
    gateway_success_rate: float = Field(default=0.9, ge=0.0, le=1.0)

    # Webhook
    webhook_max_attempts: int = Field(default=3, ge=1, le=10)
    webhook_base_delay: float = Field(default=0.5, gt=0, le=60.0)
    webhook_timeout: float = Field(default=10.0, gt=0, le=300.0)

    # False это защита от SSRF. True только для локальной
    # разработки, когда webhook приёмник поднят на localhost.
    webhook_allow_private_hosts: bool = True

    @model_validator(mode="after")
    def _check_invariants(self) -> Settings:
        """
        Кросс полевые проверки. Pydantic Field валидирует поля по отдельности,
        но не ловит несовместимые комбинации. Здесь это инварианты.
        """
        # Retry: базовая задержка не должна превышать потолок, иначе
        # min(base * m**i, cap) вырождается в константу cap.
        if self.retry_base_delay_ms > self.retry_max_delay_ms:
            raise ValueError(
                f"retry_base_delay_ms ({self.retry_base_delay_ms}) must be "
                f"<= retry_max_delay_ms ({self.retry_max_delay_ms})"
            )

        # Эмулятор шлюза: min <= max, иначе random.uniform бросит ValueError.
        if self.gateway_min_latency > self.gateway_max_latency:
            raise ValueError(
                f"gateway_min_latency ({self.gateway_min_latency}) must be "
                f"<= gateway_max_latency ({self.gateway_max_latency})"
            )

        # Защита от NaN и inf: float-поля Pydantic по умолчанию их допускают,
        # если явно не включён strict-режим для специальных значений.
        for name in (
            "outbox_poll_interval",
            "retry_multiplier",
            "gateway_min_latency",
            "gateway_max_latency",
            "gateway_success_rate",
            "webhook_base_delay",
            "webhook_timeout",
        ):
            value = getattr(self, name)
            if not isfinite(value):
                raise ValueError(f"{name} must be a finite number, got {value}")

        return self


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
