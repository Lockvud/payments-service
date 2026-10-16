import asyncio
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import func, select

from payments.models.outbox import Outbox
from payments.models.payment import Payment

API_KEY = "test-key"
HEADERS = {"X-API-Key": API_KEY}


def _payload(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "amount": "1499.90",
        "currency": "RUB",
        "description": "Order",
        "metadata": {"user_id": 7},
        "webhook_url": "https://example.com/hook",
    }
    base.update(overrides)
    return base


async def _count_outbox(db_session: Any) -> int:
    return await db_session.scalar(select(func.count()).select_from(Outbox)) or 0


# Create / Read


@pytest.mark.integration
@pytest.mark.asyncio
async def test_create_payment_returns_202(api_client: AsyncClient) -> None:
    response = await api_client.post(
        "/api/v1/payments",
        headers={**HEADERS, "Idempotency-Key": "key-1"},
        json=_payload(),
    )
    assert response.status_code == 202, response.text
    body = response.json()
    assert body["status"] == "pending"
    assert "payment_id" in body
    assert "created_at" in body


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_payment_returns_created(api_client: AsyncClient) -> None:
    created = await api_client.post(
        "/api/v1/payments",
        headers={**HEADERS, "Idempotency-Key": "key-2"},
        json=_payload(),
    )
    assert created.status_code == 202, created.text
    payment_id = created.json()["payment_id"]

    response = await api_client.get(f"/api/v1/payments/{payment_id}", headers=HEADERS)
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["id"] == payment_id
    assert body["idempotency_key"] == "key-2"
    assert body["metadata"] == {"user_id": 7}
    assert body["status"] == "pending"
    assert body["processed_at"] is None


# Idempotency


@pytest.mark.integration
@pytest.mark.asyncio
async def test_idempotency_returns_same_payment(api_client: AsyncClient) -> None:
    headers = {**HEADERS, "Idempotency-Key": "same-key"}
    r1 = await api_client.post("/api/v1/payments", headers=headers, json=_payload())
    r2 = await api_client.post("/api/v1/payments", headers=headers, json=_payload())

    assert r1.status_code == 202, r1.text
    assert r2.status_code == 202, r2.text
    assert r1.json()["payment_id"] == r2.json()["payment_id"]
    assert r1.json()["created_at"] == r2.json()["created_at"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_idempotency_ignores_body(api_client: AsyncClient) -> None:
    """
    Повторный запрос с тем же ключом и валидным, но другим телом
    возвращает исходный платёж как есть, тело игнорируется.
    """
    headers = {**HEADERS, "Idempotency-Key": "same-key-2"}
    r1 = await api_client.post("/api/v1/payments", headers=headers, json=_payload())
    r2 = await api_client.post(
        "/api/v1/payments",
        headers=headers,
        json=_payload(amount="9999.99", currency="USD", description="different"),
    )

    assert r1.status_code == 202, r1.text
    assert r2.status_code == 202, r2.text

    # Тот же платёж: совпадают payment_id и created_at.
    assert r1.json()["payment_id"] == r2.json()["payment_id"]
    assert r1.json()["created_at"] == r2.json()["created_at"]

    # Возвращён исходный платёж, исходные amount/currency, не значения из r2.
    original = await api_client.get(f"/api/v1/payments/{r1.json()['payment_id']}", headers=HEADERS)
    assert original.status_code == 200
    assert original.json()["amount"] == "1499.90"
    assert original.json()["currency"] == "RUB"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_idempotency_does_not_create_second_outbox_event(api_client: AsyncClient, db_session: Any) -> None:
    """
    Повторный запрос с тем же ключом не создаёт второе outbox событие.

    Это ключевая гарантия идемпотентности, событие пишется только в той
    транзакции, где INSERT платежа прошёл. При повторном запросе сервис
    возвращает существующий платёж до INSERT, новая строка outbox не
    появляется.
    """
    headers = {**HEADERS, "Idempotency-Key": "outbox-once"}
    r1 = await api_client.post("/api/v1/payments", headers=headers, json=_payload())
    assert r1.status_code == 202

    # До повторного запроса, одно событие.
    assert await _count_outbox(db_session) == 1

    r2 = await api_client.post("/api/v1/payments", headers=headers, json=_payload())
    assert r2.status_code == 202
    assert r1.json()["payment_id"] == r2.json()["payment_id"]

    # После повторного, по-прежнему одно событие.
    assert await _count_outbox(db_session) == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_idempotency_invalid_body_returns_422(api_client: AsyncClient, db_session: Any) -> None:
    """
    Повторный запрос с тем же ключом и невалидным телом возвращает 422.

    Тело валидируется Pydantic до вызова обработчика, идемпотентность
    не спасает от невалидного тела.
    """
    headers = {**HEADERS, "Idempotency-Key": "invalid-body-key"}
    r1 = await api_client.post("/api/v1/payments", headers=headers, json=_payload())
    assert r1.status_code == 202

    # Невалидное тело: currency, не из enum, amount, отрицательный.
    r2 = await api_client.post(
        "/api/v1/payments",
        headers=headers,
        json=_payload(amount="-1", currency="GBP"),
    )
    assert r2.status_code == 422

    # Никаких новых outbox-событий не появилось.
    assert await _count_outbox(db_session) == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_idempotency_concurrent_requests_single_payment(api_client: AsyncClient, db_session: Any) -> None:
    """
    Два одновременных запросов с одним Idempotency-Key создают один платёж.

    Проверяет гонку, один INSERT проходит, второй получает IntegrityError
    на уникальном индексе idempotency_key и после rollback возвращает
    существующий платёж. Оба клиента получают один и тот же payment_id.
    """

    headers = {**HEADERS, "Idempotency-Key": "concurrent-key"}

    r1, r2 = await asyncio.gather(
        api_client.post("/api/v1/payments", headers=headers, json=_payload()),
        api_client.post("/api/v1/payments", headers=headers, json=_payload()),
    )

    assert r1.status_code == 202, r1.text
    assert r2.status_code == 202, r2.text
    assert r1.json()["payment_id"] == r2.json()["payment_id"]

    # В БД ровно один платёж и одно outbox-событие.
    payments_count = await db_session.scalar(select(func.count()).select_from(Payment))
    outbox_count = await db_session.scalar(select(func.count()).select_from(Outbox))
    assert payments_count == 1
    assert outbox_count == 1


# Auth
@pytest.mark.integration
@pytest.mark.asyncio
async def test_missing_api_key_returns_401(api_client: AsyncClient) -> None:
    response = await api_client.post(
        "/api/v1/payments",
        headers={"Idempotency-Key": "key-3"},
        json=_payload(),
    )
    assert response.status_code == 401
    assert response.json()["detail"] == "Missing API key"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_invalid_api_key_returns_401(api_client: AsyncClient) -> None:
    response = await api_client.post(
        "/api/v1/payments",
        headers={"X-API-Key": "wrong", "Idempotency-Key": "key-4"},
        json=_payload(),
    )
    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid API key"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_401_takes_priority_over_422(api_client: AsyncClient) -> None:
    """
    Отсутствие API-Key возвращает 401.
    """
    response = await api_client.post(
        "/api/v1/payments",
        headers={"Idempotency-Key": "key-auth-priority"},
        json=_payload(),
    )
    assert response.status_code == 401
    assert "X-API-Key" in response.headers.get("www-authenticate", "")


# Валидация запроса
@pytest.mark.integration
@pytest.mark.asyncio
async def test_missing_idempotency_key_returns_422(api_client: AsyncClient) -> None:
    response = await api_client.post(
        "/api/v1/payments",
        headers=HEADERS,
        json=_payload(),
    )
    assert response.status_code == 422


@pytest.mark.integration
@pytest.mark.asyncio
async def test_blank_idempotency_key_returns_422(api_client: AsyncClient) -> None:
    """Idempotency-Key из пробелов отклоняется strip_whitespace + min_length."""
    response = await api_client.post(
        "/api/v1/payments",
        headers={**HEADERS, "Idempotency-Key": "   "},
        json=_payload(),
    )
    assert response.status_code == 422


# Чтение
@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_unknown_payment_returns_404(api_client: AsyncClient) -> None:
    response = await api_client.get(
        "/api/v1/payments/00000000-0000-0000-0000-000000000000",
        headers=HEADERS,
    )
    assert response.status_code == 404


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_invalid_uuid_returns_422(api_client: AsyncClient) -> None:
    """Невалидный UUID отсекается FastAPI до вызова обработчика."""
    response = await api_client.get(
        "/api/v1/payments/not-a-uuid",
        headers=HEADERS,
    )
    assert response.status_code == 422
