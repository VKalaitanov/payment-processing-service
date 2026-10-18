"""Проверки точности денег и безопасного ответа при сбое БД."""

import json
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest

from payment_service import api
from payment_service.dependencies import get_session
from payment_service.main import create_app


@pytest.fixture
async def api_client(settings):
    """Подставить сессию, чтобы проверять HTTP-контракт без внешней БД."""
    app = create_app(settings)
    session = AsyncMock()

    async def session_override():
        yield session

    app.dependency_overrides[get_session] = session_override
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app),
        base_url="http://test",
        headers={"X-API-Key": settings.api_key.get_secret_value(), "Idempotency-Key": "review"},
    ) as client:
        yield client, session


async def test_http_string_amount_reaches_storage_exactly(api_client, payment_body, monkeypatch):
    client, _ = api_client
    amount = "9999999999999999.99"
    saved = SimpleNamespace(id=uuid4(), status="pending", created_at=datetime.now(UTC))
    create = AsyncMock(return_value=saved)
    monkeypatch.setattr(api, "create_payment", create)
    response = await client.post("/api/v1/payments", json={**payment_body, "amount": amount})
    assert response.status_code == 202
    assert create.await_args.args[1].amount == Decimal(amount)


async def test_http_numeric_money_is_rejected_before_storage(api_client, payment_body, monkeypatch):
    client, _ = api_client
    create = AsyncMock()
    monkeypatch.setattr(api, "create_payment", create)
    body = json.dumps({**payment_body, "amount": "PLACEHOLDER"}).replace(
        '"PLACEHOLDER"', "1000000000000000.11"
    )
    response = await client.post(
        "/api/v1/payments", content=body, headers={"Content-Type": "application/json"}
    )
    assert response.status_code == 422
    create.assert_not_awaited()


async def test_unavailable_database_returns_safe_error(api_client, caplog):
    client, session = api_client
    session.execute.side_effect = ConnectionRefusedError("PRIVATE_CONNECTION")
    response = await client.get("/health")
    assert response.status_code == 503
    assert response.json()["detail"] == "База данных недоступна"
    assert "ConnectionRefusedError" in caplog.text
    assert "health" in caplog.text
    assert "PRIVATE_" not in caplog.text
    assert "PRIVATE_" not in response.text
