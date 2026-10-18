"""Проверки недоверенного JSON, размера запроса и адресов webhook."""

import json
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest

from payment_service import api
from payment_service.dependencies import get_session
from payment_service.main import create_app
from payment_service.payments import create_payment
from payment_service.processing import process_payment
from payment_service.schemas import PaymentCreate, PaymentEvent


@pytest.fixture
async def secured_client(settings, monkeypatch):
    """Проверять HTTP до записи в БД и замечать необработанные исключения."""
    app = create_app(settings)

    async def session_override():
        yield AsyncMock()

    app.dependency_overrides[get_session] = session_override
    create = AsyncMock(
        return_value=SimpleNamespace(id=uuid4(), status="pending", created_at=datetime.now(UTC))
    )
    monkeypatch.setattr(api, "create_payment", create)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app),
        base_url="http://test",
        headers={"X-API-Key": settings.api_key.get_secret_value(), "Idempotency-Key": "security"},
    ) as client:
        yield client, create


async def test_invalid_json_values_return_safe_validation_error(secured_client, payment_body):
    client, create = secured_client
    metadata = {"bad": float("nan"), "sensitive": "PRIVATE_BODY_MARKER"}
    response = await client.post(
        "/api/v1/payments",
        content=json.dumps({**payment_body, "metadata": metadata}),
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 422
    assert "PRIVATE_BODY_MARKER" not in response.text
    assert response.json()["detail"]
    assert all(set(item) == {"loc", "msg", "type"} for item in response.json()["detail"])
    create.assert_not_awaited()


async def test_stream_limit_does_not_trust_content_length(secured_client):
    client, create = secured_client

    async def body():
        for _ in range(9):
            yield b"x" * 8192

    headers = {"Content-Type": "application/json", "Content-Length": "1"}
    response = await client.post("/api/v1/payments", content=body(), headers=headers)
    assert response.status_code == 413
    create.assert_not_awaited()


@pytest.mark.integration
async def test_webhook_redirect_cannot_reach_another_origin(database, payment_body, monkeypatch):
    """Не следовать перенаправлениям webhook и не передавать получателю API-ключ."""
    from payment_service import processing

    settings, _, sessions = database
    async with sessions() as session:
        payment = await create_payment(session, PaymentCreate(**payment_body), uuid4().hex)
    monkeypatch.setattr(processing, "emulate_gateway", AsyncMock(return_value="succeeded"))
    requests = []

    def redirect(request):
        requests.append(request)
        return httpx.Response(302, headers={"Location": "http://169.254.169.254/latest/meta-data/"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(redirect)) as client:
        with pytest.raises(httpx.HTTPStatusError):
            await process_payment(sessions, client, settings, PaymentEvent(payment_id=payment.id))
    assert len(requests) == 1
    assert requests[0].url.host == "client.example"
    assert "X-API-Key" not in requests[0].headers
    assert requests[0].headers["X-Webhook-ID"] == str(payment.id)
