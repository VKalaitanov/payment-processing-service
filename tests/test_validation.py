"""Быстрые проверки денег, входных данных, идемпотентности и аутентификации."""

import httpx
import pytest
from pydantic import ValidationError

from payment_service.main import create_app
from payment_service.payments import request_hash
from payment_service.schemas import PaymentCreate


@pytest.mark.parametrize("amount", ["0", "0.001", "10000000000000000"])
def test_invalid_money(payment_body, amount):
    with pytest.raises(ValidationError):
        PaymentCreate(**{**payment_body, "amount": amount})


def test_hash_normalizes_money_and_json_order(payment_body):
    first = PaymentCreate(**{**payment_body, "amount": "10", "metadata": {"a": 1, "b": 2}})
    second = PaymentCreate(**{**payment_body, "amount": "10.00", "metadata": {"b": 2, "a": 1}})
    assert request_hash(first) == request_hash(second)
    boolean = PaymentCreate(**{**payment_body, "amount": "10", "metadata": {"a": True, "b": 2}})
    assert request_hash(first) != request_hash(boolean)


async def test_documentation_accepts_browser_login(settings):
    """Swagger требует пароль, а схема описывает ключ API и точную сумму строкой."""
    app = create_app(settings)
    key = settings.api_key.get_secret_value()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        challenge = await client.get("/docs")
        assert challenge.status_code == 401
        assert challenge.headers["WWW-Authenticate"] == 'Basic realm="Payment API docs"'
        assert (await client.get("/docs", auth=("docs", "wrong"))).status_code == 401
        response = await client.get("/docs", auth=("docs", key))
        assert response.status_code == 200
        assert "SwaggerUIBundle" in response.text
        assert "url: '/openapi.json'" in response.text
        assert key not in response.text
        assert (await client.get("/openapi.json")).status_code == 401
        schema_response = await client.get("/openapi.json", auth=("docs", key))
        assert schema_response.status_code == 200
        schema = schema_response.json()
        assert schema["components"]["securitySchemes"]["APIKeyHeader"]["name"] == "X-API-Key"
        payment_schema = schema["components"]["schemas"]["PaymentCreate"]
        assert payment_schema["properties"]["amount"]["type"] == "string"
        PaymentCreate.model_validate(payment_schema["examples"][0])
        assert (await client.get("/health", auth=("docs", key))).status_code == 401
