"""Проверка входных данных и форматы ответов сервиса."""

import json
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any, Literal
from urllib.parse import urlsplit
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator

Currency = Literal["RUB", "USD", "EUR"]
PaymentStatus = Literal["pending", "succeeded", "failed"]
MAX_ATTEMPTS = 3


def webhook_origin(url: str) -> str:
    """Получить адрес сервера и отклонить URL с паролем, фрагментом или неверным портом."""
    parsed = urlsplit(url)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        raise ValueError("Нужен HTTP(S) URL без логина, пароля и фрагмента")
    port = parsed.port
    host = parsed.hostname.lower()
    if ":" in host:
        host = f"[{host}]"
    suffix = f":{port}" if port and port != {"http": 80, "https": 443}[parsed.scheme] else ""
    return f"{parsed.scheme}://{host}{suffix}"


class PaymentCreate(BaseModel):
    """Сумма содержит не более двух знаков после запятой и никогда не округляется молча."""

    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": [
                {
                    "amount": "1200.50",
                    "currency": "RUB",
                    "description": "Оплата заказа",
                    "metadata": {"order_id": "order-42"},
                    "webhook_url": "http://host.docker.internal:8090/payments/webhook",
                }
            ]
        },
    )

    amount: Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2, allow_inf_nan=False)]
    currency: Currency
    description: str = Field(min_length=1, max_length=1000)
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: HttpUrl = Field(max_length=2048)

    @field_validator("amount", mode="before", json_schema_input_type=str)
    @classmethod
    def require_exact_amount(cls, value: Any) -> Any:
        """JSON-число уже могло потерять копейки при разборе; принимаем строку или Decimal."""
        if not isinstance(value, (str, Decimal)):
            raise ValueError('Передайте сумму строкой, например "1200.50"')
        return value

    @field_validator("description")
    @classmethod
    def meaningful_description(cls, value: str) -> str:
        """Описание из одних пробелов не помогает понять платёж."""
        if not value.strip() or "\x00" in value:
            raise ValueError("Описание не должно быть пустым")
        try:
            value.encode("utf-8")
        except UnicodeError as error:
            raise ValueError("Описание должно содержать корректный текст UTF-8") from error
        return value

    @field_validator("metadata")
    @classmethod
    def validate_metadata(cls, value: dict[str, Any]) -> dict[str, Any]:
        """Метаданные должны помещаться в 16 КиБ и содержать корректный JSON."""
        try:
            encoded = json.dumps(value, ensure_ascii=False, allow_nan=False).encode()
        except (ValueError, TypeError, RecursionError) as error:
            raise ValueError("Метаданные должны содержать корректный JSON") from error
        if len(encoded) > 16_384:
            raise ValueError("Метаданные не должны превышать 16 КиБ")
        # Убираем экранированные слеши, чтобы отличать нулевой символ от текста «\u0000».
        if b"\\u0000" in encoded.replace(b"\\\\", b""):
            raise ValueError("Метаданные не должны содержать нулевой символ")
        return value

    @field_validator("webhook_url")
    @classmethod
    def validate_webhook_url(cls, value: HttpUrl) -> HttpUrl:
        """Не разрешать спрятанные в URL учётные данные и фрагмент."""
        webhook_origin(str(value))
        return value


class PaymentAccepted(BaseModel):
    """Короткий ответ на создание или повтор того же запроса."""

    payment_id: UUID
    status: PaymentStatus
    created_at: datetime


class PaymentDetails(PaymentAccepted):
    """Поля платежа и отдельные сведения о доставке уведомления."""

    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    idempotency_key: str
    webhook_url: str
    processed_at: datetime | None
    webhook_delivered_at: datetime | None
    handled_attempt: int
    last_error: str | None


class PaymentEvent(BaseModel):
    """Номер попытки передаётся с сообщением и сохраняется при перезапуске consumer."""

    model_config = ConfigDict(extra="forbid")

    payment_id: UUID
    attempt: int = Field(default=1, ge=1, le=MAX_ATTEMPTS)
