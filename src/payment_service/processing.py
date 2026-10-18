"""Обработка оплаты, отправка webhook и сохранение повторных попыток."""

import asyncio
import random
from datetime import timedelta

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.broker import DEAD_QUEUE, PAYMENT_QUEUE
from payment_service.config import Settings
from payment_service.models import Outbox, Payment, utc_now
from payment_service.schemas import MAX_ATTEMPTS, PaymentEvent, PaymentStatus, webhook_origin


async def emulate_gateway() -> PaymentStatus:
    """Подождать 2–5 секунд: 90% оплат успешны, остальные отклонены шлюзом."""
    await asyncio.sleep(random.uniform(2, 5))
    return "succeeded" if random.random() < 0.9 else "failed"


def already_handled(payment: Payment, event: PaymentEvent) -> bool:
    """Пропустить дубль успешно завершённой или уже перенесённой попытки."""
    return payment.webhook_delivered_at is not None or payment.handled_attempt >= event.attempt


async def process_payment(
    sessions: async_sessionmaker[AsyncSession],
    client: httpx.AsyncClient,
    settings: Settings,
    event: PaymentEvent,
) -> None:
    """Сначала сохранить оплату, затем уведомить клиента, не оплачивая второй раз."""
    async with sessions() as session, session.begin():
        payment = await session.scalar(
            select(Payment).where(Payment.id == event.payment_id).with_for_update()
        )
        if payment is None:
            raise LookupError("Платёж из сообщения не найден")
        if already_handled(payment, event):
            return
        if payment.status == "pending":
            # Блокировка не даст двум копиям сообщения одновременно обратиться к шлюзу.
            payment.status = await emulate_gateway()
            payment.processed_at = utc_now()

    async with sessions() as session, session.begin():
        payment = await session.scalar(
            select(Payment).where(Payment.id == event.payment_id).with_for_update()
        )
        if payment is None:
            raise LookupError("Платёж исчез перед отправкой webhook")
        if already_handled(payment, event):
            return
        if webhook_origin(payment.webhook_url) not in settings.webhook_allowed_origins:
            raise ValueError("Адрес webhook больше не разрешён")
        payload = {
            "payment_id": str(payment.id),
            "amount": format(payment.amount, ".2f"),
            "currency": payment.currency,
            "status": payment.status,
            "description": payment.description,
            "metadata": payment.payment_metadata,
            "created_at": payment.created_at.isoformat(),
            "processed_at": payment.processed_at.isoformat(),
        }
        # Один ID во всех попытках позволяет получателю безопасно отбрасывать дубли.
        async with (
            asyncio.timeout(settings.webhook_timeout_seconds),
            client.stream(
                "POST",
                payment.webhook_url,
                json=payload,
                headers={"X-Webhook-ID": str(payment.id), "Idempotency-Key": str(payment.id)},
                timeout=settings.webhook_timeout_seconds,
                follow_redirects=False,
            ) as response,
        ):
            response.raise_for_status()
        payment.webhook_delivered_at = utc_now()
        payment.handled_attempt = event.attempt
        payment.last_error = None


def failure_code(error: Exception) -> str:
    """Вернуть полезный код ошибки без URL, SQL, паролей и данных платежа."""
    if isinstance(error, httpx.HTTPStatusError):
        return f"webhook_http_{error.response.status_code}"
    return type(error).__name__


async def record_failure(
    sessions: async_sessionmaker[AsyncSession],
    settings: Settings,
    event: PaymentEvent,
    error: Exception,
) -> None:
    """Сохранить ошибку вместе с retry или DLQ-событием, прежде чем подтвердить сообщение."""
    error_code = failure_code(error)
    async with sessions() as session, session.begin():
        payment = await session.scalar(
            select(Payment).where(Payment.id == event.payment_id).with_for_update()
        )
        if payment is not None:
            if already_handled(payment, event):
                return
            payment.handled_attempt = event.attempt
            payment.last_error = error_code
        if event.attempt < MAX_ATTEMPTS:
            retry = event.model_copy(update={"attempt": event.attempt + 1})
            session.add(
                Outbox(
                    payload=retry.model_dump(mode="json"),
                    routing_key=PAYMENT_QUEUE.name,
                    available_at=utc_now()
                    + timedelta(seconds=settings.retry_base_seconds * 2 ** (event.attempt - 1)),
                )
            )
        else:
            if payment is not None and payment.status == "pending":
                payment.status = "failed"
                payment.processed_at = utc_now()
            session.add(
                Outbox(
                    payload={**event.model_dump(mode="json"), "error": error_code},
                    routing_key=DEAD_QUEUE.name,
                )
            )
