"""Создание платежа и события выполняется в одной транзакции."""

import hashlib
import json
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.models import Outbox, Payment
from payment_service.schemas import PaymentCreate, PaymentEvent


class IdempotencyConflict(Exception):
    """Ключ уже занят запросом с другими данными."""


def request_hash(data: PaymentCreate) -> str:
    """Одинаковые суммы и порядок ключей JSON должны давать один отпечаток запроса."""
    payload = data.model_dump(mode="json")
    payload["amount"] = format(data.amount, ".2f")
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


async def create_payment(
    session: AsyncSession, data: PaymentCreate, idempotency_key: str
) -> Payment:
    """Уникальный индекс защищает от дублей даже при одновременных HTTP-запросах."""
    fingerprint = request_hash(data)
    async with session.begin():
        payment = await session.scalar(
            insert(Payment)
            .values(
                amount=data.amount,
                currency=data.currency,
                description=data.description,
                payment_metadata=data.metadata,
                webhook_url=str(data.webhook_url),
                idempotency_key=idempotency_key,
                request_hash=fingerprint,
            )
            .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
            .returning(Payment)
        )
        if payment is None:
            payment = await session.scalar(
                select(Payment).where(Payment.idempotency_key == idempotency_key)
            )
            if payment is None:
                raise RuntimeError("Платёж исчез после проверки уникального ключа")
            if payment.request_hash != fingerprint:
                raise IdempotencyConflict
        else:
            event = PaymentEvent(payment_id=payment.id)
            session.add(Outbox(payload=event.model_dump(mode="json")))
    return payment


async def get_payment(session: AsyncSession, payment_id: UUID) -> Payment | None:
    """Найти платёж по UUID без изменения его состояния."""
    return await session.get(Payment, payment_id)
