"""Таблицы платежей и событий, которые ещё нужно доставить в RabbitMQ."""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import CheckConstraint, DateTime, Index, Integer, Numeric, String, func, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def utc_now() -> datetime:
    """Вернуть текущее время с явным часовым поясом UTC."""
    return datetime.now(UTC)


class Base(DeclarativeBase):
    """Общая база моделей SQLAlchemy."""


class Payment(Base):
    """Результат оплаты хранится отдельно от результата доставки webhook."""

    __tablename__ = "payments"
    __table_args__ = (
        CheckConstraint("amount > 0", name="ck_payments_amount"),
        CheckConstraint("currency IN ('RUB', 'USD', 'EUR')", name="ck_payments_currency"),
        CheckConstraint("status IN ('pending', 'succeeded', 'failed')", name="ck_payments_status"),
        CheckConstraint("handled_attempt BETWEEN 0 AND 3", name="ck_payments_attempt"),
        CheckConstraint(
            "(status = 'pending' AND processed_at IS NULL) OR "
            "(status != 'pending' AND processed_at IS NOT NULL)",
            name="ck_payments_processed_at",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    currency: Mapped[str] = mapped_column(String(3))
    description: Mapped[str] = mapped_column(String(1000))
    # metadata занято SQLAlchemy, поэтому Python-атрибут называется иначе.
    payment_metadata: Mapped[dict[str, Any]] = mapped_column("metadata", JSONB)
    status: Mapped[str] = mapped_column(String(16), default="pending", server_default="pending")
    idempotency_key: Mapped[str] = mapped_column(String(255), unique=True)
    request_hash: Mapped[str] = mapped_column(String(64))
    webhook_url: Mapped[str] = mapped_column(String(2048))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, server_default=func.now()
    )
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    webhook_delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    handled_attempt: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    last_error: Mapped[str | None] = mapped_column(String(255))


class Outbox(Base):
    """Событие удалять не нужно: published_at показывает подтверждённую публикацию."""

    __tablename__ = "outbox"
    __table_args__ = (
        Index("ix_outbox_ready", "available_at", postgresql_where=text("published_at IS NULL")),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    routing_key: Mapped[str] = mapped_column(String(64), default="payments.new")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, server_default=func.now()
    )
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, server_default=func.now()
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
