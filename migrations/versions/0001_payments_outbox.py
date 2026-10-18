"""Создать платежи и transactional outbox."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Добавить таблицы, ограничения денег и уникальность ключа идемпотентности."""
    op.create_table(
        "payments",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("amount", sa.Numeric(18, 2), nullable=False),
        sa.Column("currency", sa.String(3), nullable=False),
        sa.Column("description", sa.String(1000), nullable=False),
        sa.Column("metadata", postgresql.JSONB(), nullable=False),
        sa.Column("status", sa.String(16), server_default="pending", nullable=False),
        sa.Column("idempotency_key", sa.String(255), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("webhook_url", sa.String(2048), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("processed_at", sa.DateTime(timezone=True)),
        sa.Column("webhook_delivered_at", sa.DateTime(timezone=True)),
        sa.Column("handled_attempt", sa.Integer(), server_default="0", nullable=False),
        sa.Column("last_error", sa.String(255)),
        sa.CheckConstraint("amount > 0", name="ck_payments_amount"),
        sa.CheckConstraint("currency IN ('RUB', 'USD', 'EUR')", name="ck_payments_currency"),
        sa.CheckConstraint(
            "status IN ('pending', 'succeeded', 'failed')", name="ck_payments_status"
        ),
        sa.CheckConstraint("handled_attempt BETWEEN 0 AND 3", name="ck_payments_attempt"),
        sa.CheckConstraint(
            "(status = 'pending' AND processed_at IS NULL) OR "
            "(status != 'pending' AND processed_at IS NOT NULL)",
            name="ck_payments_processed_at",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("idempotency_key"),
    )
    op.create_table(
        "outbox",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("routing_key", sa.String(64), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "available_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("published_at", sa.DateTime(timezone=True)),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_outbox_ready",
        "outbox",
        ["available_at"],
        postgresql_where=sa.text("published_at IS NULL"),
    )


def downgrade() -> None:
    """Удалить таблицы; откат этой первой ревизии удаляет и данные платежей."""
    op.drop_index("ix_outbox_ready", table_name="outbox")
    op.drop_table("outbox")
    op.drop_table("payments")
