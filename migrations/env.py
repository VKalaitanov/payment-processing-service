"""Alembic использует то же асинхронное подключение, что и приложение."""

import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from payment_service.config import DatabaseSettings
from payment_service.models import Base

config = context.config
if config.config_file_name:
    fileConfig(config.config_file_name)
url = DatabaseSettings().database_url.get_secret_value()


def run_migrations(connection) -> None:
    """Применить ревизии в одной транзакции PostgreSQL."""
    context.configure(connection=connection, target_metadata=Base.metadata, compare_type=True)
    with context.begin_transaction():
        context.run_migrations()


async def run_online() -> None:
    """Освободить подключение после миграции или ошибки."""
    engine = create_async_engine(url, poolclass=NullPool)
    try:
        async with engine.connect() as connection:
            await connection.run_sync(run_migrations)
    finally:
        await engine.dispose()


if context.is_offline_mode():
    context.configure(
        url=url,
        target_metadata=Base.metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()
else:
    asyncio.run(run_online())
