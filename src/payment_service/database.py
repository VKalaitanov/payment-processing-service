"""Каждая операция получает свою сессию и явную транзакцию."""

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


def create_database(url: str) -> tuple[AsyncEngine, async_sessionmaker[AsyncSession]]:
    """Создать подключение с проверкой соединения перед его повторным использованием."""
    engine = create_async_engine(
        url, pool_pre_ping=True, connect_args={"timeout": 5}, pool_timeout=5
    )
    return engine, async_sessionmaker(engine, expire_on_commit=False)
