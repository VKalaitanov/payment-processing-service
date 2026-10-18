"""HTTP-зависимости отделены от подключения к БД и сборки приложения."""

from collections.abc import AsyncIterator

from fastapi import Request
from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.config import Settings


def get_settings(request: Request) -> Settings:
    """Вернуть настройки именно этого экземпляра приложения."""
    return request.app.state.settings


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    """Закрыть сессию даже при ошибке или отмене HTTP-запроса."""
    async with request.app.state.sessions() as session:
        yield session
