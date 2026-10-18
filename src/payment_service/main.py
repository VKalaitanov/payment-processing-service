"""Сборка ASGI-приложения; сервер запускает внешний процесс Uvicorn."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError

from payment_service.api import router
from payment_service.auth import authenticate
from payment_service.config import Settings
from payment_service.database import create_database
from payment_service.errors import DATABASE_ERRORS, handle_database_error, handle_validation_error
from payment_service.middleware import RequestBodyLimitMiddleware


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Создать ресурсы при старте приложения и закрыть их при остановке."""
    engine, sessions = create_database(app.state.settings.database_url.get_secret_value())
    app.state.sessions = sessions
    try:
        yield
    finally:
        await engine.dispose()


def create_app(settings: Settings | None = None) -> FastAPI:
    """Подключить маршруты, авторизацию и обработку ошибок к независимому экземпляру API."""
    app = FastAPI(
        title="Payment Processing Service",
        version="1.0.0",
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.state.settings = settings or Settings()
    app.add_middleware(RequestBodyLimitMiddleware)
    app.middleware("http")(authenticate)
    app.add_exception_handler(RequestValidationError, handle_validation_error)
    for error_type in DATABASE_ERRORS:
        app.add_exception_handler(error_type, handle_database_error)
    app.include_router(router)
    return app
