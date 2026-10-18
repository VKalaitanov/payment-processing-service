"""Отказ подключения и ошибка SQL требуют разных ответов и безопасной диагностики."""

import json
import logging
import socket
import traceback

from asyncpg import (
    CannotConnectNowError,
    InvalidAuthorizationSpecificationError,
    InvalidCatalogNameError,
    PostgresConnectionError,
    PostgresError,
    TooManyConnectionsError,
)
from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from sqlalchemy.exc import DBAPIError, OperationalError, SQLAlchemyError
from sqlalchemy.exc import TimeoutError as PoolTimeoutError

logger = logging.getLogger(__name__)
CONNECTION_ERRORS = (
    ConnectionError,
    TimeoutError,
    socket.gaierror,
    PoolTimeoutError,
    OperationalError,
    PostgresConnectionError,
    CannotConnectNowError,
    TooManyConnectionsError,
    InvalidAuthorizationSpecificationError,
    InvalidCatalogNameError,
)
DATABASE_ERRORS = (SQLAlchemyError, PostgresError, *CONNECTION_ERRORS)


async def handle_validation_error(request: Request, error: RequestValidationError) -> Response:
    """Вернуть ошибки без исходных данных; экранирование сохраняет даже некорректный Unicode."""
    details = [
        {"type": item["type"], "loc": item["loc"], "msg": item["msg"]} for item in error.errors()
    ]
    return Response(
        content=json.dumps({"detail": details}, ensure_ascii=True),
        status_code=422,
        media_type="application/json",
    )


async def handle_database_error(request: Request, error: Exception) -> JSONResponse:
    """Отдать 503 только при сбое подключения, а ошибки SQL оставить серверными ошибками."""
    unavailable = isinstance(error, CONNECTION_ERRORS) or (
        isinstance(error, DBAPIError) and error.connection_invalidated
    )
    cause = error.orig if isinstance(error, DBAPIError) else error
    # Пишем места вызовов, но не SQL, параметры, исходные строки кода и текст исключения.
    frames = " -> ".join(
        f"{frame.filename}:{frame.lineno} ({frame.name})"
        for frame in traceback.extract_tb(error.__traceback__)
    )
    logger.error(
        "Database error=%s cause=%s sqlstate=%s frames=%s",
        type(error).__name__,
        type(cause).__name__,
        getattr(cause, "sqlstate", None),
        frames,
    )
    status = 503 if unavailable else 500
    detail = "База данных недоступна" if unavailable else "Внутренняя ошибка сервера"
    return JSONResponse(status_code=status, content={"detail": detail})
