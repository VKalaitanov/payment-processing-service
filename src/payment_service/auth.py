"""Единая проверка API-ключа для маршрутов и служебных URL."""

import secrets

from fastapi import HTTPException, Request, Response
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader, HTTPBasic
from starlette.middleware.base import RequestResponseEndpoint

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
docs_basic = HTTPBasic(auto_error=False)


async def authenticate(request: Request, call_next: RequestResponseEndpoint) -> Response:
    """Проверить ключ до маршрутизации, включая запросы к неизвестным путям."""
    supplied = request.headers.get("X-API-Key", "").encode()
    expected = request.app.state.settings.api_key.get_secret_value().encode()
    if secrets.compare_digest(supplied, expected):
        return await call_next(request)

    # Браузер открывает документацию через Basic, поскольку не добавляет X-API-Key при переходе.
    if request.url.path in {"/docs", "/openapi.json"}:
        try:
            credentials = await docs_basic(request)
        except HTTPException:
            credentials = None
        if credentials is not None:
            username_matches = secrets.compare_digest(credentials.username.encode(), b"docs")
            key_matches = secrets.compare_digest(credentials.password.encode(), expected)
            if username_matches and key_matches:
                return await call_next(request)
        return JSONResponse(
            status_code=401,
            content={"detail": "Для документации используйте логин docs и пароль из API_KEY"},
            headers={
                "WWW-Authenticate": 'Basic realm="Payment API docs"',
                "Cache-Control": "no-store",
            },
        )

    return JSONResponse(status_code=401, content={"detail": "Неверный API ключ"})
