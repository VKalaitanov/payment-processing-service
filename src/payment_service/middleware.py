"""Ограничение тела запроса до разбора JSON и проверки Pydantic."""

from starlette.exceptions import HTTPException
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

MAX_BODY_BYTES = 64 * 1024


class RequestBodyLimitMiddleware:
    """Проверять и объявленный размер, и фактически полученные байты, включая chunked-запросы."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        for name, value in scope["headers"]:
            if name == b"content-length":
                try:
                    length = int(value)
                    if length < 0:
                        raise ValueError
                except ValueError:
                    await JSONResponse({"detail": "Неверный Content-Length"}, 400)(
                        scope, receive, send
                    )
                    return
                if length > MAX_BODY_BYTES:
                    await JSONResponse({"detail": "Тело запроса превышает 64 КиБ"}, 413)(
                        scope, receive, send
                    )
                    return

        size = 0

        async def limited_receive() -> Message:
            nonlocal size
            message = await receive()
            if message["type"] == "http.request":
                size += len(message.get("body", b""))
                if size > MAX_BODY_BYTES:
                    raise HTTPException(413, "Тело запроса превышает 64 КиБ")
            return message

        await self.app(scope, limited_receive, send)
