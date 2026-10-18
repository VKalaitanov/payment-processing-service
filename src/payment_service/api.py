"""HTTP-маршруты: проверка запроса и вызов операций над платежами."""

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.responses import HTMLResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.auth import api_key_header
from payment_service.config import Settings
from payment_service.dependencies import get_session, get_settings
from payment_service.payments import IdempotencyConflict, create_payment, get_payment
from payment_service.schemas import PaymentAccepted, PaymentCreate, PaymentDetails, webhook_origin

router = APIRouter(dependencies=[Depends(api_key_header)])


@router.get("/health")
async def health(session: Annotated[AsyncSession, Depends(get_session)]) -> dict[str, str]:
    """Готовность API зависит от БД, но не от доступности RabbitMQ."""
    await session.execute(text("SELECT 1"))
    return {"status": "ok"}


@router.get("/openapi.json", include_in_schema=False)
async def openapi(request: Request) -> dict:
    """Вернуть схему после проверки ключа или входа в документацию через браузер."""
    return request.app.openapi()


@router.get("/docs", include_in_schema=False, response_class=HTMLResponse)
async def swagger(request: Request) -> HTMLResponse:
    """Открыть Swagger; для выполнения запросов ключ вводится через Authorize."""
    return get_swagger_ui_html(
        openapi_url=f"{request.scope.get('root_path', '')}/openapi.json",
        title="Payment API - Swagger",
        swagger_ui_parameters={"validatorUrl": None, "withCredentials": True},
    )


@router.post("/api/v1/payments", status_code=202, response_model=PaymentAccepted)
async def post_payment(
    data: PaymentCreate,
    session: Annotated[AsyncSession, Depends(get_session)],
    settings: Annotated[Settings, Depends(get_settings)],
    idempotency_key: Annotated[
        str,
        Header(alias="Idempotency-Key", min_length=1, max_length=255, pattern=r"^[!-~]+$"),
    ],
) -> PaymentAccepted:
    """Принять платёж или вернуть ранее созданный для того же запроса."""
    if webhook_origin(str(data.webhook_url)) not in settings.webhook_allowed_origins:
        raise HTTPException(422, "Адрес webhook не входит в WEBHOOK_ALLOWED_ORIGINS")
    try:
        payment = await create_payment(session, data, idempotency_key)
    except IdempotencyConflict as error:
        raise HTTPException(409, "Idempotency-Key уже использован с другими данными") from error
    return PaymentAccepted(
        payment_id=payment.id, status=payment.status, created_at=payment.created_at
    )


@router.get("/api/v1/payments/{payment_id}", response_model=PaymentDetails)
async def read_payment(
    payment_id: UUID, session: Annotated[AsyncSession, Depends(get_session)]
) -> PaymentDetails:
    """Вернуть результат оплаты и состояние доставки webhook."""
    payment = await get_payment(session, payment_id)
    if payment is None:
        raise HTTPException(404, "Платёж не найден")
    return PaymentDetails(
        payment_id=payment.id,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=payment.payment_metadata,
        status=payment.status,
        idempotency_key=payment.idempotency_key,
        webhook_url=payment.webhook_url,
        created_at=payment.created_at,
        processed_at=payment.processed_at,
        webhook_delivered_at=payment.webhook_delivered_at,
        handled_attempt=payment.handled_attempt,
        last_error=payment.last_error,
    )
