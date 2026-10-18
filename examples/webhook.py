"""Локальный получатель webhook для проверки первого платежа."""

from fastapi import FastAPI

from payment_service.schemas import PaymentAccepted

app = FastAPI()


@app.post("/payments/webhook", status_code=204)
async def receive_payment(notification: PaymentAccepted) -> None:
    """Показать результат в терминале и подтвердить получение кодом 204."""
    print(f"Платёж {notification.payment_id}: {notification.status}", flush=True)
