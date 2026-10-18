"""Проверки на настоящих PostgreSQL и RabbitMQ, включая гонки, retry и DLQ."""

import asyncio
import json
import os
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import httpx
import pytest
from faststream import TestApp
from pydantic import SecretStr
from sqlalchemy import func, select

from payment_service import consumer, payments, processing
from payment_service.broker import (
    DEAD_QUEUE,
    EVENT_EXCHANGE,
    PAYMENT_QUEUE,
)
from payment_service.main import create_app
from payment_service.models import Outbox, Payment, utc_now
from payment_service.outbox import publish_next
from payment_service.payments import create_payment
from payment_service.processing import process_payment, record_failure
from payment_service.schemas import PaymentCreate, PaymentEvent

pytestmark = pytest.mark.integration


async def saved_payment(sessions, body) -> Payment:
    """Создать платёж тем же способом, что и HTTP API."""
    async with sessions() as session:
        return await create_payment(session, PaymentCreate(**body), uuid4().hex)


async def counts(sessions) -> tuple[int, int]:
    """Проверять обе таблицы, чтобы замечать частично сохранённые платежи."""
    async with sessions() as session:
        return (
            await session.scalar(select(func.count()).select_from(Payment)),
            await session.scalar(select(func.count()).select_from(Outbox)),
        )


async def test_api_concurrent_idempotency_and_conflict(database, payment_body):
    settings, _, sessions = database
    app = create_app(settings)
    headers = {"X-API-Key": settings.api_key.get_secret_value(), "Idempotency-Key": "same-key"}
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client,
    ):
        responses = await asyncio.gather(
            *(
                client.post("/api/v1/payments", json=payment_body, headers=headers)
                for _ in range(12)
            )
        )
        assert all(response.status_code == 202 for response in responses)
        ids = {response.json()["payment_id"] for response in responses}
        assert len(ids) == 1
        assert await counts(sessions) == (1, 1)
        conflict = await client.post(
            "/api/v1/payments", json={**payment_body, "amount": "999"}, headers=headers
        )
        assert conflict.status_code == 409
        detail = await client.get(f"/api/v1/payments/{ids.pop()}", headers=headers)
        assert detail.status_code == 200
        assert detail.json()["amount"] == "1200.50"
        assert detail.json()["metadata"] == payment_body["metadata"]
        assert detail.json()["status"] == "pending"
        assert detail.json()["processed_at"] is None
        assert (await client.get(f"/api/v1/payments/{uuid4()}", headers=headers)).status_code == 404
        assert (await client.get("/api/v1/payments/not-uuid", headers=headers)).status_code == 422


async def test_api_validation_and_auth_do_not_write(database, payment_body):
    settings, _, sessions = database
    app = create_app(settings)
    headers = {"X-API-Key": settings.api_key.get_secret_value(), "Idempotency-Key": "valid-key"}
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client,
    ):
        assert (await client.post("/api/v1/payments", json=payment_body)).status_code == 401
        missing_key = await client.post(
            "/api/v1/payments", json=payment_body, headers={"X-API-Key": headers["X-API-Key"]}
        )
        assert missing_key.status_code == 422
        for changes in ({"amount": "0.001"}, {"webhook_url": "http://127.0.0.1/admin"}):
            response = await client.post(
                "/api/v1/payments", json={**payment_body, **changes}, headers=headers
            )
            assert response.status_code == 422
        assert await counts(sessions) == (0, 0)
        assert (await client.get("/health", headers=headers)).status_code == 200


async def test_payment_and_outbox_rollback_together(database, payment_body, monkeypatch):
    _, _, sessions = database
    monkeypatch.setattr(payments, "Outbox", Mock(side_effect=RuntimeError("event failed")))
    with pytest.raises(RuntimeError, match="event failed"):
        await saved_payment(sessions, payment_body)
    assert await counts(sessions) == (0, 0)


async def test_outbox_confirm_and_unroutable_retry(database, real_broker, payment_body):
    _, _, sessions = database
    _, broker = real_broker
    payment = await saved_payment(sessions, payment_body)
    async with sessions() as session, session.begin():
        event = await session.scalar(select(Outbox))
        event.routing_key = "unbound.queue"
    assert await publish_next(sessions, broker)
    async with sessions() as session, session.begin():
        event = await session.scalar(select(Outbox))
        assert event.published_at is None
        assert event.available_at > utc_now()
        event.routing_key = PAYMENT_QUEUE.name
        event.available_at = utc_now()
    assert await publish_next(sessions, broker)
    assert not await publish_next(sessions, broker)
    queue = await broker.declare_queue(PAYMENT_QUEUE)
    message = await queue.get(timeout=3)
    assert json.loads(message.body)["payment_id"] == str(payment.id)
    assert message.delivery_mode == 2
    await message.ack()
    async with sessions() as session:
        event = await session.scalar(select(Outbox))
        assert event.published_at is not None
        assert str(event.id) == message.message_id


async def test_crash_after_confirm_republishes_without_recharging(
    database, real_broker, payment_body, monkeypatch
):
    settings, _, sessions = database
    _, broker = real_broker
    await saved_payment(sessions, payment_body)
    publish = broker.publish

    async def crash_after_publish(*args, **kwargs):
        await publish(*args, **kwargs)
        raise asyncio.CancelledError("crash after confirm")

    with monkeypatch.context() as patch:
        patch.setattr(broker, "publish", crash_after_publish)
        with pytest.raises(asyncio.CancelledError, match="crash after confirm"):
            await publish_next(sessions, broker)
    assert await publish_next(sessions, broker)
    queue = await broker.declare_queue(PAYMENT_QUEUE)
    messages = [await queue.get(timeout=3), await queue.get(timeout=3)]
    assert messages[0].message_id == messages[1].message_id
    gateway = AsyncMock(return_value="succeeded")
    monkeypatch.setattr(processing, "emulate_gateway", gateway)
    webhook = AsyncMock(return_value=httpx.Response(204))
    async with httpx.AsyncClient(transport=httpx.MockTransport(webhook)) as client:
        for message in messages:
            await process_payment(
                sessions, client, settings, PaymentEvent.model_validate_json(message.body)
            )
            await message.ack()
    gateway.assert_awaited_once()
    webhook.assert_awaited_once()
    assert await counts(sessions) == (1, 1)


async def test_real_consumer_reaches_dlq_after_three_database_failures(real_broker, monkeypatch):
    settings, broker = real_broker
    settings = settings.model_copy(
        update={"database_url": SecretStr("postgresql+asyncpg://unused:unused@127.0.0.1:1/unused")}
    )
    process = AsyncMock(wraps=processing.process_payment)
    monkeypatch.setattr(consumer, "process_payment", process)
    app = consumer.create_consumer(settings)
    event = PaymentEvent(payment_id=uuid4())
    async with TestApp(app), asyncio.timeout(8):
        await broker.publish(
            event.model_dump(mode="json"), exchange=EVENT_EXCHANGE, queue=PAYMENT_QUEUE
        )
        queue = await broker.declare_queue(DEAD_QUEUE)
        while True:
            message = await queue.get(fail=False)
            if message is not None:
                break
            await asyncio.sleep(0.02)
        payload = json.loads(message.body)
        assert payload["payment_id"] == str(event.payment_id)
        assert payload["attempt"] == 3
        assert payload["error"] == "ConnectionRefusedError"
        await message.ack()
    assert [call.args[-1].attempt for call in process.await_args_list] == [1, 2, 3]


async def test_real_redelivery_counter_stops_nack_loop(real_broker, monkeypatch):
    settings, broker = real_broker
    process = AsyncMock(side_effect=ConnectionError("database down"))
    monkeypatch.setattr(consumer, "process_payment", process)
    record = AsyncMock(side_effect=ConnectionError())
    monkeypatch.setattr(consumer, "record_failure", record)
    monkeypatch.setattr(
        consumer, "publish_failure_without_database", AsyncMock(side_effect=ConnectionError())
    )
    app = consumer.create_consumer(settings)
    event = PaymentEvent(payment_id=uuid4())
    async with TestApp(app), asyncio.timeout(8):
        await broker.publish(
            event.model_dump(mode="json"), exchange=EVENT_EXCHANGE, queue=PAYMENT_QUEUE
        )
        queue = await broker.declare_queue(DEAD_QUEUE)
        while True:
            message = await queue.get(fail=False)
            if message is not None:
                break
            await asyncio.sleep(0.02)
        assert json.loads(message.body)["payment_id"] == str(event.payment_id)
        assert message.headers["x-death"][0]["reason"] == "rejected"
        await message.ack()
    assert process.await_count == 3
    assert [call.args[2].attempt for call in record.await_args_list] == [1, 2, 3]


async def test_webhook_has_total_timeout(database, payment_body, monkeypatch):
    settings, _, sessions = database
    settings = settings.model_copy(update={"webhook_timeout_seconds": 0.01})
    monkeypatch.setattr(processing, "emulate_gateway", AsyncMock(return_value="succeeded"))

    async def slow_webhook(request):
        await asyncio.sleep(0.1)
        return httpx.Response(204)

    payment = await saved_payment(sessions, payment_body)
    event = PaymentEvent(payment_id=payment.id)
    async with httpx.AsyncClient(transport=httpx.MockTransport(slow_webhook)) as client:
        with pytest.raises(TimeoutError) as failure:
            await process_payment(sessions, client, settings, event)
        await record_failure(sessions, settings, event, failure.value)
    async with sessions() as session:
        saved = await session.get(Payment, payment.id)
        assert saved.status == "succeeded"
        assert saved.webhook_delivered_at is None
        assert saved.last_error == "TimeoutError"
        assert saved.handled_attempt == 1


@pytest.mark.parametrize("gateway_status", ["succeeded", "failed"])
async def test_saved_gateway_result_survives_webhook_retry(
    database, payment_body, monkeypatch, gateway_status
):
    settings, _, sessions = database
    gateway = AsyncMock(return_value=gateway_status)
    monkeypatch.setattr(processing, "emulate_gateway", gateway)
    requests = []

    def webhook(request):
        requests.append(request)
        return httpx.Response(503 if len(requests) == 1 else 204)

    payment = await saved_payment(sessions, payment_body)
    first = PaymentEvent(payment_id=payment.id)
    async with httpx.AsyncClient(transport=httpx.MockTransport(webhook)) as client:
        with pytest.raises(httpx.HTTPStatusError) as failure:
            await process_payment(sessions, client, settings, first)
        await record_failure(sessions, settings, first, failure.value)
        await record_failure(sessions, settings, first, failure.value)
        await process_payment(sessions, client, settings, first)
        assert await counts(sessions) == (1, 2)
        duplicate = SimpleNamespace(
            body=first.model_dump_json().encode(),
            headers={"x-delivery-count": 1},
            ack=AsyncMock(),
            reject=AsyncMock(),
            nack=AsyncMock(),
        )
        transport = SimpleNamespace(publish=AsyncMock())
        await consumer.handle_payment(duplicate, sessions, client, settings, transport)
        duplicate.ack.assert_awaited_once()
        transport.publish.assert_not_awaited()
        assert len(requests) == 1
        async with sessions() as session:
            retry = await session.scalar(
                select(Outbox).where(Outbox.payload["attempt"].astext == "2")
            )
            assert retry.available_at > retry.created_at
            saved = await session.get(Payment, payment.id)
            assert saved.status == gateway_status
            assert saved.processed_at is not None
            assert saved.webhook_delivered_at is None
        # Новый вызов имитирует перезапуск consumer: всё состояние прочитано из БД.
        second = PaymentEvent(payment_id=payment.id, attempt=2)
        await process_payment(sessions, client, settings, second)
        await process_payment(sessions, client, settings, second)
    gateway.assert_awaited_once()
    assert len(requests) == 2
    assert requests[0].headers["X-Webhook-ID"] == requests[1].headers["X-Webhook-ID"]
    async with sessions() as session:
        saved = await session.get(Payment, payment.id)
        assert saved.status == gateway_status
        assert saved.handled_attempt == 2
        assert saved.webhook_delivered_at is not None
        assert saved.last_error is None


async def test_simultaneous_duplicate_events_charge_once(database, payment_body, monkeypatch):
    settings, _, sessions = database
    gateway = AsyncMock(return_value="succeeded")
    monkeypatch.setattr(processing, "emulate_gateway", gateway)
    webhook = AsyncMock(return_value=httpx.Response(200))
    payment = await saved_payment(sessions, payment_body)
    async with httpx.AsyncClient(transport=httpx.MockTransport(webhook)) as client:
        event = PaymentEvent(payment_id=payment.id)
        await asyncio.gather(
            *(process_payment(sessions, client, settings, event) for _ in range(6))
        )
    gateway.assert_awaited_once()
    webhook.assert_awaited_once()


async def test_processing_failure_after_three_attempts(database, payment_body, monkeypatch):
    settings, _, sessions = database
    monkeypatch.setattr(processing, "emulate_gateway", AsyncMock(side_effect=RuntimeError("down")))
    payment = await saved_payment(sessions, payment_body)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(204))
    ) as client:
        for attempt in range(1, 4):
            event = PaymentEvent(payment_id=payment.id, attempt=attempt)
            with pytest.raises(RuntimeError) as failure:
                await process_payment(sessions, client, settings, event)
            await record_failure(sessions, settings, event, failure.value)
    async with sessions() as session:
        saved = await session.get(Payment, payment.id)
        assert saved.status == "failed"
        assert saved.handled_attempt == 3
        assert saved.processed_at is not None
        dlq = await session.scalar(select(Outbox).where(Outbox.routing_key == DEAD_QUEUE.name))
        assert dlq.payload["attempt"] == 3
        assert dlq.payload["error"] == "RuntimeError"


async def test_malformed_message_goes_to_real_dlq(database, real_broker, monkeypatch):
    settings, _, _ = database
    broker_settings, broker = real_broker
    settings = settings.model_copy(update={"rabbitmq_url": broker_settings.rabbitmq_url})
    app = consumer.create_consumer(settings)
    async with TestApp(app):
        await broker.publish(
            {"payment_id": "invalid"}, exchange=EVENT_EXCHANGE, queue=PAYMENT_QUEUE
        )
        queue = await broker.declare_queue(DEAD_QUEUE)
        async with asyncio.timeout(5):
            while True:
                message = await queue.get(fail=False)
                if message is not None:
                    break
                await asyncio.sleep(0.02)
        assert json.loads(message.body) == {"payment_id": "invalid"}
        await message.ack()


async def test_migrations_match_models_and_roundtrip(database):
    settings, _, _ = database
    env = {
        **os.environ,
        "DATABASE_URL": settings.database_url.get_secret_value(),
        "API_KEY": settings.api_key.get_secret_value(),
        "RABBITMQ_URL": settings.rabbitmq_url.get_secret_value(),
        "WEBHOOK_ALLOWED_ORIGINS": '["https://client.example"]',
    }
    for args in [("check",), ("downgrade", "base"), ("upgrade", "head"), ("check",)]:
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "alembic",
            *args,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        assert process.returncode == 0, (stdout + stderr).decode()
