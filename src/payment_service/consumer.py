"""Единственный обработчик RabbitMQ делает оплату, webhook и планирует retry."""

import asyncio
import logging
from contextlib import suppress

import httpx
from faststream import AckPolicy, FastStream
from faststream.rabbit import RabbitBroker
from faststream.rabbit.annotations import RabbitMessage
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.broker import (
    EVENT_EXCHANGE,
    PAYMENT_QUEUE,
    create_broker,
    declare_topology,
    publish_failure_without_database,
)
from payment_service.config import Settings
from payment_service.database import create_database
from payment_service.outbox import run_outbox
from payment_service.processing import failure_code, process_payment, record_failure
from payment_service.schemas import MAX_ATTEMPTS, PaymentEvent

logger = logging.getLogger(__name__)


async def handle_payment(
    message: RabbitMessage,
    sessions: async_sessionmaker[AsyncSession],
    client: httpx.AsyncClient,
    settings: Settings,
    broker: RabbitBroker,
) -> None:
    """Подтвердить сообщение только после результата или надёжной передачи следующей попытки."""
    try:
        event = PaymentEvent.model_validate_json(message.body)
        # В RabbitMQ 4.3 NACK учитывает x-acquired-count, старые версии дают x-delivery-count.
        redeliveries = message.headers.get(
            "x-acquired-count", message.headers.get("x-delivery-count", 0)
        )
        if type(redeliveries) is not int or redeliveries < 0:
            raise ValueError("Неверный счётчик повторной доставки")
    except (ValidationError, ValueError):
        logger.warning("Invalid payment event rejected to DLQ")
        await message.reject(requeue=False)
        return
    attempt = event.attempt + redeliveries
    if attempt > MAX_ATTEMPTS:
        logger.warning("Payment %s redelivery limit exceeded", event.payment_id)
        await message.reject(requeue=False)
        return
    retry_event = event.model_copy(update={"attempt": attempt})
    try:
        # Дубль исходной попытки не должен запускать уже запланированный retry раньше срока.
        await process_payment(sessions, client, settings, event)
    except Exception as error:
        logger.warning(
            "Payment %s attempt %s failed: %s", event.payment_id, attempt, failure_code(error)
        )
        try:
            await record_failure(sessions, settings, retry_event, error)
        except Exception as storage_error:
            logger.error("Cannot save retry: %s", type(storage_error).__name__)
            try:
                await publish_failure_without_database(
                    broker, retry_event, failure_code(error), settings.retry_base_seconds
                )
            except Exception as transport_error:
                logger.error("Cannot publish retry: %s", type(transport_error).__name__)
                # RabbitMQ сохраняет число redelivery при возврате или разрыве соединения.
                await message.nack(requeue=True)
                return
    await message.ack()


def create_consumer(settings: Settings | None = None) -> FastStream:
    """Создать consumer и фоновый публикатор outbox с общим жизненным циклом."""
    settings = settings or Settings()
    broker = create_broker(settings)
    app = FastStream(broker)
    engine, sessions = create_database(settings.database_url.get_secret_value())
    client = httpx.AsyncClient(trust_env=False, follow_redirects=False)
    outbox_task: asyncio.Task | None = None

    @broker.subscriber(PAYMENT_QUEUE, EVENT_EXCHANGE, ack_policy=AckPolicy.MANUAL)
    async def consume(message: RabbitMessage) -> None:
        """ACK допустим только после webhook или сохранения следующего события в БД."""
        await handle_payment(message, sessions, client, settings, broker)

    @app.on_startup
    async def prepare() -> None:
        """Подключиться и создать очереди до запуска потребления сообщений."""
        await broker.connect()
        await declare_topology(broker)

    @app.after_startup
    async def start_outbox() -> None:
        """Публикация outbox не блокирует обработчик очереди."""
        nonlocal outbox_task
        outbox_task = asyncio.create_task(
            run_outbox(sessions, broker, settings.outbox_poll_seconds), name="outbox"
        )

    @app.on_shutdown
    async def stop_outbox() -> None:
        """При остановке незавершённая транзакция outbox откатится."""
        if outbox_task is not None:
            outbox_task.cancel()
            with suppress(asyncio.CancelledError):
                await outbox_task

    @app.after_shutdown
    async def cleanup() -> None:
        """Закрыть HTTP и БД после завершения активного обработчика."""
        await client.aclose()
        await engine.dispose()

    return app


def main() -> None:
    """Запустить consumer; FastStream обрабатывает сигналы завершения процесса."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    # В URL webhook могут быть секреты; штатный INFO-журнал httpx печатает URL целиком.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    asyncio.run(create_consumer().run())


if __name__ == "__main__":
    main()
