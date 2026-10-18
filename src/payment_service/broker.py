"""Долговечные очереди и подтверждённая публикация сообщений."""

import asyncio

from faststream.rabbit import Channel, RabbitBroker, RabbitExchange, RabbitQueue

from payment_service.config import Settings
from payment_service.schemas import MAX_ATTEMPTS, PaymentEvent

EVENT_EXCHANGE = RabbitExchange("payments", durable=True)
DEAD_EXCHANGE = RabbitExchange("payments.dead", durable=True)
DEAD_QUEUE = RabbitQueue("payments.dlq", durable=True, arguments={"x-queue-type": "quorum"})
PAYMENT_QUEUE = RabbitQueue(
    "payments.new",
    durable=True,
    arguments={
        "x-queue-type": "quorum",
        "x-dead-letter-exchange": DEAD_EXCHANGE.name,
        "x-dead-letter-routing-key": DEAD_QUEUE.name,
        "x-dead-letter-strategy": "at-least-once",
        "x-overflow": "reject-publish",
        "x-delivery-limit": -1,
    },
)


def create_broker(settings: Settings) -> RabbitBroker:
    """Один неподтверждённый платёж за раз; публикации ждут publisher confirm."""
    return RabbitBroker(
        settings.rabbitmq_url.get_secret_value(),
        default_channel=Channel(prefetch_count=1, publisher_confirms=True, on_return_raises=True),
        graceful_timeout=30,
    )


async def declare_topology(broker: RabbitBroker) -> None:
    """Создать DLQ до основной очереди, чтобы отклонённым сообщениям было куда попасть."""
    dead_exchange = await broker.declare_exchange(DEAD_EXCHANGE)
    dead_queue = await broker.declare_queue(DEAD_QUEUE)
    await dead_queue.bind(dead_exchange, routing_key=DEAD_QUEUE.name)
    exchange = await broker.declare_exchange(EVENT_EXCHANGE)
    queue = await broker.declare_queue(PAYMENT_QUEUE)
    await queue.bind(exchange, routing_key=PAYMENT_QUEUE.name)


async def publish_failure_without_database(
    broker: RabbitBroker, event: PaymentEvent, error_code: str, retry_base_seconds: float
) -> None:
    """При отказе БД сохранить retry или DLQ в RabbitMQ до ACK исходного сообщения."""
    if event.attempt < MAX_ATTEMPTS:
        # До подтверждения новой публикации исходное сообщение остаётся в брокере без ACK.
        await asyncio.sleep(retry_base_seconds * 2 ** (event.attempt - 1))
        next_event = event.model_copy(update={"attempt": event.attempt + 1})
        payload = next_event.model_dump(mode="json")
        exchange, routing_key = EVENT_EXCHANGE, PAYMENT_QUEUE.name
    else:
        payload = {**event.model_dump(mode="json"), "error": error_code}
        exchange, routing_key = DEAD_EXCHANGE, DEAD_QUEUE.name
    await broker.publish(
        payload,
        exchange=exchange,
        routing_key=routing_key,
        message_id=f"{event.payment_id}:{payload['attempt']}",
        persist=True,
        mandatory=True,
        timeout=5,
    )
