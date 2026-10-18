"""Публикатор работает внутри consumer; отдельный сервис ему не нужен."""

import asyncio
import logging
from datetime import timedelta

from faststream.rabbit import RabbitBroker
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.broker import DEAD_EXCHANGE, DEAD_QUEUE, EVENT_EXCHANGE
from payment_service.models import Outbox, utc_now

logger = logging.getLogger(__name__)


async def publish_next(
    sessions: async_sessionmaker[AsyncSession], broker: RabbitBroker, retry_seconds: float = 1
) -> bool:
    """Обработать готовую запись: подтвердить публикацию или отложить её повтор."""
    async with sessions() as session, session.begin():
        event = await session.scalar(
            select(Outbox)
            .where(Outbox.published_at.is_(None), Outbox.available_at <= func.now())
            .order_by(Outbox.available_at, Outbox.id)
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        if event is None:
            return False
        exchange = DEAD_EXCHANGE if event.routing_key == DEAD_QUEUE.name else EVENT_EXCHANGE
        try:
            await broker.publish(
                event.payload,
                exchange=exchange,
                routing_key=event.routing_key,
                message_id=str(event.id),
                persist=True,
                mandatory=True,
                timeout=5,
            )
        except Exception as error:
            # Неудачная запись не должна занимать начало очереди перед исправными событиями.
            event.available_at = utc_now() + timedelta(seconds=retry_seconds)
            logger.warning("Outbox %s publication failed: %s", event.id, type(error).__name__)
        else:
            # Сбой между confirm и commit может дать дубль; consumer умеет его пропустить.
            event.published_at = utc_now()
    return True


async def run_outbox(
    sessions: async_sessionmaker[AsyncSession], broker: RabbitBroker, poll_seconds: float
) -> None:
    """После ошибки оставить запись в БД и попробовать снова без потери события."""
    while True:
        try:
            if await publish_next(sessions, broker):
                continue
        except Exception as error:
            # Текст исключения может содержать пароль подключения или данные платежа.
            logger.warning("Outbox publication failed: %s", type(error).__name__)
        await asyncio.sleep(poll_seconds)
