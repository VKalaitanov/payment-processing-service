"""Интеграционные тесты создают собственные БД и vhost, затем удаляют только их."""

import asyncio
import os
import sys
from urllib.parse import quote
from uuid import uuid4

import asyncpg
import httpx
import pytest
from sqlalchemy.engine import make_url

from payment_service.broker import create_broker, declare_topology
from payment_service.config import Settings
from payment_service.database import create_database


@pytest.fixture
def settings() -> Settings:
    """Тестовые секреты не совпадают с настройками работающего приложения."""
    return Settings(
        _env_file=None,
        api_key="test-api-key-long-enough",
        database_url="postgresql+asyncpg://payments:local-postgres-password@localhost:55488/payments",
        rabbitmq_url="amqp://payments:local-rabbitmq-password@localhost:56788/",
        webhook_allowed_origins=["https://client.example"],
        retry_base_seconds=0.05,
        outbox_poll_seconds=0.02,
    )


@pytest.fixture
def payment_body() -> dict:
    """Один валидный запрос, который отдельные тесты меняют в нужном месте."""
    return {
        "amount": "1200.50",
        "currency": "RUB",
        "description": "Оплата заказа",
        "metadata": {"order_id": "order-42"},
        "webhook_url": "https://client.example/payments/webhook",
    }


@pytest.fixture
async def database(settings):
    """Применить настоящие миграции в новой пустой БД, не очищая чужие таблицы."""
    base_url = os.getenv("TEST_DATABASE_URL")
    if not base_url:
        pytest.skip("Задайте TEST_DATABASE_URL для интеграционных тестов")
    url = make_url(base_url)
    admin = await asyncpg.connect(url.set(drivername="postgresql").render_as_string(False))
    name = "payments_test_" + uuid4().hex
    await admin.execute(f'CREATE DATABASE "{name}"')
    test_url = url.set(database=name).render_as_string(False)
    settings = settings.model_copy(
        update={"database_url": settings.database_url.__class__(test_url)}
    )
    engine, sessions = create_database(test_url)
    try:
        env = {
            **os.environ,
            "DATABASE_URL": test_url,
            "API_KEY": settings.api_key.get_secret_value(),
            "RABBITMQ_URL": settings.rabbitmq_url.get_secret_value(),
            "WEBHOOK_ALLOWED_ORIGINS": '["https://client.example"]',
        }
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "alembic",
            "upgrade",
            "head",
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await process.communicate()
        assert process.returncode == 0, stderr.decode()
        yield settings, engine, sessions
    finally:
        await engine.dispose()
        await admin.execute(f'DROP DATABASE "{name}" WITH (FORCE)')
        await admin.close()


@pytest.fixture
async def real_broker(settings):
    """Изолировать очереди отдельным vhost, чтобы тесты не потребляли чужие сообщения."""
    broker_url = os.getenv("TEST_RABBITMQ_URL")
    management_url = os.getenv("TEST_RABBITMQ_MANAGEMENT_URL")
    if not broker_url or not management_url:
        pytest.skip("Задайте TEST_RABBITMQ_URL и TEST_RABBITMQ_MANAGEMENT_URL")
    url = make_url(broker_url)
    vhost = "payments_test_" + uuid4().hex
    async with httpx.AsyncClient(
        base_url=management_url, auth=(url.username, url.password), trust_env=False
    ) as management:
        response = await management.put(f"/api/vhosts/{vhost}", json={})
        response.raise_for_status()
        try:
            response = await management.put(
                f"/api/permissions/{vhost}/{quote(url.username, safe='')}",
                json={"configure": ".*", "write": ".*", "read": ".*"},
            )
            response.raise_for_status()
            settings = settings.model_copy(
                update={
                    "rabbitmq_url": settings.rabbitmq_url.__class__(
                        url.set(database=vhost).render_as_string(False)
                    )
                }
            )
            async with create_broker(settings) as broker:
                await declare_topology(broker)
                yield settings, broker
        finally:
            response = await management.delete(f"/api/vhosts/{vhost}")
            response.raise_for_status()
