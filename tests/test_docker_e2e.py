"""Полный путь через Docker: настоящий HTTP webhook, брокер и перезапуск consumer."""

import asyncio
import json
import os
import subprocess
import threading
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from uuid import uuid4

import httpx
import pytest

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_DOCKER_E2E") != "1", reason="Запускается явно на собственном Compose-проекте"
)
WEBHOOK_PORT = int(os.getenv("E2E_WEBHOOK_PORT", "8090"))
WEBHOOK_ORIGIN = f"http://host.docker.internal:{WEBHOOK_PORT}"


@pytest.fixture
def webhook_server():
    """Принимать webhook на хосте и управлять ответом без внешних сервисов."""
    received = defaultdict(list)

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            received[self.path].append((body, self.headers["X-Webhook-ID"]))
            attempts = len(received[self.path])
            failed = self.path.startswith("/dlq/") or (
                self.path.startswith("/retry/") and attempts < 3
            )
            self.send_response(503 if failed else 204)
            self.end_headers()

        def log_message(self, *args):
            """Не засорять вывод тестов журналом HTTP сервера."""

    server = ThreadingHTTPServer(("127.0.0.1", WEBHOOK_PORT), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield received
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


async def wait_for_payment(client, payment_id, predicate):
    """Ограничить ожидание, чтобы сломанный consumer не подвешивал проверку."""
    async with asyncio.timeout(45):
        while True:
            response = await client.get(f"/api/v1/payments/{payment_id}")
            response.raise_for_status()
            payment = response.json()
            if predicate(payment):
                return payment
            await asyncio.sleep(0.1)


@pytest.fixture
async def docker_client():
    key = os.environ["E2E_API_KEY"]
    async with httpx.AsyncClient(
        base_url=os.getenv("E2E_API_URL", "http://127.0.0.1:8088"),
        headers={"X-API-Key": key},
        trust_env=False,
    ) as client:
        yield client


@pytest.mark.parametrize("route, expected_attempts", [("ok", 1), ("retry", 3), ("dlq", 3)])
async def test_docker_http_webhook(
    docker_client, docker_management, webhook_server, route, expected_attempts
):
    initial_dlq_size = 0
    if route == "dlq":
        response = await docker_management.get("/api/queues/%2F/payments.dlq")
        response.raise_for_status()
        initial_dlq_size = response.json()["messages_ready"]
    path = f"/{route}/{uuid4().hex}"
    if route == "ok":
        path += "?token=AUDIT_PRIVATE_WEBHOOK_TOKEN"
    body = {
        "amount": "10.50",
        "currency": "RUB",
        "description": "Проверка полного Docker-окружения",
        "metadata": {"scenario": route},
        "webhook_url": WEBHOOK_ORIGIN + path,
    }
    headers = {"Idempotency-Key": uuid4().hex}
    response = await docker_client.post("/api/v1/payments", headers=headers, json=body)
    assert response.status_code == 202
    payment_id = response.json()["payment_id"]
    saved = await wait_for_payment(
        docker_client,
        payment_id,
        lambda payment: payment["handled_attempt"] == expected_attempts,
    )
    assert saved["status"] in {"succeeded", "failed"}
    assert saved["processed_at"] is not None
    assert (saved["webhook_delivered_at"] is None) == (route == "dlq")
    received = webhook_server[path]
    assert len(received) == expected_attempts
    assert all(
        body["payment_id"] == payment_id and event_id == payment_id for body, event_id in received
    )
    if route == "ok":
        logs = await asyncio.create_subprocess_exec(
            "docker",
            "compose",
            "logs",
            "--no-color",
            "consumer",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout, stderr = await logs.communicate()
        assert logs.returncode == 0
        assert b"AUDIT_PRIVATE_WEBHOOK_TOKEN" not in stdout + stderr
    repeated = await docker_client.post("/api/v1/payments", headers=headers, json=body)
    assert repeated.json()["payment_id"] == payment_id
    if route == "dlq":
        async with asyncio.timeout(10):
            while True:
                response = await docker_management.get("/api/queues/%2F/payments.dlq")
                response.raise_for_status()
                if response.json()["messages_ready"] > initial_dlq_size:
                    break
                await asyncio.sleep(0.2)


@pytest.fixture
async def docker_management():
    """Наблюдать рост DLQ, не забирать сообщения, оставшиеся от предыдущей проверки."""
    async with httpx.AsyncClient(
        base_url=os.getenv("E2E_RABBITMQ_MANAGEMENT_URL", "http://127.0.0.1:15688"),
        auth=(os.environ["E2E_RABBITMQ_USER"], os.environ["E2E_RABBITMQ_PASSWORD"]),
        trust_env=False,
    ) as client:
        yield client


async def compose(*args):
    """Управлять только контейнерами этого проекта, не трогая соседние сервисы."""
    process = await asyncio.create_subprocess_exec(
        "docker", "compose", *args, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    _, stderr = await process.communicate()
    assert process.returncode == 0, stderr.decode()


async def test_api_accepts_during_broker_outage_and_consumer_restart(docker_client, webhook_server):
    path = "/ok/" + uuid4().hex
    body = {
        "amount": "7.00",
        "currency": "EUR",
        "description": "Платёж во время недоступности RabbitMQ",
        "webhook_url": WEBHOOK_ORIGIN + path,
    }
    try:
        await compose("stop", "rabbitmq")
        response = await docker_client.post(
            "/api/v1/payments", headers={"Idempotency-Key": uuid4().hex}, json=body
        )
        assert response.status_code == 202
        payment_id = response.json()["payment_id"]
        await asyncio.sleep(1)
        response = await docker_client.get(f"/api/v1/payments/{payment_id}")
        assert response.json()["status"] == "pending"
        await compose("stop", "consumer")
    finally:
        await compose("up", "-d", "--wait", "rabbitmq", "consumer")
    saved = await wait_for_payment(
        docker_client, payment_id, lambda payment: payment["webhook_delivered_at"] is not None
    )
    assert saved["handled_attempt"] == 1
    assert len(webhook_server[path]) == 1


async def test_recreated_broker_keeps_durable_messages(docker_management):
    """Том и постоянное имя узла сохраняют очередь даже после замены контейнера."""
    queue = "persistence-test-" + uuid4().hex
    path = f"/api/queues/%2F/{queue}"
    response = await docker_management.put(
        path, json={"durable": True, "arguments": {"x-queue-type": "quorum"}}
    )
    response.raise_for_status()
    try:
        response = await docker_management.post(
            "/api/exchanges/%2F/amq.default/publish",
            json={
                "properties": {"delivery_mode": 2},
                "routing_key": queue,
                "payload": "durable-sentinel",
                "payload_encoding": "string",
            },
        )
        response.raise_for_status()
        assert response.json()["routed"] is True
        await compose("up", "-d", "--no-deps", "--force-recreate", "--wait", "rabbitmq")
        response = await docker_management.post(
            path + "/get", json={"count": 1, "ackmode": "ack_requeue_false", "encoding": "auto"}
        )
        response.raise_for_status()
        assert response.json()[0]["payload"] == "durable-sentinel"
    finally:
        response = await docker_management.delete(path)
        assert response.status_code in {204, 404}
