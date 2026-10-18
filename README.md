# Payment Processing Service

Асинхронная обработка платежей: FastAPI, PostgreSQL, RabbitMQ, Outbox, webhook и DLQ.
Платёжный шлюз эмулируется: обработка занимает 2-5 секунд, вероятность успеха - 90%.

## Запуск

Нужен запущенный Docker с Compose v2.
Выполняйте команды из корня проекта.

```sh
make up
```

Команда создаст `.env` из `.env.example`, соберёт образы и запустит все четыре сервиса.
Миграции применяются автоматически. Существующий `.env` сохраняется.

Без Make:

```sh
cp -n .env.example .env
chmod 600 .env
docker compose up -d --build --wait
```

## Проверка через Swagger

Для полной проверки сначала запустите приёмник webhook в отдельном терминале.
Нужен установленный `uv`:

```sh
make webhook
```

Оставьте этот терминал открытым. Затем:

1. Откройте http://127.0.0.1:8088/docs.
2. В окне браузера введите логин `docs`, пароль - значение `API_KEY` из `.env`.
3. В Swagger нажмите **Authorize** и введите тот же ключ.
4. Откройте `POST /api/v1/payments`, нажмите **Try it out**.
5. Введите `Idempotency-Key`, например `order-1`. Пример тела уже заполнен.
6. Нажмите **Execute**. Ожидается `202` с `payment_id`.
7. Подставьте этот ID в `GET /api/v1/payments/{payment_id}` и выполните запрос.

Сумма передаётся строкой: `"1200.50"`. Для нового платежа используйте новый `Idempotency-Key`.
Повтор того же запроса с прежним ключом возвращает прежний платёж.
Изменение данных с прежним ключом даёт `409`.
Тело запроса ограничено 64 КиБ; превышение даёт `413`.

В примере Swagger уже указан адрес
`http://host.docker.internal:8090/payments/webhook`.
Результат появится в терминале приёмника. Остановка - `Ctrl+C`.

В GET-ответе `status` станет `succeeded` или `failed`; оба результата допустимы для эмулятора.
После успешной доставки заполнится `webhook_delivered_at`.
Без приёмника доставка повторяется до трёх попыток, затем сообщение попадает в `payments.dlq`.
После сбоя webhook может повториться; получатель должен устранять дубли по `X-Webhook-ID`.

## Управление

- `make webhook` - тестовый приёмник webhook на порту 8090; остановка - `Ctrl+C`.
- `make status` - состояние контейнеров.
- `make logs` - логи API и consumer; выход - `Ctrl+C`.
- `make stop` - остановка с сохранением данных.
- `make restart` - пересборка и повторный запуск.
- `make down` - удаление контейнеров и сети с сохранением данных.
- Без Make - `docker compose ps`, `docker compose logs -f api consumer`, `docker compose stop`.

RabbitMQ: http://127.0.0.1:15688. Логин и пароль - `RABBITMQ_USER` и `RABBITMQ_PASSWORD` из `.env`.
Порты API и RabbitMQ меняются через `API_PORT` и `RABBITMQ_MANAGEMENT_PORT`.
Для другого webhook добавьте его origin в `WEBHOOK_ALLOWED_ORIGINS` и выполните `make up`.
PostgreSQL и AMQP доступны только внутри сети Compose.

## Тесты

Всего 30 проверок: 10 быстрых, 15 интеграционных и 5 сквозных через Docker.

Быстрые проверки без Docker; нужны `uv` и Python 3.12+:

```sh
uv sync --frozen
uv run ruff check .
uv run ruff format --check .
uv run pytest -q -m 'not integration'
```

Интеграционные проверки используют отдельные PostgreSQL и RabbitMQ:

```sh
docker compose -f compose.yaml -f compose.test.yaml up -d --wait postgres rabbitmq
TEST_DATABASE_URL='postgresql+asyncpg://payments:local-postgres-password@127.0.0.1:55488/payments' \
TEST_RABBITMQ_URL='amqp://payments:local-rabbitmq-password@127.0.0.1:56788/' \
TEST_RABBITMQ_MANAGEMENT_URL='http://127.0.0.1:15689' \
uv run pytest -q
docker compose -f compose.yaml -f compose.test.yaml down
```

Если изменили секреты в `.env`, замените их в тестовых URL.
Без `TEST_*` интеграционные тесты пропускаются.

Сквозные тесты запускайте на свободном тестовом окружении после `make up`.
Они временно останавливают RabbitMQ и consumer этого проекта; порт 8090 должен быть свободен.

```sh
RUN_DOCKER_E2E=1 \
E2E_API_KEY='local-payment-api-key' \
E2E_RABBITMQ_USER='payments' \
E2E_RABBITMQ_PASSWORD='local-rabbitmq-password' \
uv run pytest -q tests/test_docker_e2e.py
```

При изменённых секретах подставьте свои значения из `.env`.
