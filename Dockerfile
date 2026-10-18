FROM ghcr.io/astral-sh/uv:0.9.26 AS uv
FROM python:3.12.15-slim
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1
# Зависимости устанавливает uv; неиспользуемый системный pip не нужен в рабочем образе.
RUN uv pip uninstall --system pip
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY src ./src
COPY migrations ./migrations
COPY alembic.ini ./
RUN uv sync --frozen --no-dev && useradd --uid 10001 --create-home payments
USER payments
CMD ["uvicorn", "payment_service.main:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
