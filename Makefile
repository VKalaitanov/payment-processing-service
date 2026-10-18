.DEFAULT_GOAL := help

.PHONY: help up webhook stop restart down status logs

help:
	@echo "make up      - собрать и запустить весь сервис, дождаться готовности"
	@echo "make webhook - запустить тестовый приёмник на порту 8090 (нужен uv)"
	@echo "make stop    - остановить контейнеры, сохранив данные"
	@echo "make restart - остановить и снова запустить весь сервис"
	@echo "make down    - удалить контейнеры и сеть, сохранив тома с данными"
	@echo "make status  - показать состояние всех контейнеров"
	@echo "make logs    - смотреть логи API и consumer (Ctrl+C для выхода)"

# Существующий .env не перезаписывается, даже если .env.example обновился.
.env:
	cp -n .env.example .env
	chmod 600 .env

up: .env
	docker compose up -d --build --wait

webhook:
	uv run --frozen uvicorn --app-dir examples webhook:app --host 0.0.0.0 --port 8090

stop:
	docker compose stop

# Последовательные вызовы сохраняют порядок даже при make -j.
restart:
	$(MAKE) stop
	$(MAKE) up

down:
	docker compose down

status:
	docker compose ps -a

logs:
	docker compose logs -f --tail=100 api consumer
