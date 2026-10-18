"""Настройки берутся из окружения; секреты не имеют скрытых значений по умолчанию."""

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettings(BaseSettings):
    """Для миграций достаточно подключения к БД, без секретов других сервисов."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: SecretStr


class Settings(DatabaseSettings):
    """Настройки приложения дополняют независимые настройки PostgreSQL."""

    api_key: SecretStr = Field(min_length=16)
    rabbitmq_url: SecretStr
    webhook_allowed_origins: list[str] = Field(min_length=1)
    webhook_timeout_seconds: float = Field(default=10, gt=0, le=60)
    retry_base_seconds: float = Field(default=1, gt=0, le=60)
    outbox_poll_seconds: float = Field(default=0.5, gt=0, le=60)

    @field_validator("webhook_allowed_origins")
    @classmethod
    def validate_origins(cls, origins: list[str]) -> list[str]:
        """Разрешённый адрес включает протокол и порт, но не путь или пароль."""
        from payment_service.schemas import webhook_origin

        for origin in origins:
            if webhook_origin(origin) != origin.rstrip("/"):
                raise ValueError("Укажите origin без пути: например, https://client.example")
        return [origin.rstrip("/") for origin in origins]
