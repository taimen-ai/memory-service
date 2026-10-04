"""Тесты параметров сессии Postgres: выключение JIT у графовых запросов (ADR-010)."""

from __future__ import annotations

import pytest
from psycopg.conninfo import conninfo_to_dict

from platform_memory.core.config import Settings
from platform_memory.core.db import session_options

pytestmark = pytest.mark.unit

_URL = "postgresql://memory@db:5432/company_brain"


def _settings(**kwargs: object) -> Settings:
    """Настройки с отключённым чтением .env — тест не зависит от окружения разработчика."""
    kwargs.setdefault("database_url", _URL)
    return Settings(_env_file=None, **kwargs)  # type: ignore[call-arg]


def test_jit_off_by_default() -> None:
    """По умолчанию сервис ходит в БД с jit=off: так повелось с AGE (ADR-010), пересмотр — замером MEM-ADR-023."""
    assert session_options(_settings()) == {"options": "-c jit=off"}


def test_jit_can_be_enabled_explicitly() -> None:
    """CB_DB_JIT=true возвращает поведение Postgres по умолчанию — параметры сессии не задаются."""
    assert session_options(_settings(db_jit=True)) == {}


def test_explicit_options_in_url_win() -> None:
    """Заданный вручную options в CB_DATABASE_URL не перетирается: настройка сессии остаётся за владельцем."""
    settings = _settings(database_url=f"{_URL}?options=-c%20statement_timeout%3D5s")
    assert session_options(settings) == {}


def test_options_value_is_valid_libpq_syntax() -> None:
    """Значение должно разбираться libpq как параметр соединения (иначе connect упадёт в рантайме)."""
    options = session_options(_settings())["options"]
    parsed = conninfo_to_dict(
        f"{_URL}?options={options.replace(' ', '%20').replace('=', '%3D', 1)}"
    )
    assert parsed["options"] == options
