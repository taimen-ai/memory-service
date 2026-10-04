"""Интеграционные тесты на реальной БД (PostgreSQL + pgvector).

Граф — обычные таблицы (MEM-ADR-023): расширение графа и суперпользователь не нужны,
достаточно расширений ``vector``/``pg_trgm`` и роли-владельца базы. Запуск, например::

    docker run -d --name memtest-db -e POSTGRES_USER=brain -e POSTGRES_PASSWORD=brain \
      -e POSTGRES_DB=company_brain -p 127.0.0.1:5435:5432 pgvector/pgvector:pg16
    docker exec memtest-db psql -U brain -d company_brain \
      -c 'CREATE EXTENSION IF NOT EXISTS vector' -c 'CREATE EXTENSION IF NOT EXISTS pg_trgm'
    CB_TEST_DATABASE_URL=postgresql://brain:brain@127.0.0.1:5435/company_brain \
      uv run pytest tests/integration -q

Без переменной окружения весь каталог скипается — обычный `pytest tests/`
остаётся самодостаточным (инвариант проекта). Провайдеры — fake/echo: сеть
и LLM не нужны (ADR-013).
"""

from __future__ import annotations

import os
from pathlib import Path
import uuid

import pytest

from platform_memory.core.config import Settings

DB_URL = os.environ.get("CB_TEST_DATABASE_URL", "")

pytestmark = pytest.mark.integration


def pytest_collection_modifyitems(config, items):
    # Hook получает items ВСЕЙ сессии — скипаем только тесты этого каталога.
    if DB_URL:
        return
    here = str(config.rootpath / "tests" / "integration")
    skip = pytest.mark.skip(reason="CB_TEST_DATABASE_URL не задан — интеграционные тесты пропущены")
    for item in items:
        if str(item.fspath).startswith(here):
            item.add_marker(skip)
            item.add_marker(pytest.mark.integration)


def _tables(settings: Settings) -> tuple[str, ...]:
    """Все реляционные таблицы прогона (зачищаются вместе с графом)."""
    return (
        settings.chunks_table,
        settings.observations_table,
        settings.context_traces_table,
        settings.domain_packs_table,
        f"{settings.domain_packs_table}_tenant",
        settings.namespace_settings_table,
        settings.snapshots_table,
        f"{settings.snapshots_table}_items",
        f"{settings.snapshots_table}_state",
        settings.entity_embeddings_table,
    )


@pytest.fixture()
def settings() -> Settings:
    """Изолированные настройки прогона: свой граф и свои таблицы на каждый тест."""
    suffix = uuid.uuid4().hex[:8]
    return Settings(
        _env_file=None,
        database_url=DB_URL,
        graph_name=f"t_{suffix}",
        chunks_table=f"chunks_{suffix}",
        observations_table=f"obs_{suffix}",
        context_traces_table=f"traces_{suffix}",
        domain_packs_table=f"packs_{suffix}",
        namespace_settings_table=f"nssettings_{suffix}",
        snapshots_table=f"snaps_{suffix}",
        entity_embeddings_table=f"entemb_{suffix}",
        default_namespace="nexus",
        embedding_provider="fake",
        embedding_dim=64,
        llm_provider="echo",
    )


@pytest.fixture()
def stores(settings):
    """Открытые сторы на одном соединении; в конце — полная зачистка артефактов теста."""
    from platform_memory.core import db as dbmod
    from platform_memory.graph.facts import FactStore
    from platform_memory.graph.store import GraphStore
    from platform_memory.index import VectorIndex
    from platform_memory.observations.store import ObservationStore

    conn = dbmod.connect(settings, autocommit=True)
    graph = GraphStore(conn, settings.graph_name, settings.default_namespace)
    graph.ensure_schema()
    index = VectorIndex(
        conn, settings.chunks_table, settings.embedding_dim, settings.default_namespace
    )
    index.ensure_schema()
    obs = ObservationStore(conn, settings.observations_table, settings.default_namespace)
    obs.ensure_schema()
    facts = FactStore(graph)
    yield conn, graph, facts, index, obs
    try:
        dbmod.drop_graph(conn, settings.graph_name)
        with conn.cursor() as cur:
            for table in _tables(settings):
                cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
    finally:
        conn.close()


# --- фикстуры для тестов HTTP-хендлеров и ingest mini-vault (ADR-017) ---

_MINI_VAULT = Path(__file__).parent / "fixtures" / "mini_vault"


@pytest.fixture()
def test_settings(settings) -> Settings:
    """Настройки хендлер-тестов: mini-vault и слаг проекта (CB_PROJECT_VALUES) для ingest."""
    return settings.model_copy_with(vault_path=str(_MINI_VAULT), project_values="alpha")


@pytest.fixture()
def clean_db(test_settings):
    """Схема памяти, как её создал бы lifespan; в конце — зачистка графа и таблиц теста."""
    from platform_memory.core import db as dbmod
    from platform_memory.graph.store import GraphStore
    from platform_memory.index import VectorIndex

    conn = dbmod.connect(test_settings, autocommit=True)
    try:
        GraphStore(conn, test_settings.graph_name, test_settings.default_namespace).ensure_schema()
        VectorIndex(
            conn,
            test_settings.chunks_table,
            test_settings.embedding_dim,
            test_settings.default_namespace,
        ).ensure_schema()
    finally:
        conn.close()
    yield
    conn = dbmod.connect(test_settings, autocommit=True)
    try:
        dbmod.drop_graph(conn, test_settings.graph_name)
        with conn.cursor() as cur:
            for table in _tables(test_settings):
                cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
    finally:
        conn.close()
