"""Smoke-тест M1.4: агент записал decision/task → retrieval нашёл → вернул цитату.

Покрывает сквозной цикл: write_and_index_fact (origin='agent') → retrieve → hits содержат
агентский узел, sources указывают на agent:run/... (не .md-файл vault).

Требует Postgres (pgvector): CB_TEST_DATABASE_URL, см. conftest.
"""

from __future__ import annotations

import pytest

from platform_memory.retrieval import retrieve as run_retrieve
from platform_memory.retrieval import write_and_index_fact

pytestmark = pytest.mark.integration


def test_agent_decision_found_by_retrieval_with_citation(test_settings, clean_db):
    """M1.4 smoke (decision): write_fact → retrieve → hits + source с цитатой на agent:run/..."""
    # Шаг 1: агент пишет decision BIZ-901 с явным source.
    write_and_index_fact(
        test_settings,
        "BIZ-901",
        "decision",
        "Выбрали Python для бэкенда",
        properties={"rationale": "зрелая экосистема, команда знает Python"},
        run_id="smoke-run-001",
        source="agent:run/smoke-run-001",
    )

    # Шаг 2: retrieval находит записанный узел по тексту.
    result = run_retrieve(test_settings, "Python бэкенд решение", k=5)

    hit_keys = {h.node_key for h in result.hits}
    assert "BIZ-901" in hit_keys, f"decision BIZ-901 не найден в hits: {hit_keys}"

    # Шаг 3: sources содержат цитату, указывающую на агентский источник.
    source_keys = {s.node_key for s in result.sources}
    assert "BIZ-901" in source_keys, f"decision BIZ-901 не найден в sources: {source_keys}"

    decision_source = next(s for s in result.sources if s.node_key == "BIZ-901")
    assert decision_source.source_path, "source_path не должен быть пустым"
    assert "agent" in decision_source.source_path, (
        f"цитата должна указывать на агентский источник: {decision_source.source_path!r}"
    )


def test_agent_task_found_by_retrieval_with_citation(test_settings, clean_db):
    """M1.4 smoke (task): write_fact → retrieve → source_path начинается с agent:run/..."""
    write_and_index_fact(
        test_settings,
        "T-901",
        "task",
        "Настроить CI/CD пайплайн для Python проекта",
        properties={"content": "Написать GitHub Actions workflow для автодеплоя"},
        run_id="smoke-run-002",
        source="agent:run/smoke-run-002",
    )

    result = run_retrieve(test_settings, "CI CD автодеплой GitHub Actions", k=5)

    hit_keys = {h.node_key for h in result.hits}
    assert "T-901" in hit_keys, f"task T-901 не найден в hits: {hit_keys}"

    task_source = next((s for s in result.sources if s.node_key == "T-901"), None)
    assert task_source is not None, "T-901 должен быть в sources"
    assert not task_source.source_path.endswith(".md"), (
        f"агентский факт не должен ссылаться на vault .md: {task_source.source_path!r}"
    )
    assert task_source.source_path.startswith("agent:"), (
        f"source_path должен быть agent:run/...: {task_source.source_path!r}"
    )


def test_agent_fact_both_types_same_run(test_settings, clean_db):
    """M1.4 smoke (decision + task в одном run): оба найдены retrieval'ом с корректными цитатами."""
    run_id = "smoke-run-003"
    source = f"agent:run/{run_id}"

    write_and_index_fact(
        test_settings,
        "BIZ-902",
        "decision",
        "Использовать PostgreSQL как основную СУБД",
        properties={"rationale": "AGE + pgvector, уже используется в Brain"},
        run_id=run_id,
        source=source,
    )
    write_and_index_fact(
        test_settings,
        "T-902",
        "task",
        "Написать unit-тесты модуля авторизации",
        properties={"content": "Покрыть тестами login, logout, refresh токена"},
        run_id=run_id,
        source=source,
    )

    result_decision = run_retrieve(test_settings, "PostgreSQL база данных решение", k=5)
    result_task = run_retrieve(test_settings, "тесты авторизация логин", k=5)

    assert "BIZ-902" in {h.node_key for h in result_decision.hits}, (
        f"BIZ-902 не найден: {[h.node_key for h in result_decision.hits]}"
    )
    assert "T-902" in {h.node_key for h in result_task.hits}, (
        f"T-902 не найден: {[h.node_key for h in result_task.hits]}"
    )

    for result, key in ((result_decision, "BIZ-902"), (result_task, "T-902")):
        src = next((s for s in result.sources if s.node_key == key), None)
        assert src is not None, f"{key} не в sources"
        assert src.source_path == source, (
            f"{key} source_path ожидался {source!r}, получили {src.source_path!r}"
        )
