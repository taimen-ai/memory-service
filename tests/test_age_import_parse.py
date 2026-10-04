"""Разбор текста agtype для переноса графа из AGE (MEM-ADR-023 п.5). БД не нужна."""

from __future__ import annotations

import pytest

from platform_memory.graph.age_import import MigrationReport, NamespaceCounts, parse_agtype

pytestmark = pytest.mark.unit


def test_plain_map_is_json():
    assert parse_agtype('{"a": 1, "b": [true, null], "c": {"d": "e"}}') == {
        "a": 1,
        "b": [True, None],
        "c": {"d": "e"},
    }


def test_type_annotations_are_stripped_outside_strings():
    text = '{"n": 1.5::numeric, "v": {"id": 1, "label": "x", "properties": {}}::vertex}'
    assert parse_agtype(text) == {"n": 1.5, "v": {"id": 1, "label": "x", "properties": {}}}
    assert parse_agtype("[1, 2]::path") == [1, 2]


def test_strings_with_double_colon_and_escapes_survive():
    text = '{"k": "a::vertex", "q": "он сказал \\"да\\" \\\\ ::edge", "n": 2::numeric}'
    assert parse_agtype(text) == {"k": "a::vertex", "q": 'он сказал "да" \\ ::edge', "n": 2}


@pytest.mark.parametrize("text", ['{"x": NaN}', '{"x": Infinity}', "{not json", '{"x": 1::}'])
def test_unparsable_is_value_error(text):
    with pytest.raises(ValueError):
        parse_agtype(text)


def test_none_is_none():
    assert parse_agtype(None) is None


def test_reconciliation_rules():
    ok = NamespaceCounts(age_nodes=3, dup_nodes=1, table_nodes=2, age_edges=1, table_edges=1)
    assert ok.ok
    skipped = NamespaceCounts(age_nodes=2, skipped_nodes=1, table_nodes=1)
    assert not skipped.ok
    extra = NamespaceCounts(age_nodes=1, table_nodes=2)  # в таблицах больше, чем в AGE
    assert not extra.ok
    report = MigrationReport(graph="g", namespaces={"a": ok, "b": skipped, "c": extra})
    assert report.mismatched() == ["b", "c"] and not report.ok
    dry = MigrationReport(graph="g", dry_run=True, namespaces={"a": ok, "b": skipped, "c": extra})
    assert dry.mismatched() == ["b"]  # в dry-run таблицы не пишутся — только пропуски
