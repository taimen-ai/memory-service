"""Онтология Company Brain: виды по умолчанию, natural-id resolver, утилиты для меток графа.

Виды и шаблоны идентификаторов — данные, а не код (MEM-ADR-020; TAI-ADR-0042): бизнес-
онтология vault лежит пакетом по умолчанию ``core/packs/default.json`` (соответствует
``schemas/*.schema.json`` в vault; сами схемы в репозиторий не копируются). Natural-id
resolver (``is_natural_id``/``id_type``) берёт idPatterns из каталога пакетов — по
умолчанию из пакета по умолчанию, либо из переданного каталога namespace.
Неизвестные entity_type допускаются и сохраняются как свободные узлы (label ``entity``),
если namespace не в строгом режиме (``domain/registry.py``).
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from platform_memory.core.kinds import KindCatalog, default_catalog, default_pack

# Синтетические виды, которые ingest создаёт поверх vault.
SYNTHETIC_TYPES: frozenset[str] = frozenset({"question", "project"})

# Виды пакета по умолчанию, формализованные схемами в vault (ядро онтологии).
CORE_TYPES: frozenset[str] = frozenset(
    k.kind for k in default_pack().kinds if k.kind not in SYNTHETIC_TYPES
)

# Шаблоны стабильных natural-id пакета по умолчанию (вид -> первый idPattern).
ID_PATTERNS: dict[str, re.Pattern[str]] = {
    k.kind: k.id_patterns[0] for k in default_pack().kinds if k.id_patterns
}


def is_natural_id(token: str, catalog: KindCatalog | None = None) -> bool:
    """Похож ли токен целиком на стабильный id какого-либо вида каталога."""
    return id_type(token, catalog) is not None


def id_type(token: str, catalog: KindCatalog | None = None) -> str | None:
    """Вид сущности по виду id (idPatterns пакетов), либо None."""
    return (catalog or default_catalog()).id_kind(token.strip())


# Допустимые символы метки графа (вид узла, тип ребра): начинается с буквы/_, далее буквы/цифры/_.
_LABEL_OK = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_LABEL_CLEAN = re.compile(r"[^A-Za-z0-9_]")


def sanitize_label(entity_type: str | None) -> str:
    """Привести entity_type к валидной метке графа. Неизвестное/пустое -> 'entity'."""
    if not entity_type:
        return "entity"
    label = entity_type.strip()
    if _LABEL_OK.match(label):
        return label
    cleaned = _LABEL_CLEAN.sub("_", label)
    if not cleaned or not re.match(r"^[A-Za-z_]", cleaned):
        cleaned = f"t_{cleaned}" if cleaned else "entity"
    return cleaned


def load_schemas(schemas_dir: str | Path) -> dict[str, dict[str, Any]]:
    """Опционально подгрузить JSON-схемы из каталога (для валидации). Ключ — entity_type."""
    out: dict[str, dict[str, Any]] = {}
    path = Path(schemas_dir)
    if not path.is_dir():
        return out
    for f in sorted(path.glob("*.schema.json")):
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        const = (data.get("properties", {}).get("entity_type", {}) or {}).get("const")
        if const:
            out[const] = data
    return out


def required_fields(schema: dict[str, Any]) -> list[str]:
    """Список required-полей схемы (без entity_type)."""
    return [f for f in schema.get("required", []) if f != "entity_type"]
