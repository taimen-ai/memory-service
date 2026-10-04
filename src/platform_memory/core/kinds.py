"""Доменные пакеты видов: виды сущностей и связи как данные (MEM-ADR-020; TAI-ADR-0042).

Движок product-neutral: ни один домен не зашит в код. Домен приходит пакетом::

    {"name": "software-delivery", "version": 1,
     "kinds": [{"kind": "endpoint",
                "naturalKey": "<METHOD> <path с параметрами как {}>",
                "aliases": ["<path с исходными именами параметров>"],
                "kindAliases": ["route"],
                "idPatterns": ["\\b(?:GET|POST)\\s+/[\\w{}./:-]+"],
                "attributes": {"type": "object", "properties": {"method": {"type": "string"}}}}],
     "relations": [{"relation": "calls", "fromKinds": ["ui_call"], "toKinds": ["endpoint"],
                    "temporal": true}]}

* ``version`` — строка или число (приводится к строке: ``1`` -> ``"1"``);
* ``naturalKey`` — JSON Schema строки ключа (подмножество: type/pattern/minLength/
  maxLength/enum) или шаблон ключа с плейсхолдерами ``<name …>`` / ``{name}``
  (``"<repo>:<path>"``, ``"{project}-{number}"``). Имя плейсхолдера — первое слово
  в скобках, остальное — пояснение для человека. Шаблон собирает ключ из атрибутов,
  если снимок ключа не дал, и в строгом режиме проверяет форму ключа;
* ``aliases`` — **формы естественного ключа** сущности, шаблоны с теми же
  плейсхолдерами (``"ADR-<number>"`` для ключа ``"<SERIES>-<number>"``). Значение
  плейсхолдера берётся из атрибутов сущности, затем из частей её ключа; псевдонимы
  пишутся в узел (``props.aliases``), и якорь обхода разрешается по ним;
* ``kindAliases`` — синонимы **имени вида** (``ticket`` → ``issue``): запись и якоря
  обхода принимают их и приводят к каноническому виду;
* ``idPatterns`` — регулярные выражения, извлекающие идентификаторы вида из текста
  (именованная группа ``id`` или всё совпадение);
* ``attributes`` — JSON Schema атрибутов (то же подмножество + object/properties/required);
  у снимков ``required`` верхнего уровня проверяется у сведения версий всех источников
  сущности, а не у вклада одного снимка (MEM-ADR-022);
* ``temporal`` связи — у фактов связи есть интервал валидности (по умолчанию ``true``);
* ``cardinality`` связи — ``many`` (по умолчанию: у субъекта набор объектов, сверка
  закрывает только пропавшие пары) или ``one`` (у субъекта один объект: смена объекта
  при сверке снимка закрывает старый факт с ``superseded_by`` нового);
* ``searchable: {"fields": [...]}`` вида — сущности вида индексируются для поиска по
  смыслу (MEM-ADR-020, амендмент 2026-09-28): текст эмбеддинга — ``title`` и значения
  перечисленных атрибутов (``search_text``). Вид без ``searchable`` не индексируется;
* ``sourcePriority`` вида — порядок источников снимков (glob по ``source``, ``*``/``?``):
  при сведении атрибутов сущности из нескольких источников значение атрибута берётся
  из источника, совпавшего с более ранним шаблоном (MEM-ADR-022). Источник вне списка —
  после всех перечисленных; при равном приоритете — источник с более поздним
  ``observedAt``.

Модуль чистый (БД не нужна): разбор и валидация пакета, каталог видов namespace,
извлечение идентификаторов. Хранение — ``domain/registry.py``. Пакет по умолчанию
(бизнес-онтология vault, бывший код ``core/ontology.py``) лежит данными в
``core/packs/default.json``.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from fnmatch import fnmatchcase
from functools import lru_cache
from pathlib import Path
from typing import Any

# Базовые виды есть всегда, в любом namespace и при любом наборе пакетов.
BASE_KINDS: frozenset[str] = frozenset({"document", "entity", "fact"})

DEFAULT_PACK_NAME = "default"
# Префикс ссылки на пакет арендатора: ``tenant:<имя>[@<версия>]`` (MEM-ADR-020, амендмент
# 2026-09-28). Имя пакета двоеточия не содержит, поэтому префикс однозначен.
TENANT_REF_PREFIX = "tenant:"
_DEFAULT_PACK_FILE = Path(__file__).parent / "packs" / "default.json"

PACK_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")
PACK_VERSION_RE = re.compile(r"^[0-9A-Za-z][0-9A-Za-z.+-]{0,31}$")
# Имя вида/связи — валидная метка графа (вид становится label узла, связь — типом ребра).
KIND_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{0,62}$")

MAX_KINDS = 200
MAX_RELATIONS = 400
MAX_ID_PATTERNS = 10
MAX_PATTERN_LEN = 300
MAX_ALIASES = 20
MAX_SEARCHABLE_FIELDS = 20
MAX_SOURCE_PRIORITY = 20
MAX_SOURCE_PATTERN_LEN = 200
# Имя атрибута в searchable.fields — как ключ атрибутов сущности.
ATTR_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")
CARDINALITIES = ("many", "one")

# Плейсхолдер шаблона ключа: ``<name пояснение>`` или ``{name}``.
_PLACEHOLDER = re.compile(r"<([A-Za-z_][A-Za-z0-9_]*)[^<>]*>|\{([A-Za-z_][A-Za-z0-9_]*)\}")


class PackError(ValueError):
    """Некорректный доменный пакет (400 на HTTP-границе)."""


class UnknownKindError(ValueError):
    """Вид сущности не объявлен пакетами namespace в строгом режиме."""


class AttributesError(ValueError):
    """Атрибуты/ключ сущности не соответствуют схеме вида (строгий режим)."""


class UnknownRelationError(ValueError):
    """Связь не объявлена пакетами namespace или концы вне fromKinds/toKinds (строгий режим)."""


# --- подмножество JSON Schema (без внешней зависимости) ---

_JSON_TYPES: dict[str, tuple[type, ...]] = {
    "string": (str,),
    "integer": (int,),
    "number": (int, float),
    "boolean": (bool,),
    "object": (dict,),
    "array": (list, tuple),
    "null": (type(None),),
}


def schema_errors(value: Any, schema: dict[str, Any], path: str = "$") -> list[str]:
    """Проверить значение по подмножеству JSON Schema; вернуть список нарушений.

    Поддержано: type (строка или список), enum, const, pattern, minLength, maxLength,
    minimum, maximum, properties, required, additionalProperties (bool), items.
    Прочие ключевые слова игнорируются (документированный предел, MEM-ADR-020).
    """
    errors: list[str] = []
    if not isinstance(schema, dict):
        return errors
    stype = schema.get("type")
    if stype:
        allowed = [stype] if isinstance(stype, str) else list(stype)
        ok = False
        for t in allowed:
            py = _JSON_TYPES.get(str(t))
            if py is None:
                continue
            # bool — подкласс int в Python, но не integer/number в JSON Schema.
            if t in ("integer", "number") and isinstance(value, bool):
                continue
            if isinstance(value, py):
                ok = True
                break
        if not ok:
            return [f"{path}: ожидается {stype}"]
    if "enum" in schema and value not in (schema.get("enum") or []):
        errors.append(f"{path}: значение не из enum")
    if "const" in schema and value != schema["const"]:
        errors.append(f"{path}: ожидается const")
    if isinstance(value, str):
        if "pattern" in schema and not re.search(str(schema["pattern"]), value):
            errors.append(f"{path}: не соответствует pattern")
        if "minLength" in schema and len(value) < int(schema["minLength"]):
            errors.append(f"{path}: короче minLength")
        if "maxLength" in schema and len(value) > int(schema["maxLength"]):
            errors.append(f"{path}: длиннее maxLength")
    if isinstance(value, int | float) and not isinstance(value, bool):
        if "minimum" in schema and value < schema["minimum"]:
            errors.append(f"{path}: меньше minimum")
        if "maximum" in schema and value > schema["maximum"]:
            errors.append(f"{path}: больше maximum")
    if isinstance(value, dict):
        props = schema.get("properties") or {}
        for req in schema.get("required") or []:
            if req not in value:
                errors.append(f"{path}.{req}: обязательное поле отсутствует")
        for key, sub in props.items():
            if key in value:
                errors.extend(schema_errors(value[key], sub, f"{path}.{key}"))
        if schema.get("additionalProperties") is False:
            for key in value:
                if key not in props:
                    errors.append(f"{path}.{key}: лишнее поле")
    if isinstance(value, list | tuple) and isinstance(schema.get("items"), dict):
        for i, item in enumerate(value):
            errors.extend(schema_errors(item, schema["items"], f"{path}[{i}]"))
    return errors


# --- модель пакета ---


@dataclass(frozen=True, slots=True)
class KeyTemplate:
    """Шаблон естественного ключа: литералы и плейсхолдеры ``<name …>`` / ``{name}``.

    Один и тот же разбор служит трём целям: собрать ключ из атрибутов, проверить
    форму ключа (строгий режим) и разобрать ключ на части для псевдонимов вида.
    """

    text: str
    names: tuple[str, ...]
    regex: re.Pattern[str]

    @classmethod
    def parse(cls, text: str) -> KeyTemplate | None:
        """Шаблон из строки; None — плейсхолдеров нет (это не шаблон)."""
        names: list[str] = []
        parts: list[str] = []
        pos = 0
        for m in _PLACEHOLDER.finditer(text):
            parts.append(re.escape(text[pos : m.start()]))
            names.append(m.group(1) or m.group(2))
            # Одинаковые плейсхолдеры дважды — второй обязан совпасть с первым.
            idx = names.index(names[-1])
            parts.append(f"(?P=g{idx})" if idx != len(names) - 1 else f"(?P<g{idx}>.+?)")
            pos = m.end()
        if not names:
            return None
        parts.append(re.escape(text[pos:]))
        return cls(text=text, names=tuple(names), regex=re.compile("".join(parts), re.S))

    def split(self, key: str) -> dict[str, str] | None:
        """Части ключа по плейсхолдерам; None — ключ не соответствует шаблону."""
        m = self.regex.fullmatch(key)
        if m is None:
            return None
        out: dict[str, str] = {}
        for i, name in enumerate(self.names):
            value = m.groupdict().get(f"g{i}")
            if value is not None:
                out.setdefault(name, value)
        return out

    def render(self, lookup) -> str | None:
        """Подставить значения ``lookup(name)``; None — какого-то значения нет."""
        missing = False

        def _sub(m: re.Match[str]) -> str:
            nonlocal missing
            value = lookup(m.group(1) or m.group(2))
            if value is None:
                missing = True
                return ""
            return value

        out = _PLACEHOLDER.sub(_sub, self.text)
        return None if missing else out


@lru_cache(maxsize=1024)
def key_template(text: str) -> KeyTemplate | None:
    """Разобранный шаблон ключа (кэш: шаблоны пакета разбираются на каждую сущность)."""
    return KeyTemplate.parse(text)


def _scalar(value: Any) -> str | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, str | int | float):
        text = str(value).strip()
        return text or None
    return None


def _lookup(name: str, *sources: dict[str, Any]) -> str | None:
    """Значение плейсхолдера: точное имя, затем без учёта регистра — по источникам по порядку."""
    for src in sources:
        if name in src and _scalar(src[name]) is not None:
            return _scalar(src[name])
        low = name.lower()
        for key, value in src.items():
            if str(key).lower() == low and _scalar(value) is not None:
                return _scalar(value)
    return None


@dataclass(frozen=True, slots=True)
class KindSpec:
    """Вид сущности из пакета."""

    kind: str
    natural_key: dict[str, Any] | str | None = None
    aliases: tuple[str, ...] = ()  # шаблоны форм ключа
    kind_aliases: tuple[str, ...] = ()  # синонимы имени вида
    id_patterns: tuple[re.Pattern[str], ...] = ()
    attributes: dict[str, Any] | None = None
    pack: str = ""
    searchable: tuple[str, ...] = ()  # атрибуты текста эмбеддинга; пусто — не индексируется
    source_priority: tuple[str, ...] = ()  # glob-шаблоны source по убыванию приоритета

    @property
    def key_template(self) -> KeyTemplate | None:
        return key_template(self.natural_key) if isinstance(self.natural_key, str) else None

    def key_errors(self, natural_key: str) -> list[str]:
        """Нарушения схемы naturalKey: JSON Schema или форма шаблона."""
        if isinstance(self.natural_key, dict):
            return schema_errors(natural_key, self.natural_key, "naturalKey")
        template = self.key_template
        if template is not None and template.split(natural_key) is None:
            return [f"naturalKey: {natural_key!r} не соответствует шаблону {template.text!r}"]
        return []

    def build_key(self, attributes: dict[str, Any]) -> str | None:
        """Собрать natural_key по шаблону из атрибутов; None — у вида нет шаблона."""
        template = self.key_template
        if template is None:
            return None
        missing = [n for n in template.names if _lookup(n, attributes) is None]
        if missing:
            raise AttributesError(
                f"naturalKey вида {self.kind!r}: нет атрибутов {', '.join(dict.fromkeys(missing))}"
            )
        return template.render(lambda n: _lookup(n, attributes))

    def key_aliases(self, natural_key: str, attributes: dict[str, Any] | None = None) -> list[str]:
        """Формы ключа сущности по шаблонам ``aliases`` (без самого ключа).

        Плейсхолдер берётся из атрибутов, затем из частей ключа по шаблону
        ``naturalKey``; форма, для которой значения нет, пропускается.
        """
        if not self.aliases:
            return []
        template = self.key_template
        parts = (template.split(natural_key) if template is not None else None) or {}
        attrs = attributes or {}
        out: list[str] = []
        for text in self.aliases:
            alias_tpl = key_template(text)
            if alias_tpl is None:
                continue
            value = alias_tpl.render(lambda n: _lookup(n, attrs, parts))
            if value and value != natural_key and value not in out:
                out.append(value)
        return out

    def extract_ids(self, text: str) -> list[str]:
        """Идентификаторы вида из текста по idPatterns (группа ``id`` или всё совпадение)."""
        out: list[str] = []
        for pattern in self.id_patterns:
            for match in pattern.finditer(text or ""):
                token = match.groupdict().get("id") or match.group(0)
                token = token.strip()
                if token and token not in out:
                    out.append(token)
        return out

    def matches_id(self, token: str) -> bool:
        """Токен целиком — идентификатор этого вида (fullmatch по idPatterns)."""
        return any(p.fullmatch(token.strip()) for p in self.id_patterns)

    def search_text(self, title: str, attributes: dict[str, Any] | None) -> str:
        """Текст эмбеддинга сущности: title и ``имя: значение`` полей ``searchable``.

        Порядок — как в ``fields``; отсутствующие и пустые значения пропускаются,
        список — через запятую, объект — JSON. Пустая строка — вид не индексируется.
        """
        if not self.searchable:
            return ""
        attrs = attributes or {}
        lines = [str(title or "").strip()]
        for name in self.searchable:
            value = _search_value(attrs.get(name))
            if value:
                lines.append(f"{name}: {value}")
        return "\n".join(line for line in lines if line)

    def source_rank(self, source: str) -> int:
        """Ранг источника при сведении атрибутов: номер первого совпавшего шаблона
        ``sourcePriority`` (меньше — сильнее); вне списка — после всех шаблонов."""
        for i, pattern in enumerate(self.source_priority):
            if fnmatchcase(source, pattern):
                return i
        return len(self.source_priority)

    def to_payload(self) -> dict[str, Any]:
        out: dict[str, Any] = {"kind": self.kind}
        if self.natural_key is not None:
            out["naturalKey"] = self.natural_key
        if self.aliases:
            out["aliases"] = list(self.aliases)
        if self.kind_aliases:
            out["kindAliases"] = list(self.kind_aliases)
        if self.id_patterns:
            out["idPatterns"] = [p.pattern for p in self.id_patterns]
        if self.attributes is not None:
            out["attributes"] = self.attributes
        # Только если задано: каноническая форма пакетов без поля не меняется.
        if self.searchable:
            out["searchable"] = {"fields": list(self.searchable)}
        if self.source_priority:
            out["sourcePriority"] = list(self.source_priority)
        return out


@dataclass(frozen=True, slots=True)
class RelationSpec:
    """Связь из пакета: имя, допустимые виды концов, темпоральность, кардинальность."""

    relation: str
    from_kinds: tuple[str, ...] = ()
    to_kinds: tuple[str, ...] = ()
    temporal: bool = True
    cardinality: str = "many"
    pack: str = ""

    @property
    def edge_label(self) -> str:
        """Тип ребра графа (как у FactStore: предикат в верхнем регистре)."""
        return self.relation.upper()

    def to_payload(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "relation": self.relation,
            "fromKinds": list(self.from_kinds),
            "toKinds": list(self.to_kinds),
            "temporal": self.temporal,
        }
        # Значение по умолчанию не пишем: каноническая форма пакетов без поля не меняется.
        if self.cardinality != "many":
            out["cardinality"] = self.cardinality
        return out


@dataclass(frozen=True, slots=True)
class DomainPack:
    """Доменный пакет: имя, версия (иммутабельна после регистрации), виды и связи."""

    name: str
    version: str
    kinds: tuple[KindSpec, ...] = ()
    relations: tuple[RelationSpec, ...] = ()
    description: str = ""
    # Namespace-владелец пакета арендатора; пусто — общий пакет. В каноническую форму
    # (и хэш версии) не входит: владелец — место хранения, а не содержимое.
    owner: str = ""

    @property
    def ref_name(self) -> str:
        """Имя в ссылке: ``name`` у общего пакета, ``tenant:name`` у пакета арендатора."""
        return f"{TENANT_REF_PREFIX}{self.name}" if self.owner else self.name

    @property
    def ref(self) -> str:
        return f"{self.ref_name}@{self.version}"

    def to_payload(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "name": self.name,
            "version": self.version,
            "kinds": [k.to_payload() for k in self.kinds],
            "relations": [r.to_payload() for r in self.relations],
        }
        if self.description:
            out["description"] = self.description
        return out

    def canonical_json(self) -> str:
        """Каноническая форма для сравнения версий (иммутабельность по содержимому)."""
        return json.dumps(self.to_payload(), ensure_ascii=False, sort_keys=True)


def _search_value(value: Any) -> str:
    if value is None or value == "" or value == []:
        return ""
    if isinstance(value, list | tuple):
        return ", ".join(v for v in (_search_value(x) for x in value) if v)
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value).strip()


def _parse_searchable(raw: Any, attrs: dict[str, Any] | None, where: str) -> tuple[str, ...]:
    """``searchable: {fields: [...]}``: 1..20 уникальных имён атрибутов.

    Если у вида объявлены ``attributes.properties``, каждое поле должно быть среди них.
    """
    if raw is None:
        return ()
    if not isinstance(raw, dict):
        raise PackError(f"{where}.searchable: ожидается объект {{fields: [...]}}")
    fields = raw.get("fields")
    if not isinstance(fields, list) or not fields:
        raise PackError(f"{where}.searchable.fields: нужен непустой список имён атрибутов")
    if len(fields) > MAX_SEARCHABLE_FIELDS:
        raise PackError(f"{where}.searchable.fields: не больше {MAX_SEARCHABLE_FIELDS}")
    for name in fields:
        if not isinstance(name, str) or not ATTR_NAME_RE.match(name):
            raise PackError(
                f"{where}.searchable.fields: некорректное имя атрибута {name!r}: "
                "[A-Za-z_][A-Za-z0-9_]{0,63}"
            )
    if len(set(fields)) != len(fields):
        raise PackError(f"{where}.searchable.fields: имена повторяются")
    props = attrs.get("properties") if isinstance(attrs, dict) else None
    if isinstance(props, dict) and props:
        unknown = [f for f in fields if f not in props]
        if unknown:
            raise PackError(
                f"{where}.searchable.fields: атрибуты {unknown} не объявлены в "
                "attributes.properties вида"
            )
    return tuple(fields)


def _parse_source_priority(raw: Any, where: str) -> tuple[str, ...]:
    """``sourcePriority``: до 20 уникальных glob-шаблонов ``source`` снимка."""
    patterns = _str_list(raw, f"{where}.sourcePriority", limit=MAX_SOURCE_PRIORITY)
    for pattern in patterns:
        if len(pattern) > MAX_SOURCE_PATTERN_LEN:
            raise PackError(
                f"{where}.sourcePriority: шаблон длиннее {MAX_SOURCE_PATTERN_LEN} символов"
            )
    if len(set(patterns)) != len(patterns):
        raise PackError(f"{where}.sourcePriority: шаблоны повторяются")
    return tuple(patterns)


def _str_list(raw: Any, where: str, *, limit: int) -> list[str]:
    if raw is None:
        return []
    if not isinstance(raw, list) or not all(isinstance(x, str) and x.strip() for x in raw):
        raise PackError(f"{where}: ожидается список непустых строк")
    if len(raw) > limit:
        raise PackError(f"{where}: больше {limit} элементов")
    return [x.strip() for x in raw]


def _compile_patterns(raw: Any, where: str) -> tuple[re.Pattern[str], ...]:
    out: list[re.Pattern[str]] = []
    for i, text in enumerate(_str_list(raw, where, limit=MAX_ID_PATTERNS)):
        if len(text) > MAX_PATTERN_LEN:
            raise PackError(f"{where}[{i}]: длиннее {MAX_PATTERN_LEN} символов")
        try:
            out.append(re.compile(text))
        except re.error as exc:
            raise PackError(f"{where}[{i}]: невалидное регулярное выражение: {exc}") from exc
    return tuple(out)


def parse_pack(payload: dict[str, Any]) -> DomainPack:
    """Разобрать и провалидировать пакет из JSON (camelCase и snake_case)."""
    if not isinstance(payload, dict):
        raise PackError("Пакет должен быть JSON-объектом")
    name = str(payload.get("name", "") or "")
    raw_version = payload.get("version", "")
    # Версия числом (``version: 1`` в YAML пакета) приводится к строке; bool — не версия.
    if isinstance(raw_version, bool) or not isinstance(raw_version, str | int | float):
        raise PackError(f"Некорректная версия пакета {raw_version!r}")
    version = str(raw_version).strip()
    if not PACK_NAME_RE.match(name):
        raise PackError(f"Некорректное имя пакета {name!r}: [a-z0-9][a-z0-9._-]{{0,63}}")
    if not PACK_VERSION_RE.match(version):
        raise PackError(f"Некорректная версия пакета {version!r}")
    raw_kinds = payload.get("kinds") or []
    raw_rels = payload.get("relations") or []
    if not isinstance(raw_kinds, list) or not isinstance(raw_rels, list):
        raise PackError("kinds и relations должны быть списками")
    if len(raw_kinds) > MAX_KINDS or len(raw_rels) > MAX_RELATIONS:
        raise PackError(f"Не больше {MAX_KINDS} видов и {MAX_RELATIONS} связей в пакете")

    kinds: list[KindSpec] = []
    names: set[str] = set()
    for i, raw in enumerate(raw_kinds):
        where = f"kinds[{i}]"
        if not isinstance(raw, dict):
            raise PackError(f"{where}: ожидается объект")
        kind = str(raw.get("kind", "") or "")
        if not KIND_NAME_RE.match(kind):
            raise PackError(f"{where}.kind: некорректное имя вида {kind!r}")
        if kind in BASE_KINDS:
            raise PackError(f"{where}.kind: базовый вид {kind!r} нельзя переопределять")
        kind_aliases = _str_list(
            raw.get("kindAliases", raw.get("kind_aliases")),
            f"{where}.kindAliases",
            limit=MAX_ALIASES,
        )
        for alias in kind_aliases:
            if not KIND_NAME_RE.match(alias):
                raise PackError(f"{where}.kindAliases: некорректное имя вида {alias!r}")
        aliases = _str_list(raw.get("aliases"), f"{where}.aliases", limit=MAX_ALIASES)
        for alias in aliases:
            if key_template(alias) is None:
                raise PackError(
                    f"{where}.aliases: {alias!r} — форма ключа без плейсхолдера "
                    "(<name> или {name}); синонимы имени вида — в kindAliases"
                )
        for alias in [kind, *kind_aliases]:
            if alias in names:
                raise PackError(f"{where}: имя/псевдоним {alias!r} уже объявлен в пакете")
            if alias in BASE_KINDS:
                raise PackError(f"{where}: псевдоним {alias!r} совпадает с базовым видом")
            names.add(alias)
        nkey = raw.get("naturalKey", raw.get("natural_key"))
        if nkey is not None and not isinstance(nkey, dict | str):
            raise PackError(f"{where}.naturalKey: JSON Schema (объект) или шаблон (строка)")
        if isinstance(nkey, dict) and "pattern" in nkey:
            try:
                re.compile(str(nkey["pattern"]))
            except re.error as exc:
                raise PackError(f"{where}.naturalKey.pattern: {exc}") from exc
        attrs = raw.get("attributes")
        if attrs is not None and not isinstance(attrs, dict):
            raise PackError(f"{where}.attributes: ожидается JSON Schema (объект)")
        kinds.append(
            KindSpec(
                kind=kind,
                natural_key=nkey,
                aliases=tuple(aliases),
                kind_aliases=tuple(kind_aliases),
                id_patterns=_compile_patterns(
                    raw.get("idPatterns", raw.get("id_patterns")), f"{where}.idPatterns"
                ),
                attributes=attrs,
                pack=name,
                searchable=_parse_searchable(raw.get("searchable"), attrs, where),
                source_priority=_parse_source_priority(
                    raw.get("sourcePriority", raw.get("source_priority")), where
                ),
            )
        )

    relations: list[RelationSpec] = []
    rel_names: set[str] = set()
    for i, raw in enumerate(raw_rels):
        where = f"relations[{i}]"
        if not isinstance(raw, dict):
            raise PackError(f"{where}: ожидается объект")
        rel = str(raw.get("relation", "") or "")
        if not KIND_NAME_RE.match(rel):
            raise PackError(f"{where}.relation: некорректное имя связи {rel!r}")
        if rel in rel_names:
            raise PackError(f"{where}.relation: связь {rel!r} уже объявлена")
        rel_names.add(rel)
        temporal = raw.get("temporal", True)
        if not isinstance(temporal, bool):
            raise PackError(f"{where}.temporal: ожидается bool")
        cardinality = str(raw.get("cardinality", "many") or "many")
        if cardinality not in CARDINALITIES:
            raise PackError(f"{where}.cardinality: одно из {list(CARDINALITIES)}")
        relations.append(
            RelationSpec(
                relation=rel,
                from_kinds=tuple(
                    _str_list(
                        raw.get("fromKinds", raw.get("from_kinds")),
                        f"{where}.fromKinds",
                        limit=MAX_KINDS,
                    )
                ),
                to_kinds=tuple(
                    _str_list(
                        raw.get("toKinds", raw.get("to_kinds")), f"{where}.toKinds", limit=MAX_KINDS
                    )
                ),
                temporal=temporal,
                cardinality=cardinality,
                pack=name,
            )
        )
    return DomainPack(
        name=name,
        version=version,
        kinds=tuple(kinds),
        relations=tuple(relations),
        description=str(payload.get("description", "") or ""),
    )


def split_pack_ref(ref: str) -> tuple[bool, str, str]:
    """Ссылка на пакет -> ``(tenant, name, version)``; пустая версия — последняя."""
    text = (ref or "").strip()
    tenant = text.startswith(TENANT_REF_PREFIX)
    if tenant:
        text = text[len(TENANT_REF_PREFIX) :]
    name, _, version = text.partition("@")
    return tenant, name.strip(), version.strip()


def version_key(version: str) -> tuple:
    """Ключ сортировки версий: числовые сегменты сравниваются как числа."""
    parts = re.split(r"[.+-]", version)
    return tuple((0, int(p), "") if p.isdigit() else (1, 0, p) for p in parts)


@lru_cache(maxsize=1)
def default_pack() -> DomainPack:
    """Пакет по умолчанию — данными из ``core/packs/default.json``."""
    return parse_pack(json.loads(_DEFAULT_PACK_FILE.read_text(encoding="utf-8")))


# --- каталог видов namespace ---


@dataclass(slots=True)
class KindCatalog:
    """Объединение пакетов, действующих в namespace, плюс базовые виды.

    При совпадении имён побеждает пакет, стоящий раньше в списке (порядок включения
    в настройке namespace); ``conflicts`` фиксирует перекрытые объявления.
    """

    packs: tuple[DomainPack, ...] = ()
    strict: bool = False
    kinds: dict[str, KindSpec] = field(default_factory=dict)
    kind_aliases: dict[str, str] = field(default_factory=dict)  # синоним -> каноническое имя
    relations: dict[str, RelationSpec] = field(default_factory=dict)
    conflicts: list[str] = field(default_factory=list)

    @classmethod
    def build(cls, packs: Iterable[DomainPack], *, strict: bool = False) -> KindCatalog:
        cat = cls(packs=tuple(packs), strict=strict)
        for pack in cat.packs:
            for spec in pack.kinds:
                if spec.kind in cat.kinds or spec.kind in cat.kind_aliases:
                    cat.conflicts.append(f"{pack.ref}:{spec.kind}")
                    continue
                cat.kinds[spec.kind] = spec
                for alias in spec.kind_aliases:
                    if alias in cat.kinds or alias in cat.kind_aliases:
                        cat.conflicts.append(f"{pack.ref}:{alias}")
                        continue
                    cat.kind_aliases[alias] = spec.kind
            for rel in pack.relations:
                if rel.relation in cat.relations:
                    cat.conflicts.append(f"{pack.ref}:{rel.relation}")
                    continue
                cat.relations[rel.relation] = rel
        return cat

    def canonical(self, kind: str) -> str:
        """Каноническое имя вида (псевдоним -> вид); неизвестное — как есть."""
        kind = (kind or "").strip()
        return self.kind_aliases.get(kind, kind)

    def spec(self, kind: str) -> KindSpec | None:
        return self.kinds.get(self.canonical(kind))

    def is_known(self, kind: str) -> bool:
        canon = self.canonical(kind)
        return canon in BASE_KINDS or canon in self.kinds

    def relation(self, name: str) -> RelationSpec | None:
        """Связь по имени (регистр предиката FactStore не важен)."""
        return self.relations.get((name or "").strip().lower())

    def check_relation(self, relation: str, from_kind: str, to_kind: str) -> str:
        """Проверить связь и виды её концов; вернуть имя связи в нижнем регистре.

        Нестрогий режим — любая связь. Строгий — связь объявлена пакетами namespace,
        а канонические виды концов входят в ``fromKinds``/``toKinds`` (пустой список —
        любой вид), иначе ``UnknownRelationError``.
        """
        name = (relation or "").strip().lower()
        if not self.strict:
            return name
        spec = self.relations.get(name)
        if spec is None:
            raise UnknownRelationError(
                f"Связь {relation!r} не объявлена доменными пакетами namespace "
                f"(строгий режим); известны: {', '.join(sorted(self.relations)) or '—'}"
            )
        src, dst = self.canonical(from_kind), self.canonical(to_kind)
        if spec.from_kinds and src not in spec.from_kinds:
            raise UnknownRelationError(
                f"Связь {name!r}: вид {src!r} не входит в fromKinds {list(spec.from_kinds)}"
            )
        if spec.to_kinds and dst not in spec.to_kinds:
            raise UnknownRelationError(
                f"Связь {name!r}: вид {dst!r} не входит в toKinds {list(spec.to_kinds)}"
            )
        return name

    def key_aliases(
        self, kind: str, natural_key: str, attributes: dict[str, Any] | None = None
    ) -> list[str]:
        """Формы ключа сущности по шаблонам ``aliases`` её вида (пусто — вид без них)."""
        spec = self.spec(kind)
        return spec.key_aliases(natural_key, attributes) if spec is not None else []

    def check_entity(
        self,
        kind: str,
        *,
        natural_key: str = "",
        attributes: dict[str, Any] | None = None,
        partial: bool = False,
    ) -> str:
        """Проверить вид сущности; вернуть каноническое имя.

        Нестрогий режим — прежнее поведение: любой вид допустим, псевдоним
        приводится к каноническому имени. Строгий режим — неизвестный вид
        отвергается (``UnknownKindError``), ключ и атрибуты проверяются по схемам вида.
        ``partial`` — атрибуты — вклад одного источника снимка: ``required`` верхнего
        уровня не проверяется (его проверяет сверка у сведения, MEM-ADR-022).
        """
        canon = self.canonical(kind or "entity") or "entity"
        if not self.strict:
            return canon
        if canon in BASE_KINDS:
            return canon
        spec = self.kinds.get(canon)
        if spec is None:
            raise UnknownKindError(
                f"Вид сущности {kind!r} не объявлен доменными пакетами namespace "
                f"(строгий режим); известны: {', '.join(sorted(self.kinds)) or '—'}"
            )
        errors: list[str] = []
        if natural_key:
            errors.extend(spec.key_errors(natural_key))
        if spec.attributes is not None and attributes is not None:
            schema = spec.attributes
            if partial and isinstance(schema, dict):
                schema = {k: v for k, v in schema.items() if k != "required"}
            errors.extend(schema_errors(attributes, schema, "attributes"))
        if errors:
            raise AttributesError(f"Сущность вида {canon!r}: " + "; ".join(errors[:10]))
        return canon

    def extract_ids(self, text: str, kinds: Sequence[str] = ()) -> list[tuple[str, str]]:
        """Пары (вид, идентификатор) из текста по idPatterns (опц. только заданные виды)."""
        wanted = {self.canonical(k) for k in kinds if k}
        out: list[tuple[str, str]] = []
        for name, spec in self.kinds.items():
            if wanted and name not in wanted:
                continue
            for token in spec.extract_ids(text):
                if (name, token) not in out:
                    out.append((name, token))
        return out

    def id_kind(self, token: str) -> str | None:
        """Вид, чьи idPatterns целиком совпадают с токеном, либо None."""
        for name, spec in self.kinds.items():
            if spec.matches_id(token):
                return name
        return None

    def source_priorities(self) -> dict[str, tuple[str, ...]]:
        """``sourcePriority`` видов каталога (виды без него не входят)."""
        return {n: spec.source_priority for n, spec in self.kinds.items() if spec.source_priority}

    def required_attributes(self, kind: str) -> tuple[str, ...]:
        """Обязательные атрибуты вида (``required`` верхнего уровня схемы ``attributes``).

        Только строгий режим: нестрогий схемы атрибутов не проверяет.
        """
        spec = self.kinds.get(kind) if self.strict else None
        schema = spec.attributes if spec is not None else None
        required = schema.get("required") if isinstance(schema, dict) else None
        return tuple(str(n) for n in required) if isinstance(required, list) else ()

    def source_rank(self, kind: str, source: str) -> int:
        """Ранг источника для атрибутов сущности вида (``KindSpec.source_rank``); вид без
        ``sourcePriority`` или неизвестный — у всех источников равный ранг."""
        spec = self.spec(kind)
        return spec.source_rank(source) if spec is not None else 0

    def searchable_kinds(self) -> dict[str, KindSpec]:
        """Виды каталога с ``searchable`` (индексируемые для поиска по смыслу)."""
        return {name: spec for name, spec in self.kinds.items() if spec.searchable}

    def to_payload(self) -> dict[str, Any]:
        return {
            "strict": self.strict,
            "packages": [p.ref for p in self.packs],
            "base_kinds": sorted(BASE_KINDS),
            "kinds": sorted(self.kinds),
            "kindAliases": dict(sorted(self.kind_aliases.items())),
            "relations": sorted(self.relations),
            "searchable": {
                name: list(spec.searchable)
                for name, spec in sorted(self.searchable_kinds().items())
            },
            "conflicts": list(self.conflicts),
        }


@lru_cache(maxsize=1)
def default_catalog() -> KindCatalog:
    """Каталог пакета по умолчанию (нестрогий) — для vault-ingest без БД."""
    return KindCatalog.build([default_pack()])
