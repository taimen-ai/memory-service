# Адаптировано из graphify/cache.py (https://github.com/safishamsi/graphify), MIT License.
# Copyright (c) 2026 Safi Shamsi. See THIRD_PARTY.md for the full license text.
#
# Перенесён паттерн per-file semantic-cache: content-hash → пропуск неизменённого.
# Адаптировано под наш конвейер:
#   * хэшируем тело заметки (frontmatter уже отделён в VaultDoc — метадан-правки кэш не рвут);
#   * кэш ЧАНКОВ версионируется по версии экстрактора (CHUNKER_VERSION) — аналог AST-кэша;
#   * кэш ЭМБЕДДИНГОВ НЕ версионируется по чанкеру (иначе перебиллим Eden AI), а ключуется
#     по модели/размерности эмбеддинга (смена модели меняет вектор — это корректность, не версия).
# Не перенесено: stat-fastpath по mtime (тело уже в памяти), SSRF/относительные source_file,
# atexit-stat-index, hyperedges.
"""Семантический кэш конвейера: пропуск неизменённых заметок при ре-ingest.

Дорогая операция — эмбеддинг тел заметок через Eden AI. Кэш хранит вектор по хэшу входа
эмбеддинга, поэтому повторный прогон по неизменённому vault не перебиллит провайдера.
Кэш чанков (дешёвый, но версионируемый) демонстрирует правило: артефакты экстрактора
инвалидируются сменой его версии, артефакты LLM/эмбеддера — нет.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from pathlib import Path

from platform_memory.ingest.chunker import CHUNKER_VERSION

# Метка валидной файловой компоненты пути кэша (имя модели может содержать слэши/двоеточия).
_SLUG_RE = re.compile(r"[^A-Za-z0-9._-]+")


def _slug(text: str) -> str:
    return _SLUG_RE.sub("-", text).strip("-") or "default"


def content_hash(text: str) -> str:
    """SHA256 текста (тела заметки или входа эмбеддинга) — ключ кэша."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _atomic_write(path: Path, data: bytes) -> None:
    """Атомарно записать файл (tmp + os.replace), best-effort при гонках/ошибках ФС."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=path.name + ".", suffix=".tmp")
    try:
        os.write(fd, data)
        os.close(fd)
        os.replace(tmp, path)
    except Exception:
        try:
            os.close(fd)
        except OSError:
            pass
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


class _JsonDirCache:
    """Файловый кэш «hash → JSON» в каталоге. База для chunk/embedding-кэшей."""

    def __init__(self, base: str | Path) -> None:
        self.base = Path(base)

    def get(self, key: str) -> object | None:
        entry = self.base / f"{key}.json"
        if not entry.exists():
            return None
        try:
            return json.loads(entry.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return None

    def put(self, key: str, value: object) -> None:
        try:
            _atomic_write(self.base / f"{key}.json", json.dumps(value).encode("utf-8"))
        except OSError:
            pass  # кэш — оптимизация; сбой записи не должен валить ingest


class ChunkCache(_JsonDirCache):
    """Кэш результатов чанкинга. Версионируется по CHUNKER_VERSION (аналог AST-кэша graphify).

    Каталог: ``{cache_dir}/chunks/v{CHUNKER_VERSION}/`` — чанки от других версий чанкера
    не консультируются (могут быть устаревшими после изменения правил разбиения).
    """

    def __init__(self, cache_dir: str | Path) -> None:
        super().__init__(Path(cache_dir) / "chunks" / f"v{CHUNKER_VERSION}")

    def get_chunks(self, body: str) -> list[dict] | None:
        val = self.get(content_hash(body))
        return val if isinstance(val, list) else None

    def put_chunks(self, body: str, chunks: list[dict]) -> None:
        self.put(content_hash(body), chunks)


class EmbeddingCache(_JsonDirCache):
    """Кэш векторов эмбеддинга. НЕ версионируется по чанкеру; ключуется по модели/размерности.

    Каталог: ``{cache_dir}/embeddings/{model}-{dim}/`` — смена модели/размерности даёт
    другой вектор (корректность), но смена версии чанкера кэш НЕ инвалидирует (не перебиллить).
    """

    def __init__(self, cache_dir: str | Path, *, model: str, dim: int) -> None:
        super().__init__(Path(cache_dir) / "embeddings" / f"{_slug(model)}-{dim}")

    def get_vector(self, text: str) -> list[float] | None:
        val = self.get(content_hash(text))
        return val if isinstance(val, list) else None

    def put_vector(self, text: str, vector: list[float]) -> None:
        self.put(content_hash(text), list(vector))

    def embed_with_cache(self, texts: list[str], embed_fn) -> tuple[list[list[float]], int, int]:
        """Вернуть (векторы в порядке texts, попаданий, промахов).

        ``embed_fn(list[str]) -> list[list[float]]`` вызывается ТОЛЬКО для промахов —
        так повторный ingest по неизменённым заметкам не перебиллит Eden AI. Дубликаты
        входов внутри одного вызова тоже эмбеддятся один раз.
        """
        vectors: list[list[float] | None] = [None] * len(texts)
        miss_idx: list[int] = []
        miss_texts: list[str] = []
        miss_seen: dict[str, int] = {}  # text -> позиция в miss_texts (дедуп внутри вызова)
        hits = 0
        for i, text in enumerate(texts):
            cached = self.get_vector(text)
            if cached is not None:
                vectors[i] = cached
                hits += 1
            elif text in miss_seen:
                miss_idx.append(i)
                miss_texts.append(text)
            else:
                miss_seen[text] = len(miss_texts)
                miss_idx.append(i)
                miss_texts.append(text)

        if miss_texts:
            # Эмбеддим только уникальные промахи, затем разворачиваем по позициям.
            unique_texts = list(dict.fromkeys(miss_texts))
            fresh = embed_fn(unique_texts)
            by_text = dict(zip(unique_texts, fresh, strict=True))
            for i, text in zip(miss_idx, miss_texts, strict=True):
                vec = by_text[text]
                vectors[i] = vec
                self.put_vector(text, vec)

        misses = len({t for t in miss_texts})
        return [v if v is not None else [] for v in vectors], hits, misses


class ExtractionCache(_JsonDirCache):
    """Кэш результатов LLM-извлечения сущностей. Как embedding-кэш — не версионируется по
    экстрактору (LLM-вывод, перебиллить нельзя), namespace по модели LLM.

    Каталог: ``{cache_dir}/extraction/{model}/`` — смена модели LLM даёт другой результат,
    но смена версии чанкера/кода кэш НЕ инвалидирует.
    """

    def __init__(self, cache_dir: str | Path, *, model: str) -> None:
        super().__init__(Path(cache_dir) / "extraction" / _slug(model))

    def get_fragment(self, body: str) -> dict | None:
        val = self.get(content_hash(body))
        return val if isinstance(val, dict) else None

    def put_fragment(self, body: str, fragment: dict) -> None:
        self.put(content_hash(body), fragment)
