# Context Memory Engine — концептуальная модель

Как memory-service хранит знания и собирает контекст (ADR-016). Формула:

> **Observations — свидетельства. Facts — интерпретации. Documents — источники.
> Граф выражает связи. Retrieval находит кандидатов. Context Compiler решает,
> что полезно сейчас.**

И: текущее состояние приложения может передаваться как *ephemeral context*, но
движок памяти **не является source of truth для операционного состояния**.

## Формы памяти

```
Source (внешняя система)
  │
  ▼
Observation ─────────────┐
  │                      │ provenance
  ▼                      │
Episode                  │
  │                      │
  ├── Entity             │
  ├── Fact               │
  └── Relation           │
        │                │
        ▼                │
 Knowledge Graph (SQL)   │
        │                │
        ├───────────────┐│
        │               ││
Documents ── Vector ────┤│
Documents ── FTS/trgm ──┤│
                        ▼▼
                 Context Compiler
                        │
                        ▼
                   ContextPack
```

### Observation — сырое свидетельство

Immutable-запись того, что источник сообщил или наблюдал. Таблица
`observations` (реляционная — не граф): source identity
`(system, stream, external_id)`, `kind`, три времени (см. ниже), `scopes`,
человекочитаемый `content`, структурированный `data`, `assertions`,
`provenance.uri`, статус обработки. Повтор одного внешнего события не создаёт
дубликат: `observation_id` детерминирован от identity (`obs-<sha256[:24]>`).

Наблюдение — **evidence, а не истина**: движок никогда не «исправляет» и не
переписывает его. Меняется только статус обработки (и redaction при удалении).

### Episode — осмысленный фрагмент опыта (derived)

«Встреча случилась», «деплой упал», «решение принято». Физически — узел графа
`type='episode'` с provenance до наблюдений; отдельной таблицы нет. Создаётся
detерминированно (assertion `entity` c `type: "episode"`) или consolidation'ом.
Различие принципиально: observation — сырое свидетельство, episode —
представление памяти, выведенное из свидетельств.

### Fact — temporal-интерпретация

Ребро графа с `fact_id`, validity-интервалом и классом свидетельства:

```
Alice WORKS_ON Project X
  valid_from = 2026-08-01   valid_to = null (действует)
  evidence   = asserted     confidence = 0.9
  observation_ids = [obs-…] (чем доказан)
```

- **Три времени** различаются явно: `occurred_at` (event time — когда произошло),
  `ingested_at` (когда узнала память), `valid_from/valid_to` (validity —
  когда факт истинен). Полный bitemporal не строим, но модель его не блокирует.
- **Supersession**: новый факт с `supersedes` закрывает `valid_to` старого и
  связывает его `superseded_by` — история не уничтожается никогда. «Alice больше
  не работает над X» — это закрытие интервала, не удаление.
- **Конфликты не разрешаются автоматически**: перекрывающиеся версии одного
  `(subject, predicate)` обе остаются, retrieval помечает их `conflict: true` —
  решает потребитель, LLM не переписывает историю тихо.
- **Evidence class**: `asserted` (структурированное утверждение источника) >
  `extracted` (детерминированное правило) > `inferred` (LLM) ≈ `derived`
  (consolidation). Confidence — ranking/audit-сигнал, не математическая истина.
- Повтор того же утверждения из нового наблюдения **усиливает** факт
  (пополняет `observation_ids`, поднимает confidence до максимума), а не плодит
  рёбра.

### Document — источник-цитата

Существующий слой: чанки в pgvector + русский FTS + trigram, с `source_path`
для показа оригинала. Text-assertion наблюдения создаёт такой же цитируемый
чанк с provenance до наблюдения.

## Изоляция: namespace и scope

- **namespace** — жёсткая граница базы знаний (хранение, ADR-003): «банк» и
  «карта москвича» не пересекаются никогда. Модель one-customer-one-instance
  остаётся поддерживаемой и предпочтительной для on-prem.
- **scope** — метка видимости ВНУТРИ namespace: пара `"type:id"`
  (`project:alpha`, `task:T-1`, `user:alice`) без зашитых типов. Читающий
  запрос со scopes видит элементы с непустым пересечением **плюс** элементы
  вовсе без scopes (общая память KB). Движок не решает оргправа — вызывающее
  приложение передаёт разрешённые scopes (extension point для внешней
  авторизации).

## Ingest: детерминированная проекция без LLM

```
POST observation
  ├─ raw запись (идемпотентно, ACK сразу)              — всегда
  ├─ structured assertions → узлы/факты/чанки           — синхронно, без LLM
  └─ unstructured content → FTS/trgm observations       — виден сразу
       └─ LLM-extraction                                — только явно (consolidate)
```

Детерминированное событие системы не обязано проходить через LLM, чтобы узнать
свою семантику (§28 ТЗ). Ошибка проекции не теряет сырую запись: статус
`partially_processed`/`failed` + идемпотентный redrive (`cb consolidate`).

### Уровни консистентности (§31)

| Что | Когда видно retrieval |
|---|---|
| Сырое наблюдение (lexical/recent-каналы) | сразу после ACK (read-after-ingest) |
| Узлы/факты из structured assertions | сразу после ACK (синхронная проекция) |
| Вектор text-assertion | после embedding (синхронно при доступном провайдере, иначе после consolidate) |
| LLM-derived знания | только после явного extraction |

## Context Compiler

`build_context(ContextRequest) -> ContextPack` — сборка evidence без
LLM-синтеза (синтез — отдельный слой `query()`).

```
ContextRequest{query, scopes, anchors, ephemeral_context, budget, strategy?, as_of?}
  ├─ resolve: идентификаторы query/anchors → сущности журнала сверки (ключ, псевдоним,
  │           нормализованная форма, суффикс ключа; без LLM; MEM-ADR-020 амендмент)
  ├─ lexical: точные идентификаторы (UUID/SHA/коды/файлы) + русский FTS
  ├─ vector:  pgvector по чанкам
  ├─ graph:   expansion от разрешённых сущностей, якорей и топ-хитов (max_depth ≤ 2, max_nodes, max_edges)
  ├─ facts:   temporal-факты вокруг якорей (as_of; конфликты помечаются)
  └─ recent:  свежие наблюдения по scopes
        ▼
  RRF-merge → скоринг → dedup → секции → token budget → ContextPack + trace
```

### Скоринг (документированный, детерминированный — §23)

Базис — Reciprocal Rank Fusion: `Σ weight_канала / (60 + rank + 1)`; веса:
resolve 1.0, lexical 1.0, vector 1.0, facts 1.0, graph 0.7, recent 0.6. Аддитивные бонусы:

| Сигнал | Бонус |
|---|---|
| Сущность разрешена по ключу, псевдониму или нормализованной форме | +0.040 |
| Сущность разрешена по суффиксу ключа | +0.015 |
| Точное совпадение идентификатора из запроса | +0.010 |
| Элемент в явно запрошенном scope | +0.004 |
| Действующий факт (интервал не закрыт) | +0.003 |
| Сила свидетельства (за ранг: asserted=3 → +0.006) | +0.002/ранг |
| Свежесть (эксп. спад, полупериод 14 дней) | ≤ +0.005 |

Разрешённые сущности несут `evidence: resolved` (бонус силы свидетельства не
начисляется — у них свой бонус); документ, найденный только вектором, —
`evidence: inferred` (пометка без бонуса, порядок выдачи не меняется).

Канал `resolve` раздаёт лимит разрешённых сущностей (20) в два прохода: сначала
точные совпадения (ключ, псевдоним, нормализованная форма) всех кандидатов, затем
суффиксные на остаток. Видимость разрешённой сущности проверяется по `payload.scopes`
её версии в журнале сверки; при нескольких источниках одного ключа текст и цитата
берутся из видимой версии. Регистр и ведущие нули номеров (`ADR-62`, `cp-adr-0062`)
не нормализуются — формы номеров задают `aliases` доменного пакета.

Все слагаемые возвращаются в `signals` каждого элемента — выдача объяснима,
а `GET /api/memory/context/trace/{id}` показывает счётчики каналов, ranking- и
budget-решения компиляции.

### Бюджет (§21)

Оценка размера — `len(text)/CB_CONTEXT_CHARS_PER_TOKEN` (дефолт 4.0). Секция
`current` (ephemeral вызывающего) включается первой; остальные элементы
отбираются жадно по score. Что не влезло — в `budget.dropped` с причиной.
`ephemeral_context` не сохраняется движком (в trace — только размер).

### Секции ContextPack (§22)

`current` → `relevant_facts` → `episodes` → `documents` → `related_entities` →
`recent_observations`. `to_text()` — рендер по умолчанию; потребитель волен
собрать свой промпт из machine-readable секций.

### Типизированный обход (MEM-ADR-020)

`compile_typed_context({anchors, traverse, as_of, namespaces})` — второй режим
компилятора для контекста задачи: вместо ранжирования кандидатов — детерминированный
обход по связям доменных пакетов.

```
anchors[{kind?, value}]
  └─ natural_key → псевдонимы ключа (aliases) → idPatterns вида → normalized → suffix
     → (semantic, только allow_semantic; evidence inferred); пачкой на все якоря и namespaces
traverse[{relation, direction in|out|both, depth, limit, from anchors|previous}]
  └─ BFS по рёбрам relation, валидным на as_of (закрытые не проходятся), limit новых на шаг;
     уровень BFS — запрос рёбер по graphid фронта и запрос концов по их меткам
        ▼
  разделы по видам + факты + used{entities, facts, snapshots} + trace
```

Сущность видна на `as_of`, если в журнале снимков (reconcile) есть её версия,
валидная на этот момент (атрибуты — из версии); сущность без журнала — по своему
`valid_from/valid_to`. Видимость по principal (MEM-ADR-019) и PII-барьер — как у
`build_context`. Прежний graph-канал `build_context` (`expand_neighbors`) по-прежнему
не смотрит на валидность рёбер — см. «Последствия» MEM-ADR-020.

## Provenance (§25)

Каждый значимый элемент восстановим до источника:

```
ContextItem
  → Fact (observation_ids) / Chunk (node_key, meta.observation_id) / Observation
    → Observation (source.system/stream/external_id, provenance.uri)
      → оригинальное событие внешней системы
```

## Deletion (§40)

`DELETE /api/memory/observations/{id}?mode=redact|purge`: чанки наблюдения
удаляются, из фактов вычёркивается свидетельство, факты без оставшихся
свидетельств получают `evidence_lost` и выпадают из retrieval (но не из графа —
аудит). Событие удаления — в аудит-контуре KB (ADR-001). Dangling truth без
source awareness не остаётся.

## LLM optionality (§27)

Без LLM работают: ingest наблюдений и документов, structured-проекция,
lexical/vector/graph/hybrid retrieval, build_context, provenance, trace,
consolidation-redrive. LLM нужен только для: extraction unstructured-контента,
суммаризации, меток сообществ, синтеза ответа (`query(synthesize=true)`).
Движок никогда не хранит скрытый chain-of-thought моделей (§41) — принимаются
только явные наблюдения, сводки, tool-результаты, документы и структурные факты.

## Бенчмарки и seam хранилища

`benchmarks/bench.py` — ingest rps, латентности lexical/vector/graph, hybrid
context на бюджетах (датасеты 1k/10k/100k). `benchmarks/graph_ops.py` —
протокол `GraphBenchOps` (entity lookup, 1-hop, 2-hop, temporal filter,
neighborhood, shortest path): будущий альтернативный графовый движок
сравнивается с текущей реализацией протокола (таблицы PostgreSQL, MEM-ADR-023), без переписывания Context API
(ADR-015: runtime-абстракцию хранилища не строим). Реализации — `TableBenchOps` (таблицы)
и `AgeBenchOps` (запросы прежнего кода на AGE); `bench.py engines` сравнивает их на одних
данных вместе с гибридным запросом Context Compiler и JIT вкл./выкл., `graph_gen.py`
генерирует граф для такого сравнения — замер MEM-ADR-023 п.6, отчёт
[`benchmarks/graph-engines.md`](../benchmarks/graph-engines.md). `benchmarks/typed_context.py` —
профиль `/context/typed` на графе размера staging (снимки пяти репозиториев и узлы
чужого namespace): латентность по фазам и число SQL (MEM-ADR-020, амендмент
«пакетное разрешение якорей»).
