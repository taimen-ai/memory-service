# platform-memory

Product-neutral **Context Memory Engine** for AI applications:

```
observe → normalize → retain → consolidate → retrieve → assemble context → provide provenance
```

A typed knowledge graph (plain PostgreSQL tables, MEM-ADR-023) with **temporal facts**, first-class
**observations** (immutable evidence with idempotent ingest), vector + lexical +
graph **hybrid retrieval** over a document vault, and a **Context Compiler** that
assembles a token-budgeted, provenance-rich `ContextPack` for any LLM/harness/agent —
exposed through a Python API, an HTTP service, an MCP server and a CLI.

External systems (issue trackers, CRM, email, coding harnesses, agent runtimes)
talk to one generic Memory API; domain-specific adapters live outside this package.
Conceptual model: [`docs/context-engine.md`](./docs/context-engine.md).

> **Isolation invariant (AGENTS.md, invariant 1).** This package is licence- and
> dependency-isolated. The memory engine never imports another `platform_*` package and
> keeps its own config (`CB_*`), its own `psycopg3 + PostgreSQL + pgvector` data layer and its
> own `openai` LLM/embeddings client, so this repository (Apache-2.0 `LICENSE` +
> `THIRD_PARTY.md`) builds, tests and ships on its own.
>
> **One deliberate exception (ADR-018; superproject ADR-0013/0030):** the HTTP edge
> verifies platform IAM access tokens through the shared enforcement SDK
> `platform-auth-sdk` (`platform_auth`), like every other resource service. The import
> is confined to `server/iam.py` and the SDK is optional: a build without it drops that
> module and the dependency and keeps the static-key grants (ADR-017).

## Requirements

- Python ≥ 3.12
- PostgreSQL 16 with the **pgvector** extension (`pg_trgm` recommended) — e.g. the
  official `pgvector/pgvector:pg16` image. The graph lives in ordinary tables
  (MEM-ADR-023); no graph extension and no superuser are needed: the service runs as
  the owner of its database, and the extensions are created by a database setup step
  (`CREATE EXTENSION vector; CREATE EXTENSION pg_trgm;`), not by the service.
- An OpenAI-compatible endpoint for embeddings and (optionally) LLM synthesis —
  offline `fake`/`echo` providers are available for tests and air-gapped runs.

## Install

```bash
pip install platform-memory                 # core
pip install "platform-memory[leiden]"       # + graspologic (better community detection)
pip install "platform-memory[mcp]"          # + MCP graph server (stdio / streamable HTTP)
```

## Configuration

All settings are read from the environment with the `CB_` prefix (Pydantic Settings).
The essentials:

| Variable | Default | Notes |
|----------|---------|-------|
| `CB_DATABASE_URL` | `postgresql://brain:brain@localhost:5432/company_brain` | or set `POSTGRES_*` parts |
| `CB_GRAPH_NAME` | `company_brain` | PostgreSQL schema of the graph tables (`graph_nodes`, `graph_edges`) |
| `CB_DB_JIT` | `false` | PostgreSQL JIT for service connections. Off by default since the Apache AGE era, when bogus cost estimates made LLVM recompile every graph query (ADR-010); whether it is still needed on the graph tables is decided by the MEM-ADR-023 benchmark. |
| `CB_EMBEDDING_PROVIDER` | `openai` | `openai` (needs `CB_EMBEDDING_API_KEY`) or `fake` |
| `CB_EMBEDDING_API_KEY` | — | required unless provider is `fake` |
| `CB_LLM_PROVIDER` | `openai` | `openai` (needs `CB_LLM_API_KEY`) or `echo` (offline) |
| `CB_LLM_API_KEY` | — | required unless provider is `echo` |
| `CB_VAULT_PATH` | — | document vault to ingest |
| `CB_SERVER_API_KEY` | — | if set, the HTTP service requires `Authorization: Bearer …` (full access to every namespace) |
| `CB_SERVER_TIMING` | `false` | per-phase request timing: `Server-Timing` response header and a log line (`platform_memory.timing`) with auth, visibility and engine phases (e.g. `typed.anchors`, `typed.traverse`); for diagnostics, exposes internals — keep off in public setups (MEM-ADR-020 amendment) |
| `CB_API_KEYS` | — | optional JSON registry of keys with per-namespace prefix grants (`{prefix, read, write}`, optional `pii`, optional `service` — domain pack registration, MEM-ADR-020); see ADR-017 |
| `CB_IAM_ENABLED` | `false` | also accept platform IAM access tokens (Bearer JWT) next to the static keys; see ADR-018 |
| `CB_IAM_ISSUER` | — | exact `iss` of the token (IAM public issuer URL) |
| `CB_IAM_JWKS_URL` | — | IAM JWKS endpoint (internal address; keys are cached, rotation-aware) |
| `CB_IAM_AUDIENCE` | `memory-service` | exact `aud` the token must carry (lists are rejected) |
| `CB_IAM_LEEWAY_SECONDS` | `5` | clock-skew tolerance for `exp`/`nbf`/`iat` |
| `CB_CORE_ONLY` | `false` | restrict `reconcile`, `packages` (including `GET`) and `namespaces/{ns}/kinds` to the core identity — the service scope (IAM `memory:service`, registry key with `"service": true`); everyone else gets `403`, even with a namespace grant (MEM-ADR-020 amendment) |
| `CB_CORE_IDENTITIES` | — | extra caller labels trusted as the core, comma-separated (registry key `name`, `base`/`pii-full` for legacy keys, `iam:<principal_type>:<principal_id>`); a non-empty list also turns the restriction on |
| `CB_POLICY_ENABLED` | `false` | principal-level visibility (MEM-ADR-019): IAM tokens of humans/agents get readable namespaces and scopes from policy-service; service accounts with `memory:on-behalf` pass `allowedNamespaces`/`allowedScopes`; any caller may pass them to narrow its visibility — intersection, never wider (MEM-ADR-021) |
| `CB_POLICY_URL` | `http://localhost:8030` | policy-service base URL |
| `CB_POLICY_AUDIENCE` | `policy-service` | IAM audience of policy-service |
| `CB_POLICY_CACHE_TTL_SECONDS` | `5` | cache of the visibility answer per subject |
| `CB_IAM_BASE_URL`, `CB_IAM_CLIENT_ID`, `CB_IAM_CLIENT_SECRET` | — | service identity of memory-service in IAM (client credentials) for the policy-service call |

IAM token → grants: scopes `memory:read` / `memory:write` are the read/write flags,
`memory:pii` is the full-PII entitlement (ADR-002), `memory:service` is the core
identity: registering domain packs (MEM-ADR-020) and, with `CB_CORE_ONLY`/
`CB_CORE_IDENTITIES`, the only scope admitted to the core routes; namespaces are `tenant:<tenant_id>`
and its subtree `tenant:<tenant_id>:*` (how the Control Plane names tenant memory) plus
every entry of the optional `memory_namespaces` claim. Any token defect → `401`
(same body as a wrong key); JWKS unreachable → `503` (fail closed).

See `src/platform_memory/core/config.py` for the full list.

## Python quickstart

```python
from platform_memory import get_settings, ingest_vault, query

settings = get_settings()                      # reads CB_* env
ingest_vault(settings, "/path/to/vault")       # project a vault into graph + index
answer = query(settings, "who owns billing?")
print(answer.text)
for src in answer.sources:                     # citations back to source files
    print(" -", src)
```

Context engine quickstart (no LLM required — deterministic core):

```python
from platform_memory import build_context, get_settings, retain_observation

settings = get_settings()
retain_observation(settings, {
    "source": {"system": "issue-tracker", "stream": "events", "external_id": "ev-1842"},
    "kind": "work.completed",
    "occurred_at": "2026-08-11T10:00:00Z",
    "scopes": ["project:alpha"],
    "content": "Regression was fixed",
    "assertions": [{"assert": "fact", "fact": {
        "subject": "person:alice", "predicate": "WORKS_ON", "object": "project:alpha",
        "valid_from": "2026-08-01T00:00:00Z"}}],
})
pack = build_context(settings, {
    "query": "Continue resolving the deployment issue",
    "scopes": ["project:alpha"],
    "ephemeral_context": {"current_state": "deploy pending"},
    "budget": {"tokens": 12000},
})
print(pack.to_text())        # or iterate pack.sections for machine-readable items
```

Public API (`from platform_memory import …`): `Settings`, `get_settings`,
`Node`, `Edge`, `Chunk`, `Provenance`, `query`, `retrieve`, `Answer`, `Retrieval`,
`Source`, `write_and_index_fact`, `ingest_vault`, `IngestStats`, `GraphStore` —
plus the context engine: `Observation`, `ObservationStore`, `FactStore`,
`retain_observation(s)`, `process_observations`, `consolidate`,
`delete_observation`, `build_context`, `ContextRequest`, `ContextPack`.

## CLI

```bash
cb init-db                       # create the graph schema + all tables (idempotent)
cb ingest --vault ./vault        # project a vault into graph + index
cb communities                   # Leiden/Louvain community detection + LLM labels
cb query "who owns billing?"     # ask a question, get an answer with citations
cb stats                         # graph statistics
cb observe '{"kind": "…", "content": "…"}'   # retain one observation (idempotent)
cb observations                  # recent observations + processing statuses
cb context "deploy issue" --scope project:alpha   # compile a ContextPack (debug)
cb trace ctx-…                   # why was that context assembled
cb consolidate                   # redrive unprocessed observations (idempotent)
cb merge-entities                # re-merge entity nodes from all snapshot sources (MEM-ADR-022)
cb redrive-legacy --writer-scope workspace:w1 --dry-run   # list pre-writer-visibility
                                 # observations with source.system — check it first
cb redrive-legacy --writer-scope workspace:w1   # redrive them (or --unrestricted /
                                 # --namespace-level); audited, MEM-ADR-019
cb migrate-graph-from-age --dry-run   # graph still in Apache AGE: preview the move
cb migrate-graph-from-age        # copy AGE graph into graph tables (MEM-ADR-023, idempotent;
                                 # per-namespace reconciliation, exit 1 on mismatch)
```

(`platform-memory-serve` runs the HTTP service; `cb`/`platform-memory-*` aliases are equivalent.)

## HTTP service

```bash
platform-memory-serve            # FastAPI app on CB_SERVER_HOST:CB_SERVER_PORT (default 127.0.0.1:8077)
```

Key routes: `GET /healthz`, `POST /api/brain/query`, `POST /api/brain/recall`,
`POST /api/brain/search`, `POST /api/brain/retain`, `POST /api/brain/facts`,
`POST /api/brain/audit`, `GET /api/brain/nodes`, `GET /api/brain/sources/{natural_key}`,
`GET /api/brain/stats`, `GET /api/brain/trace/{trace_id}`,
`POST /api/brain/documents` + `DELETE /api/brain/documents/{natural_key}` (batch ingest of
pre-chunked documents, ADR-017).

Context engine routes (additive; same auth and PII barrier):
`POST /api/memory/observations` (+ `:batch`), `GET/DELETE /api/memory/observations/{id}`,
`POST /api/memory/context`, `GET /api/memory/context/trace/{id}`,
`POST /api/memory/consolidate`. Contract: [`docs/INTEGRATION.md`](./docs/INTEGRATION.md).

Domain kinds as data (MEM-ADR-020; additive): the engine ships no domain in code —
entity kinds, relations and identifier patterns come as versioned **domain packs**
(`POST/GET /api/memory/packages`, service scope; the former business ontology is the
built-in `default` pack in `core/packs/default.json`). A knowledge base can register its
own **tenant pack** (`"scope": "tenant", "namespace": "<owner>"`, write grant on the
owner): it is visible only in the owner namespace and below it, is referenced as
`tenant:<name>[@<version>]`, and its pack, kind and relation names may not clash with
common packs (`409` with `detail.code`). A namespace can switch on strict
mode (`PUT /api/memory/namespaces/{ns}/kinds`) — entities of unknown kinds are then
rejected with `422` (for snapshots, relations and their end kinds are checked too); a
pack applies only in namespaces where it is enabled. `POST /api/memory/reconcile`
takes the pack's snapshot document as is (`pack, source, scope, snapshotId,
observedAt, entities, relations`) and reconciles it against the same `(source, scope)`
(opens new, supersedes changed incl. provenance, closes vanished; nothing is deleted;
a repeat of the last accepted `snapshotId` is a no-op `duplicate`, an earlier one
coming back after another (A → B → A) is applied anew; relations to entities not yet present are kept pending
and linked when the target arrives; the answer lists the `{kind, key}` of opened, changed
and closed nodes in `changes`, at most `CB_RECONCILE_CHANGES_LIMIT` (1000) per list,
with `truncated`; `dryRun: true` returns the plan without writing, `stateToken` +
`expectedState` apply exactly that plan or answer `409 snapshot_stale`, and
`conflicts` lists snapshot entities another source holds open), and `POST /api/memory/context/typed` walks typed relations
from anchors (natural key → key alias → `idPatterns`, semantic only on request) with
`direction`/`depth`/`limit`, only over facts valid at `as_of`; `where` filters anchor
candidates (semantic ones too) and the entities each step reaches by attributes (`eq`,
`in`, `prefix` by dot segments, `lte`/`gte` for numbers and ISO dates, `exists`). A pack
kind with `searchable: {fields}` has its entities embedded (title + those attributes) on
reconcile (not on `dryRun`) and reindexed on `PUT …/kinds`, so semantic anchors find
them by meaning. An entity written by several snapshot sources is **merged**, not
overwritten (MEM-ADR-022): an attribute given by one source survives another source's
snapshot that lacks it, a conflict of one attribute goes to the kind's `sourcePriority`
(glob patterns on `source`), then to the source that set that attribute last; `required`
of a strict kind is checked on the merge, not on each snapshot. The graph node and the
entity search row merge only versions with the same scopes, so a merge never widens
visibility. Give a kind fed by an accounting system and a CRM an explicit priority
(e.g. `legal_entity`: `"sourcePriority": ["erp:*"]`) — otherwise a CRM snapshot with a
PII-masked name of a sole proprietor, observed later, wins the name. After upgrading run
`cb merge-entities` once to re-merge existing nodes (otherwise the first `PUT …/kinds`
of each namespace re-merges it synchronously). New tables: `CB_DOMAIN_PACKS_TABLE` (+`_tenant`),
`CB_NAMESPACE_SETTINGS_TABLE`, `CB_SNAPSHOTS_TABLE` (+`_items`, `_state`),
`CB_ENTITY_EMBEDDINGS_TABLE` (entity embeddings of `searchable` kinds).

## Memory Console (admin UI)

Server-rendered admin interface at `GET /console` (Jinja2, no client-side framework):
overview, per-KB article table, single-article import, a Q&A demo page (question →
LLM-synthesized answer with source citations, same engine as `POST /api/brain/query`),
search verification with citations, delete with confirmation, and the KB audit trail.
It calls the engine in-process — the service API key is never sent to the browser.

Disabled by default. To enable:

| Variable | Default | Notes |
|----------|---------|-------|
| `CB_CONSOLE_ENABLED` | `false` | when `false`, every `/console` route returns 404 |
| `CB_CONSOLE_NAMESPACES` | — | CSV allowlist of knowledge bases shown in the UI (empty → default namespace only) |

**Security:** the console has no authentication of its own. Do not expose the
service port directly — put `/console` behind a reverse proxy / network policy
that authenticates the administrator (see `deploy/RUNBOOK.md`). One instance
serves one customer; namespaces separate that customer's knowledge bases.
Article import writes as a caller without workspaces (MEM-ADR-019): it never
overwrites an object scoped to a workspace or principal.

## Public demo showcase (ADR-009)

A public, **read-only** marketing surface at `GET /demo` — a "verifiable memory"
showcase (ask a question → top-k passages with score → open the original source with
provenance). Same in-process pattern as the console (the API key never reaches the
browser), but deliberately narrower: **one namespace, read-only, masked, rate-limited.**
Disabled by default.

| Variable | Default | Notes |
|----------|---------|-------|
| `CB_DEMO_PUBLIC_ENABLED` | `false` | when `false`, every `/demo` route returns 404 |
| `CB_DEMO_NAMESPACE` | `demo` | the single knowledge base the demo exposes (client cannot override) |
| `CB_DEMO_RATE_LIMIT` | `30` | requests/min per IP to `/demo/api/*` (`0` = unlimited) |
| `CB_DEMO_BRAND_NAME` / `CB_DEMO_BRAND_TAGLINE` / `CB_DEMO_CTA_URL` / `CB_DEMO_DOMAIN` | neutral | brand/domain, all in config — no brand hardcoded in markup |

Ships a browser-only **offline** build too (`demo/build_canned.py` → self-contained
`dist/index.html`). **Do not enable on a contour with real data** — the demo is for a
clean synthetic knowledge base only. Full guide: `demo/README.md`.

## Client SDK (`client/`)

`platform-memory-client` — the canonical HTTP client for this service (superproject
ADR-0030): `MemoryClient` / `AsyncMemoryClient` over `/api/brain/*` and `/api/memory/*`,
httpx + pydantic only, bearer as a string, a callable or an async credential provider
(IAM access tokens, ADR-018). It ships as a separate distribution in `client/` so that
consumers add it as the path dependency `../memory-service/client` without the engine's
stack; the service itself installs it only for `tests/client`. See `client/README.md`.

## Mounting into a host application

The engine stays decoupled: a host FastAPI app can mount the router from
`platform_memory.server.app` under its own prefix, rather than the package reaching
into the host. This keeps `platform-memory` reusable across products.

## MCP server

```bash
platform-memory-mcp              # stdio (local) or streamable HTTP (shared, api-key gated)
```

Tools: `query_graph`, `get_node`, `get_neighbors`, `get_community`, `shortest_path`,
`build_context`, `remember_observation`.

The MCP server is a service surface: it is authorized by the static service key
(`CB_SERVER_API_KEY`) or runs locally over stdio, and sees the whole knowledge base,
like the static key on the HTTP API. An agent acting for an end user passes
`allowed_scopes` (list of `"type:id"`) to any tool to narrow visibility (MEM-ADR-019/021):
objects outside it are neither returned nor overwritten. End users should use the HTTP
API with an IAM token. Over streamable HTTP without an api-key the server binds only to a
loopback address (`127.0.0.1`, `::1`, `localhost`); any other `--host` without
`CB_SERVER_API_KEY`/`--api-key` refuses to start.

## Deployment

`deploy/` contains a reproducible isolated-instance setup (one customer = one
instance): `docker-compose.yml` (service + dedicated PostgreSQL database),
`.env.example`, an end-to-end `smoke-test.sh` that runs fully offline on the
`fake`/`echo` providers, and `RUNBOOK.md` (reverse proxy requirements, backup /
restore, upgrade).

## Development

```bash
uv sync --extra mcp        # reproducible env: runtime + dev tools (pytest, ruff) from uv.lock
uv run pytest tests/ -q    # unit tests, no external services required
uv run ruff check . && uv run ruff format --check .
```

Integration tests (real PostgreSQL + pgvector, offline fake/echo providers) and the
benchmark suite need a throwaway database (an ordinary, non-superuser role is enough
once the extensions exist):

```bash
docker run -d --name memtest-db -e POSTGRES_USER=brain -e POSTGRES_PASSWORD=brain \
  -e POSTGRES_DB=company_brain -p 127.0.0.1:5435:5432 pgvector/pgvector:pg16
docker exec memtest-db psql -U brain -d company_brain \
  -c 'CREATE EXTENSION IF NOT EXISTS vector' -c 'CREATE EXTENSION IF NOT EXISTS pg_trgm'
CB_TEST_DATABASE_URL=postgresql://brain:brain@127.0.0.1:5435/company_brain \
  uv run pytest tests/ -q                      # unit + integration + E2E DoD scenario
CB_TEST_DATABASE_URL=postgresql://brain:brain@127.0.0.1:5435/company_brain \
  uv run python benchmarks/bench.py --size 10000   # ingest rps + retrieval latencies
```

## Licence

Apache-2.0 — see [`LICENSE`](./LICENSE) and third-party attributions in
[`THIRD_PARTY.md`](./THIRD_PARTY.md).
