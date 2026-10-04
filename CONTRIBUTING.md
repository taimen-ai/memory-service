# Contributing to Taimen Memory Service

Thank you for taking the time to contribute. This repository holds the
**Taimen Memory Service** (`platform-memory`): a product-neutral context
memory engine — a typed knowledge graph in PostgreSQL tables with vector, lexical and
graph retrieval on pgvector — with an HTTP service, an MCP server, a CLI and a
thin client SDK. It is one component of Taimen, an organizational runtime in
which people, AI agents, workflows and services execute the work of an
organization; the platform is developed in the open under the Apache License 2.0.

## Before you start

- Read the platform overview in the
  [guide](https://github.com/taimen-ai/taimen/tree/main/guide/docs/overview)
  (in Russian).
  This component keeps its own series of architecture decision records in
  [`docs/decisions/`](docs/decisions/) (`ADR-NNN-<slug>.md`, referenced from the
  platform registry as `MEM-ADR-NNN`). ADRs are written in Russian with an
  English title line; English summaries are provided on request in the ADR's
  discussion.
- The consumer-facing contract is [`docs/INTEGRATION.md`](docs/INTEGRATION.md)
  and the conceptual model is [`docs/context-engine.md`](docs/context-engine.md).
  The invariants of the package (isolation, backward compatibility of
  `/api/brain/*`, self-contained build and tests, no secrets or customer data in
  the repository) are listed in [`AGENTS.md`](AGENTS.md) and apply to human
  contributors too.
- Check the open issues before starting a large change. For anything that changes an
  API, a data model or a service boundary, open an issue first and propose an ADR.

## Contributor License Agreement

We require a signed Contributor License Agreement (CLA) for every
contribution, so that the project can be relicensed or defended without
tracking down every author. The CLA is checked by cla-assistant on each pull
request; you sign once.

- Individuals: [`cla/CLA-individual.md`](https://github.com/taimen-ai/taimen/blob/main/cla/CLA-individual.md)
- Companies contributing on behalf of employees: [`cla/CLA-entity.md`](https://github.com/taimen-ai/taimen/blob/main/cla/CLA-entity.md)

The CLA grants the project a copyright and patent licence to your
contribution; you keep your copyright.

## Development setup

The component is a separate repository (a git submodule of the Taimen umbrella)
managed with [uv](https://docs.astral.sh/uv/). It depends on
`platform-auth-sdk` (another submodule of the umbrella) by path
(`../../sdk/platform-auth-sdk`, see `[tool.uv.sources]` in `pyproject.toml`), so
either work from the umbrella checkout (`services/memory-service` and
`sdk/platform-auth-sdk`, TAI-ADR-0064) or keep both checked out in that layout:

```bash
git clone --recurse-submodules <umbrella-url> taimen && cd taimen/services/memory-service
# or: clone this repository to services/memory-service and platform-auth-sdk to sdk/platform-auth-sdk

uv sync --extra mcp        # runtime + dev tools (pytest, ruff, httpx, the client SDK) from uv.lock
uv run pytest tests/ -q    # unit tests; no external services required
uv run ruff check . && uv run ruff format --check .
```

The same checks are `make` targets — `make install`, `make lint`, `make test`,
`make check` (lint + test) — and CI (`.github/workflows/ci.yml`) calls exactly
these targets.

Requirements: Python ≥ 3.12 and, for the integration tests only, Docker. The
engine runs fully offline on the `fake` embeddings and `echo` LLM providers
(`CB_EMBEDDING_PROVIDER=fake`, `CB_LLM_PROVIDER=echo`); no API keys are needed
for development.

The integration tests (`tests/integration/`, real PostgreSQL + pgvector) and the
benchmarks are skipped unless `CB_TEST_DATABASE_URL` points at a throwaway
database. No graph extension and no superuser are required (MEM-ADR-023): create
the `vector` and `pg_trgm` extensions once, then any role owning the database works:

```bash
docker run -d --name memtest-db -e POSTGRES_USER=brain -e POSTGRES_PASSWORD=brain \
  -e POSTGRES_DB=company_brain -p 127.0.0.1:5435:5432 pgvector/pgvector:pg16
docker exec memtest-db psql -U brain -d company_brain \
  -c 'CREATE EXTENSION IF NOT EXISTS vector' -c 'CREATE EXTENSION IF NOT EXISTS pg_trgm'
CB_TEST_DATABASE_URL=postgresql://brain:brain@127.0.0.1:5435/company_brain \
  uv run pytest tests/ -q                          # unit + integration + E2E scenario
CB_TEST_DATABASE_URL=postgresql://brain:brain@127.0.0.1:5435/company_brain \
  uv run python benchmarks/bench.py --size 10000   # ingest rps + retrieval latencies
```

The client SDK in `client/` (`platform-memory-client`) is a separate
distribution with its own `pyproject.toml`; the service installs it from the
local path only for `tests/client`. The service image is built with the root of
the umbrella as the context, because the SDK is at `sdk/platform-auth-sdk`:
`cd ../.. && docker build -f services/memory-service/Dockerfile -t memory-service .`
(`deploy/` has a Compose setup and an offline `smoke-test.sh`).

## Pull requests

- One logical change per pull request; keep the history linear (rebase, no
  merge commits).
- Tests and `ruff check` / `ruff format --check` must pass; behaviour changes
  come with tests (unit tests for everything that does not need the database,
  integration tests under `tests/integration/` for the PostgreSQL/pgvector layer).
- Commit messages explain *why*, not *what*; reference the ADR or issue.
- Existing `/api/brain/*` routes and their response schemas are not broken:
  extend additively, and put breaking changes behind a new versioned path with
  an ADR. Public API changes (routes, schemas, MCP tools, `CB_*` variables)
  update `docs/INTEGRATION.md` and the README and, when they break
  compatibility, the component's release notes.
- New dependencies keep `THIRD_PARTY.md` up to date; vendored code keeps the
  original copyright header.
- The pull request template asks you to confirm the CLA and that no secrets,
  customer data or internal hostnames are included.

## Reporting bugs and security issues

Bugs: open an issue in this repository with the version, steps to reproduce
and logs. Security issues: see [SECURITY.md](SECURITY.md) and do not open a
public issue.

## Code of conduct

This project follows the [Contributor Covenant](CODE_OF_CONDUCT.md).
