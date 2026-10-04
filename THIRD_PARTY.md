# Third-party code (platform_memory)

This file records third-party code **vendored** (ported with adaptation) into
`platform_memory`, together with its licences. Vendoring means the algorithms and
structure are taken from the source but rewritten against our data models (Pydantic /
dataclass `Node`/`Edge`, Apache AGE, Eden AI). Every vendored file carries the original
author's copyright header.

This file lives **inside the package** so that `memory-service` stays a self-contained,
licence-clean unit that builds and ships on its own (isolation invariant).

> Migration note: the table below uses the **target** paths inside `platform_memory`.
> The 8 vendored files are moved verbatim (with their copyright headers preserved) from
> the frozen Nexus source tag `v-migrate-20260620` in Phase 1. Paths shown are the
> post-rename destinations; until Phase 1 lands the logic these are the planned homes.

## Runtime dependency outside the package (not vendored): platform-auth-sdk

- **Source:** separate repository `platform-auth-sdk` of the Taimen platform
  (path dependency `../../sdk/platform-auth-sdk`), licensed under Apache-2.0.
- **Why:** verification of platform IAM access tokens on the HTTP edge
  (`src/platform_memory/server/iam.py` only) — ADR-018; superproject ADR-0013/0030.
- **Optional:** the engine builds and runs without it — drop `server/iam.py` and the
  dependency, and static key grants (ADR-017) keep working; nothing else in the package
  imports `platform_auth`. Its own transitive dependencies are permissive
  (`pyjwt` MIT, `cryptography` Apache-2.0/BSD, `httpx` BSD-3).

## graphify

- **Source:** https://github.com/safishamsi/graphify (branch `v8`, commit `be3dcfc`, version 0.8.40)
- **Licence:** MIT — Copyright (c) 2026 Safi Shamsi
- **What was vendored (8 files):**
  - `graphify/_minhash.py` → `src/platform_memory/ingest/_minhash.py`
    (datasketch-compatible MinHash + band-LSH without scipy; ported almost verbatim)
  - `graphify/dedup.py` → `src/platform_memory/ingest/fuzzy_resolver.py`
    (fuzzy entity resolution: normalisation → entropy gate → MinHash/LSH blocking →
    Jaro-Winkler → shared-community boost → union-find; adapted to `Node` and a
    natural-key priority / arbitration policy instead of destructive auto-merge)
  - `graphify/semantic_cleanup.py` + `graphify/security.py` (sanitize helpers) →
    `src/platform_memory/core/llm_safety.py`
    (LLM trust boundary: validation of untrusted JSON extraction, dropping
    "sentences-as-nodes", sanitize_label/sanitize_metadata; adapted to our nodes/edges
    contract. Anti-prompt-injection is original platform_memory code on top)
  - `graphify/cache.py` → `src/platform_memory/ingest/cache.py`
    (semantic cache: skip unchanged notes; chunk-cache versioned by chunker version,
    embedding-cache keyed by model/dimension)
  - `graphify/cluster.py` → `src/platform_memory/graph/cluster.py`
    (Leiden/graspologic → Louvain fallback, large-community split, cohesion;
    ported almost verbatim — algorithm over an arbitrary networkx graph)
  - `graphify/analyze.py` → `src/platform_memory/graph/analyze.py`
    (god-nodes, cross-community bridges, auto-questions; code-specific layer —
    language families, import cycles, AST stubs — not ported, signals adapted to our domain)
  - `graphify/serve.py` (_pick_seeds/_bfs/_dfs/_subgraph_to_text) →
    `src/platform_memory/retrieval/expansion.py`
    (seed-break threshold, depth-bounded traversal with hub throttling, subgraph
    serialisation under a token budget with citations; lexical IDF scoring NOT ported —
    seeds come from pgvector)
  - `graphify/serve.py` (MCP scaffold) →
    `src/platform_memory/mcp/server.py`
    (low-level mcp.server.Server, stdio + Streamable HTTP transports, api-key middleware,
    per-session state; graph source is the AGE store, tools query_graph/get_node/
    get_neighbors/shortest_path rewritten onto GraphStore/retrieve_subgraph)

### MIT License (graphify)

```
MIT License

Copyright (c) 2026 Safi Shamsi

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

<!-- generated-dependencies -->

## Third-party Python dependencies of `memory-service`

Generated by `tools/generate_third_party.py` from the installed environment
(`uv.lock`); do not edit below the marker by hand. The machine-readable list is
`sbom.json` (CycloneDX 1.5). Licence names come from package metadata.

### Components under copyleft licences

The components below are used under the following conditions:

- linking is dynamic: the library is imported as is, there is no static linking;
- the library source is neither modified nor vendored into this repository
  (it is installed as a standard package from PyPI at the version pinned in `uv.lock`);
- interaction goes through this component's own adapters, which remain under
  this component's licence;
- the user keeps the ability to replace or rebuild the library.

- certifi 2026.6.17 — MPL-2.0
- psycopg 3.3.4 — LGPL-3.0-only
- psycopg-binary 3.3.4 — LGPL-3.0-only
- tqdm 4.69.0 — MPL-2.0 AND MIT

### Other third-party components

- annotated-doc 0.0.4 — MIT
- annotated-types 0.7.0 — MIT License
- anyio 4.14.2 — MIT
- attrs 26.1.0 — MIT
- cffi 2.1.0 — MIT-0
- click 8.4.2 — BSD-3-Clause
- cryptography 50.0.1 — Apache-2.0 OR BSD-3-Clause
- distro 1.9.0 — Apache License, Version 2.0
- fastapi 0.139.2 — MIT
- h11 0.16.0 — MIT
- httpcore 1.0.9 — BSD-3-Clause
- httptools 0.8.0 — MIT
- httpx 0.28.1 — BSD-3-Clause
- httpx-sse 0.4.3 — MIT
- idna 3.18 — BSD-3-Clause
- Jinja2 3.1.6 — BSD License
- jiter 0.16.0 — MIT
- jsonschema 4.26.0 — MIT
- jsonschema-specifications 2025.9.1 — MIT
- markdown-it-py 4.2.0 — MIT License
- MarkupSafe 3.0.3 — BSD-3-Clause
- mcp 1.28.1 — MIT
- mdurl 0.1.2 — MIT License
- networkx 3.6.1 — BSD-3-Clause
- numpy 2.5.1 — BSD-3-Clause AND 0BSD AND MIT AND Zlib AND CC0-1.0
- openai 1.109.1 — Apache-2.0
- pgvector 0.5.0 — MIT
- pycparser 3.0 — BSD-3-Clause
- pydantic 2.13.4 — MIT
- pydantic-settings 2.14.2 — MIT
- pydantic_core 2.46.4 — MIT
- Pygments 2.20.0 — BSD-2-Clause
- PyJWT 2.15.1 — MIT
- python-dotenv 1.2.2 — BSD-3-Clause
- python-multipart 0.0.32 — Apache-2.0
- PyYAML 6.0.3 — MIT
- RapidFuzz 3.14.5 — MIT
- referencing 0.37.0 — MIT
- rich 15.0.0 — MIT
- rpds-py 2026.6.3 — MIT
- shellingham 1.5.4 — ISC License
- sniffio 1.3.1 — MIT OR Apache-2.0
- sse-starlette 3.4.5 — BSD-3-Clause
- starlette 1.3.1 — BSD-3-Clause
- typer 0.27.0 — MIT
- typing-inspection 0.4.2 — MIT
- typing_extensions 4.16.0 — PSF-2.0
- uvicorn 0.51.0 — BSD-3-Clause
- uvloop 0.22.1 — MIT License
- watchfiles 1.2.0 — MIT
- websockets 16.1.1 — BSD-3-Clause
