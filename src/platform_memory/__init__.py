"""platform_memory — product-neutral memory engine for the Taimen platform.

Typed knowledge graph (PostgreSQL tables) + vector retrieval (pgvector) over a document
vault, exposed via HTTP (``/api/brain/*``), MCP and CLI.

ISOLATION INVARIANT: this package is licence- and dependency-isolated. It MUST NOT
import any other ``platform_*`` package and MUST NOT depend on the infrastructure of
other platform services (shared LLM layers, external vector stores,
asyncpg/SQLAlchemy/Alembic). It keeps its own config (``CB_*``), its own psycopg3 +
PostgreSQL + pgvector data layer and its own ``openai`` LLM/embeddings client, so the whole
repository (with its own LICENSE + THIRD_PARTY.md) builds and runs on its own.

Public API
----------
Configuration:      ``Settings``, ``get_settings``
Core graph types:   ``Node``, ``Edge``, ``Chunk``, ``Provenance``
Retrieval:          ``query``, ``retrieve``, ``Answer``, ``Retrieval``, ``Source``,
                    ``write_and_index_fact``
Ingestion:          ``ingest_vault``, ``IngestStats``
Low-level graph:    ``GraphStore``
Context engine:     ``Observation``, ``retain_observation(s)``, ``build_context``,
                    ``ContextRequest``, ``ContextPack``, ``FactStore``,
                    ``consolidate``, ``delete_observation`` (ADR-016)

Quickstart::

    from platform_memory import get_settings, ingest_vault, query

    settings = get_settings()                      # reads CB_* env (DB, LLM, embeddings)
    ingest_vault(settings, "/path/to/vault")       # project a vault into graph + index
    answer = query(settings, "who owns billing?")
    print(answer.text, answer.sources)
"""

from platform_memory.context import (
    ContextItem,
    ContextPack,
    ContextRequest,
    ContextSection,
    build_context,
)
from platform_memory.core import Chunk, Edge, Node, Provenance, Settings, get_settings
from platform_memory.core.observations import Observation
from platform_memory.graph import GraphStore
from platform_memory.graph.facts import FactStore
from platform_memory.ingest import IngestStats, ingest_vault
from platform_memory.observations import ObservationRecord, ObservationStore
from platform_memory.observations.consolidate import consolidate, delete_observation
from platform_memory.observations.ingest import (
    process_observations,
    retain_observation,
    retain_observations,
)
from platform_memory.retrieval import (
    Answer,
    Retrieval,
    Source,
    query,
    retrieve,
    write_and_index_fact,
)

__all__ = [
    # configuration
    "Settings",
    "get_settings",
    # core graph types
    "Node",
    "Edge",
    "Chunk",
    "Provenance",
    # retrieval
    "query",
    "retrieve",
    "Answer",
    "Retrieval",
    "Source",
    "write_and_index_fact",
    # ingestion
    "ingest_vault",
    "IngestStats",
    # low-level graph access
    "GraphStore",
    # context memory engine (ADR-016)
    "Observation",
    "ObservationRecord",
    "ObservationStore",
    "FactStore",
    "retain_observation",
    "retain_observations",
    "process_observations",
    "consolidate",
    "delete_observation",
    "build_context",
    "ContextRequest",
    "ContextPack",
    "ContextSection",
    "ContextItem",
]
