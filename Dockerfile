# ---------------------------------------------------------------------------
# platform-memory — standalone service image (HTTP memory API).
#
# BUILD CONTEXT IS THE SUPERPROJECT ROOT (ADR-018; superproject ADR-0013/0030):
# the HTTP edge verifies IAM access tokens through the shared `platform-auth-sdk`,
# which is a path dependency at `sdk/platform-auth-sdk` of the umbrella layout
# (this repository is `services/memory-service`, TAI-ADR-0064). Like every other
# resource service (entitlement-service, control-plane) the image is therefore
# built with `-f services/memory-service/Dockerfile` from the root:
#
#   docker build -f services/memory-service/Dockerfile -t memory-service .
#
# `Dockerfile.dockerignore` next to this file (BuildKit picks it over the root
# `.dockerignore`) limits the context to services/memory-service/ and
# sdk/platform-auth-sdk/.
#
# ISOLATION INVARIANT (CLAUDE.md, invariant 1) still holds for the engine: the SDK is
# confined to `server/iam.py`; nothing else from the superproject enters the image. A
# build without the SDK only needs to drop that module and the SDK dependency.
#
# Run:
#   docker run -e CB_DATABASE_URL=... -e CB_EMBEDDING_PROVIDER=fake -p 8077:8077 memory-service
# ---------------------------------------------------------------------------

# ---- Builder stage --------------------------------------------------------
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder

# The venv is created at /app/.venv (not the project default ./.venv) so its path is
# the same in the runtime stage: console-script shebangs embed the absolute path.
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0 \
    UV_PROJECT_ENVIRONMENT=/app/.venv

WORKDIR /app/services/memory-service

# Sibling enforcement SDK first: pyproject [tool.uv.sources] points at
# ../../sdk/platform-auth-sdk; the image keeps that relative path under /app.
COPY sdk/platform-auth-sdk /app/sdk/platform-auth-sdk

# Project metadata + lockfile: dependency layer is cached until the lock changes.
# hatchling reads README for the long description.
COPY services/memory-service/pyproject.toml services/memory-service/uv.lock \
     services/memory-service/README.md services/memory-service/LICENSE \
     services/memory-service/THIRD_PARTY.md ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project --no-editable

# Now the package itself, installed non-editable so the venv is self-contained
# (psycopg[binary] ships wheels: no libpq/build toolchain is needed at runtime).
COPY services/memory-service/src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable

# ---- Runtime stage --------------------------------------------------------
FROM python:3.12-slim-bookworm AS runtime

RUN groupadd --system --gid 999 appuser \
    && useradd --system --gid 999 --uid 999 --create-home appuser

COPY --from=builder --chown=appuser:appuser /app/.venv /app/.venv

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    # Bind to all interfaces inside the container (config default is 127.0.0.1).
    CB_SERVER_HOST=0.0.0.0 \
    CB_SERVER_PORT=8077

WORKDIR /app
USER appuser

EXPOSE 8077

# 127.0.0.1, not localhost: in slim images `localhost` may resolve to ::1 first while
# uvicorn listens on IPv4 only -> permanently "unhealthy" container.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8077/healthz', timeout=3)"]

# Console-script entry point from pyproject ([project.scripts]).
CMD ["platform-memory-serve"]
