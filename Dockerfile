# Orchestrator only. The per-worker Syncthing uses the official
# syncthing/syncthing image (see docker-compose.yml / k8s.yaml).
# python3.13 = same interpreter the test/type gates run on (no version skew)
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

WORKDIR /app
# HOME + cache under /app so the non-root user can write them (no /home/app).
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy HOME=/app UV_CACHE_DIR=/app/.cache

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY modelsync ./modelsync
RUN uv sync --frozen --no-dev

# Run unprivileged. /data (state volume mount) is created owned by the app user;
# an empty named volume inherits this ownership on first use, so it stays
# writable without root. The Syncthing sidecar runs root by design; this doesn't.
RUN useradd --system --uid 10001 app \
    && mkdir -p /data \
    && chown -R app:app /app /data
USER app

EXPOSE 8585
# --no-sync: don't re-verify deps against the lock on every start (already synced)
HEALTHCHECK --interval=30s --timeout=4s --retries=3 \
    CMD ["uv", "run", "--no-sync", "python", "-c", \
         "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8585/app.js',timeout=3).status==200 else 1)"]
CMD ["uv", "run", "--no-sync", "modelsync"]
