FROM python:3.13-slim-bookworm

COPY --from=ghcr.io/astral-sh/uv:0.9.18 /uv /uvx /bin/

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project \
    && .venv/bin/python -m playwright install --with-deps --only-shell chromium \
    && uv cache clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*

COPY crawler ./crawler

RUN useradd --create-home --uid 1000 crawler \
    && mkdir -p /data \
    && chown -R crawler:crawler /app /data \
    && chmod -R a+rX /ms-playwright

ENV PATH="/app/.venv/bin:$PATH"
USER crawler

CMD ["celery", "-A", "crawler.worker", "worker", "--loglevel=info", "-E", "--concurrency=1", "-Q", "celery"]
