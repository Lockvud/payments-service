# syntax=docker/dockerfile:1.10

FROM python:3.14.8-slim-bookworm AS builder

# uv закреплён на конкретной проверенной версии. Плавающий тег :0.5
# недопустим, он не даёт воспроизводимости и легко устаревает.
COPY --from=ghcr.io/astral-sh/uv:0.13.1 /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /app

# Слой зависимостей отдельно от исходников, кэш не сбрасывается при правках кода.
# --frozen на этом этапе: uv.lock уже скопирован и должен использоваться как есть,
# без попытки разрешить зависимости заново (в образе нет сети/индекса).
COPY pyproject.toml uv.lock README.md ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev

COPY alembic.ini ./
COPY alembic ./alembic
COPY src ./src

# --locked на финальном шаге: uv проверяет, что uv.lock соответствует pyproject.toml,
# и упадёт с ошибкой при рассинхроне. Это дешевле, чем ловить несоответствие в рантайме.
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev

FROM python:3.14.8-slim-bookworm AS runtime

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN groupadd --system app && useradd --system --gid app --home /app app
COPY --from=builder --chown=app:app /app /app
USER app
WORKDIR /app

# CMD по умолчанию, API. Consumer и outbox-publisher переопределяют command в docker-compose.yml
CMD ["uvicorn", "payments.main:app", "--host", "0.0.0.0", "--port", "8000"]