FROM mcr.microsoft.com/devcontainers/python:3.14-bookworm

# uv закреплён на конкретной версии, как и в production образе.
COPY --from=ghcr.io/astral-sh/uv:0.13.1 /uv /uvx /usr/local/bin/

# Инструменты для работы с инфраструктурой и отладки.
RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
    jq \
    make \
    && rm -rf /var/lib/apt/lists/*

USER vscode
WORKDIR /workspaces/payment-service