import ipaddress
import logging
import socket
from functools import lru_cache
from typing import Any
from urllib.parse import urlparse

import httpx

from payments.core.config import get_settings

logger = logging.getLogger(__name__)


class WebhookDeliveryError(RuntimeError):
    """Webhook не доставлен. Обрабатывается retry-логикой консьюмера."""


class UnsafeWebhookUrlError(WebhookDeliveryError):
    """URL ведёт на приватный/служебный адрес. Не ретраится."""


@lru_cache(maxsize=1)
def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=get_settings().webhook_timeout,
        follow_redirects=False,
    )


def _is_private_host(host: str) -> bool:
    """
    Проверяет, резолвится ли хост в приватный/loopback/link-local адрес.

    Это неполная защита от SSRF. DNS rebinding (первый резолв,
    публичный IP, второй, приватный) здесь не закрывается. Полноценное
    решение, закрепить IP при резолве и использовать его в запросе
    """
    try:
        infos = socket.getaddrinfo(host, None, proto=socket.IPPROTO_TCP)
    except socket.gaierror as exc:
        raise WebhookDeliveryError(f"DNS resolution failed for {host}: {exc}") from exc

    for info in infos:
        ip_str = info[4][0]
        ip = ipaddress.ip_address(ip_str)
        if (
            ip.is_private
            or ip.is_loopback
            or ip.is_link_local
            or ip.is_reserved
            or ip.is_multicast
            or ip.is_unspecified
        ):
            return True
    return False


class WebhookNotifier:
    """
    Доставка webhook-уведомлений.

    Одна HTTP-попытка на вызов. Retry-политика, на уровне консьюмера
    (через retry-очереди RabbitMQ с TTL), чтобы переживать рестарт процесса
    и не растягивать транзакцию БД на время backoff.
    """

    def __init__(self, http: httpx.AsyncClient | None = None) -> None:
        self._http = http or _client()
        self._settings = get_settings()

    async def deliver(self, url: str, payload: dict[str, Any]) -> None:
        if not self._settings.webhook_allow_private_hosts:
            host = urlparse(url).hostname
            if host is None:
                raise UnsafeWebhookUrlError(f"cannot parse host from {url}")
            if _is_private_host(host):
                raise UnsafeWebhookUrlError(f"webhook URL resolves to a private address: {url}")

        try:
            response = await self._http.post(url, json=payload)
            response.raise_for_status()
        except httpx.HTTPError as exc:
            logger.warning("webhook %s failed: %s", url, exc)
            raise WebhookDeliveryError(f"webhook to {url} failed: {exc}") from exc
