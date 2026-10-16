import asyncio
import logging
import random
from decimal import Decimal

from payments.core.config import get_settings
from payments.models.payment import Currency

logger = logging.getLogger(__name__)


class PaymentGateway:
    """
    Эмуляция внешнего платёжного шлюза.

    charge() возвращает bool. True это операция принята, False это бизнес-отказ.
    Исключения означают инфраструктурную проблему и должны приводить
    к retry сообщения на уровне consumer.

    Эмулятор не идемпотентен на своей стороне. Если процесс упадёт между
    успешным charge() и commit() в БД, при повторной обработке charge()
    выполнится заново. В реальной интеграции для этого передаётся
    idempotency-key в PSP
    """

    def __init__(self) -> None:
        self._settings = get_settings()

    async def charge(self, amount: Decimal, currency: Currency) -> bool:
        s = self._settings
        await asyncio.sleep(random.uniform(s.gateway_min_latency, s.gateway_max_latency))
        return random.random() < s.gateway_success_rate
