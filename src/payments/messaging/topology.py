from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from payments.core.config import Settings, get_settings

PAYMENTS_EXCHANGE = "payments"
DLX_EXCHANGE = "payments.dlx"

PAYMENTS_QUEUE = "payments.q"
DLQ = "payments.dlq"

PAYMENT_ROUTING_KEY = "payment.process"
DLQ_ROUTING_KEY = "payment.dead"


def _retry_exchange_name(delay_ms: int) -> str:
    return f"payments.retry.{delay_ms}ms"


def _retry_queue_name(delay_ms: int) -> str:
    return f"payments.retry.{delay_ms}ms.q"


def _retry_routing_key(delay_ms: int) -> str:
    return f"payment.retry.{delay_ms}ms"


def compute_retry_delays(
    max_attempts: int,
    base_ms: int,
    multiplier: float,
    cap_ms: int,
) -> list[int]:
    """
    Считает TTL для retry-стадий.

    Стадий ровно max_attempts - 1. Индекс в возвращаемом списке — это
    номер СЛЕДУЮЩЕЙ попытки минус 2:

      delay[0] — очередь, в которую уходит событие после провала attempt=1,
                 чтобы вернуться к попытке attempt=2;
      delay[1] — после провала attempt=2, вернуться к attempt=3;
      ...
      delay[N-2] — после провала attempt=N-1, вернуться к attempt=N.

    После провала attempt=N retry-стадии нет, событие идёт в DLQ.

    Формула: delay[i] = min(base_ms * multiplier**i, cap_ms).

    Пример base=5000, multiplier=2.0, cap=300000:
      max_attempts=2 -> [5000]
      max_attempts=3 -> [5000, 10000]
      max_attempts=4 -> [5000, 10000, 20000]
      max_attempts=5 -> [5000, 10000, 20000, 40000]
    """
    if max_attempts < 2:
        return []
    return [min(int(base_ms * (multiplier**i)), cap_ms) for i in range(max_attempts - 1)]


def _build_retry_queue(delay_ms: int) -> RabbitQueue:
    return RabbitQueue(
        _retry_queue_name(delay_ms),
        durable=True,
        routing_key=_retry_routing_key(delay_ms),
        arguments={
            "x-message-ttl": delay_ms,
            "x-dead-letter-exchange": PAYMENTS_EXCHANGE,
            "x-dead-letter-routing-key": PAYMENT_ROUTING_KEY,
        },
    )


def _build_retry_topology(
    *,
    max_attempts: int,
    base_ms: int,
    multiplier: float,
    cap_ms: int,
) -> tuple[
    list[int],
    list[RabbitExchange],
    list[RabbitQueue],
    dict[int, tuple[RabbitExchange, str]],
]:
    """
    Возвращает (delays, exchanges, queues, stages).

    stages — карта `next_attempt → (exchange, routing_key)`. Ключ — это
    номер СЛЕДУЮЩЕЙ попытки. Consumer вызывает
    RETRY_STAGES[event.attempt + 1]:

      event.attempt=1 (провал) -> RETRY_STAGES[2] -> delays[0]
      event.attempt=2 (провал) -> RETRY_STAGES[3] -> delays[1]
      ...
      event.attempt=N (провал) -> RETRY_STAGES[N+1] отсутствует -> DLQ

    Поэтому ключи — от 2 до max_attempts включительно.
    """
    delays = compute_retry_delays(max_attempts, base_ms, multiplier, cap_ms)
    exchanges = [RabbitExchange(_retry_exchange_name(d), type=ExchangeType.DIRECT, durable=True) for d in delays]
    queues = [_build_retry_queue(d) for d in delays]
    stages: dict[int, tuple[RabbitExchange, str]] = {
        next_attempt: (
            exchanges[next_attempt - 2],
            _retry_routing_key(delays[next_attempt - 2]),
        )
        for next_attempt in range(2, max_attempts + 1)
    }
    return delays, exchanges, queues, stages


# Глобальные объекты строятся при импорте модуля. Это осознанный компромисс:
# @broker.subscriber в consumer.py требует объекты RabbitQueue/RabbitExchange
# на этапе импорта, фабрика здесь не подойдёт.
#
# Побочный эффект: env-переменные должны быть выставлены ДО первого импорта
# topology. В тестах это обеспечивает pytest_configure в conftest.py, который
# вызывается до сбора тестовых модулей. В проде env выставляется docker-compose
# до старта процесса.
#
# Смена env после импорта не обновит топологию. Для тестов есть
# reset_topology_for_tests() — вызывать после get_settings.cache_clear().
_settings: Settings = get_settings()

payments_exchange = RabbitExchange(PAYMENTS_EXCHANGE, type=ExchangeType.DIRECT, durable=True)
dlx_exchange = RabbitExchange(DLX_EXCHANGE, type=ExchangeType.DIRECT, durable=True)

payments_queue = RabbitQueue(
    PAYMENTS_QUEUE,
    durable=True,
    routing_key=PAYMENT_ROUTING_KEY,
    arguments={
        # Safety net на случай, если consumer не сможет ни обработать
        # сообщение, ни переопубликовать его копию (например, публикация
        # не подтвердилась — брокер недоступен). FastStream применит
        # REJECT_ON_ERROR к reject(requeue=False), RabbitMQ перенаправит
        # сообщение в DLX к payments.dlq.
        #
        # Это НЕ абсолютная гарантия доставки: при одновременном отказе
        # payments.q и payments.dlx сообщение может потеряться. Для
        # тестового задания такой уровень защиты избыточен; в проде
        # рекомендуется мониторинг DLQ и алертинг.
        "x-dead-letter-exchange": DLX_EXCHANGE,
        "x-dead-letter-routing-key": DLQ_ROUTING_KEY,
    },
)

dlq = RabbitQueue(DLQ, durable=True, routing_key=DLQ_ROUTING_KEY)


def _apply_settings(settings: Settings) -> None:
    """Перестраивает retry-топологию из настроек. Вызывается при импорте и в тестах."""
    global _RETRY_DELAYS_MS, retry_exchanges, retry_queues, RETRY_STAGES
    _RETRY_DELAYS_MS, retry_exchanges, retry_queues, RETRY_STAGES = _build_retry_topology(
        max_attempts=settings.process_max_attempts,
        base_ms=settings.retry_base_delay_ms,
        multiplier=settings.retry_multiplier,
        cap_ms=settings.retry_max_delay_ms,
    )


_apply_settings(_settings)


async def declare_topology(broker: RabbitBroker) -> None:
    """
    Идемпотентное объявление exchanges, queues и bindings.

    Retry-очереди и exchange'и генерируются из process_max_attempts и
    параметров экспоненциальной задержки. Увеличение PROCESS_MAX_ATTEMPTS
    автоматически добавляет нужное количество стадий — правки в коде не
    требуются.
    """
    await broker.declare_exchange(payments_exchange)
    await broker.declare_exchange(dlx_exchange)
    for exchange in retry_exchanges:
        await broker.declare_exchange(exchange)

    robust_payments_queue = await broker.declare_queue(payments_queue)
    await robust_payments_queue.bind(payments_exchange.name, routing_key=PAYMENT_ROUTING_KEY)

    robust_dlq = await broker.declare_queue(dlq)
    await robust_dlq.bind(dlx_exchange.name, routing_key=DLQ_ROUTING_KEY)

    for delay, exchange, queue in zip(_RETRY_DELAYS_MS, retry_exchanges, retry_queues, strict=True):
        robust_retry_queue = await broker.declare_queue(queue)
        await robust_retry_queue.bind(exchange.name, routing_key=_retry_routing_key(delay))


def assert_topology_consistent(process_max_attempts: int) -> None:
    """
    Проверяет, что RETRY_STAGES согласован с process_max_attempts.

    При динамической генерации топологии это тождество выполняется всегда,
    но проверка остаётся как защита от рассинхронизации в тестах, где
    env выставляется не в том порядке.
    """
    expected_attempts = set(range(2, process_max_attempts + 1))
    actual_attempts = set(RETRY_STAGES.keys())
    if expected_attempts != actual_attempts:
        raise RuntimeError(
            f"topology mismatch: process_max_attempts={process_max_attempts}, "
            f"RETRY_STAGES keys={sorted(actual_attempts)}, "
            f"expected={sorted(expected_attempts)}."
        )


def reset_topology_for_tests() -> None:
    """
    Пересоздаёт глобальные объекты топологии после сброса get_settings.

    Вызывать в conftest.py после get_settings.cache_clear(), до импорта
    consumer.py. Иначе декоратор @broker.subscriber зафиксирует объекты
    со старыми TTL.
    """
    global _settings
    _settings = get_settings()
    _apply_settings(_settings)


# Явные аннотации, mypy должен знать типы глобалов, которые присваиваются
# в _apply_settings через global.
_RETRY_DELAYS_MS: list[int]
retry_exchanges: list[RabbitExchange]
retry_queues: list[RabbitQueue]
RETRY_STAGES: dict[int, tuple[RabbitExchange, str]]
