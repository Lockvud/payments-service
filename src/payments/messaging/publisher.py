from faststream.rabbit import RabbitBroker, RabbitExchange

from payments.schemas.events import PaymentEvent


async def publish_event(
    broker: RabbitBroker,
    event: PaymentEvent,
    exchange: RabbitExchange,
    routing_key: str,
) -> None:
    """
    Единая точка публикации событий.

    Гарантии:
    - persist=True, сообщение durable, переживёт рестарт RabbitMQ.
    - publisher_confirms=True на брокере, после возврата
      из этой функции RabbitMQ подтвердил приём сообщения exchange.
      Только после этого безопасно ack исходное: копия уже сохранена.

    Что эти гарантии не покрывают:
    - Дедупликацию. message_id/correlation_id, метаданные для трейсинга;
      RabbitMQ не отбрасывает дубликаты по message_id. Если Outbox Publisher
      упадёт между publish и UPDATE outbox.status='published', следующий
      запуск опубликует событие повторно, и consumer получит дубликат.
      Идемпотентность обеспечивает обработчик: SELECT FOR UPDATE + проверка
      статуса в PaymentService.process, плюс UNIQUE(idempotency_key).
    - Маршрутизацию. Confirm подтверждает приём от exchange, а не доставку
      в очередь. Unroutable сообщения (exchange без binding на routing_key)
      отбрасываются молча. У нас bindings создаются в declare_topology()
      при старте процесса; расхождение топологии восстанавливается перезапуском.
    """
    await broker.publish(
        event,
        exchange=exchange,
        routing_key=routing_key,
        persist=True,
        message_id=str(event.payment_id),
        correlation_id=str(event.payment_id),
    )
