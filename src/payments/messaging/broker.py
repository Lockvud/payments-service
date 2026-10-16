from functools import lru_cache

from faststream.rabbit import RabbitBroker

from payments.core.config import get_settings


@lru_cache(maxsize=1)
def get_broker() -> RabbitBroker:
    """
    RabbitBroker для публикации и потребления.
    """
    return RabbitBroker(str(get_settings().rabbitmq_url))
