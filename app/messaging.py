from faststream.rabbit import QueueType, RabbitBroker, RabbitExchange, RabbitQueue
from pamqp.commands import Basic


EXCHANGE = RabbitExchange("payments", durable=True)
DEAD = RabbitQueue("payments.dlq", queue_type=QueueType.QUORUM)


def dead_letter_to(target: str) -> dict:
    return {
        "x-dead-letter-exchange": EXCHANGE.name,
        "x-dead-letter-routing-key": target,
        "x-dead-letter-strategy": "at-least-once",
        "x-overflow": "reject-publish",
    }


NEW = RabbitQueue("payments.new", queue_type=QueueType.QUORUM, arguments=dead_letter_to(DEAD.name))
RETRY = tuple(
    RabbitQueue(
        f"payments.retry.{seconds}s",
        queue_type=QueueType.QUORUM,
        arguments={**dead_letter_to(NEW.name), "x-message-ttl": seconds * 1000},
    )
    for seconds in (1, 2)
)


async def declare_topology(broker: RabbitBroker):
    exchange = await broker.declare_exchange(EXCHANGE)
    for definition in (DEAD, NEW, *RETRY):
        queue = await broker.declare_queue(definition)
        await queue.bind(exchange, routing_key=definition.name)


async def publish(broker: RabbitBroker, queue: RabbitQueue, body: bytes, message_id: str, attempt: int):
    confirmation = await broker.publish(
        body, exchange=EXCHANGE, routing_key=queue.name, persist=True, mandatory=True,
        content_type="application/json", message_id=message_id, headers={"attempt": attempt}, timeout=5,
    )
    if not isinstance(confirmation, Basic.Ack):
        raise RuntimeError("Publication was not confirmed by RabbitMQ")
