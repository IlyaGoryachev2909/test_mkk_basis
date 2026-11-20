import asyncio
import logging
import os
from contextlib import asynccontextmanager, suppress

import httpx
from faststream import AckPolicy, FastStream
from faststream.rabbit import Channel, RabbitBroker, RabbitMessage

from app.db import engine
from app.messaging import EXCHANGE, NEW, RETRY, declare_topology, publish
from app.outbox import relay
from app.processing import process_payment
from app.schemas import PaymentEvent


logger = logging.getLogger(__name__)


async def main():
    broker = RabbitBroker(
        os.environ["AMQP_URL"], graceful_timeout=15,
        default_channel=Channel(prefetch_count=1, publisher_confirms=True, on_return_raises=True),
    )

    @asynccontextmanager
    async def lifespan():
        await broker.connect()
        await declare_topology(broker)
        dispatcher = asyncio.create_task(relay(broker))
        try:
            yield
        finally:
            dispatcher.cancel()
            with suppress(asyncio.CancelledError):
                await dispatcher
            await engine.dispose()

    async with httpx.AsyncClient(timeout=5, trust_env=False) as client:
        @broker.subscriber(
            NEW, EXCHANGE, ack_policy=AckPolicy.NACK_ON_ERROR,
            decoder=lambda message: message.body,
        )
        async def consume(message: RabbitMessage):
            attempt = message.headers.get("attempt", 1)
            if type(attempt) is not int or not 1 <= attempt <= len(RETRY) + 1:
                await message.reject()
                return
            try:
                event = PaymentEvent.model_validate_json(message.body)
                await process_payment(event.payment_id, attempt, client)
            except Exception as exc:
                logger.warning(
                    "Payment event %s failed on attempt %s (%s)",
                    message.message_id, attempt, type(exc).__name__,
                )
                if attempt > len(RETRY):
                    await message.reject()
                    return
                # A failed retry publication propagates. NACK_ON_ERROR requeues the original.
                await publish(broker, RETRY[attempt - 1], message.body, message.message_id, attempt + 1)

        await FastStream(broker, lifespan=lifespan).run()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
