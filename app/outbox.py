import asyncio
import logging
from datetime import UTC, datetime

from faststream.rabbit import RabbitBroker
from sqlalchemy import select

from app.db import Outbox, sessions
from app.messaging import NEW, publish
from app.schemas import PaymentEvent


logger = logging.getLogger(__name__)


async def publish_next(broker: RabbitBroker) -> bool:
    async with sessions.begin() as session:
        event = await session.scalar(
            select(Outbox)
            .where(Outbox.published_at.is_(None))
            .order_by(Outbox.created_at, Outbox.payment_id)
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        if event is None:
            return False
        body = PaymentEvent(payment_id=event.payment_id).model_dump_json().encode()
        await publish(broker, NEW, body, str(event.payment_id), attempt=1)
        event.published_at = datetime.now(UTC)
    return True


async def relay(broker: RabbitBroker):
    while True:
        try:
            if not await publish_next(broker):
                await asyncio.sleep(0.5)
        except Exception:
            logger.exception("Outbox publication failed, retrying")
            await asyncio.sleep(2)
