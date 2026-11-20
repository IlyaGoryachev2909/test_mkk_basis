import asyncio
import random
from datetime import UTC, datetime
from uuid import UUID

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import Payment, sessions


async def lock_payment(session: AsyncSession, payment_id: UUID) -> Payment:
    return (await session.scalars(
        select(Payment).where(Payment.id == payment_id).with_for_update()
    )).one()


async def simulate_gateway() -> str:
    await asyncio.sleep(random.uniform(2, 5))
    return "succeeded" if random.random() < 0.9 else "failed"


async def process_payment(payment_id: UUID, attempt: int, client: httpx.AsyncClient):
    async with sessions.begin() as session:
        payment = await lock_payment(session, payment_id)
        if payment.status == "pending":
            payment.status = await simulate_gateway()
            payment.processed_at = datetime.now(UTC)

    # Commit the gateway result before making any outbound HTTP request.
    failure = None
    async with sessions.begin() as session:
        payment = await lock_payment(session, payment_id)
        if payment.webhook_delivered_at or payment.webhook_last_attempt > attempt:
            return
        if payment.webhook_last_attempt == attempt:
            # A previous delivery committed failure but may have crashed before scheduling a retry.
            raise RuntimeError("Webhook previously failed on this attempt")

        payload = {
            "payment_id": str(payment.id), "status": payment.status,
            "amount": str(payment.amount), "currency": payment.currency,
            "processed_at": payment.processed_at.isoformat(),
        }
        try:
            # Bound the entire call, including a receiver that keeps a response stream open.
            async with asyncio.timeout(6):
                async with client.stream(
                    "POST", payment.webhook_url, json=payload,
                    headers={"Idempotency-Key": str(payment.id)},
                ) as response:
                    response.raise_for_status()
        except (httpx.HTTPError, TimeoutError) as exc:
            failure = exc
        else:
            payment.webhook_delivered_at = datetime.now(UTC)
        payment.webhook_last_attempt = attempt

    if failure is not None:
        raise failure
