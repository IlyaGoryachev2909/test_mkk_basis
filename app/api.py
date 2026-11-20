import hashlib
import json
import os
import secrets
from contextlib import asynccontextmanager
from typing import Annotated
from uuid import UUID, uuid4

from fastapi import Depends, FastAPI, Header, HTTPException, Security
from fastapi.security import APIKeyHeader
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from app.db import Outbox, Payment, engine, sessions
from app.schemas import PaymentAccepted, PaymentCreate, PaymentDetail


api_key = os.environ["API_KEY"].encode()
if not api_key:
    raise ValueError("API_KEY must be nonempty")


def authenticate(key: Annotated[str | None, Security(APIKeyHeader(name="X-API-Key", auto_error=False))]):
    if not key or not secrets.compare_digest(key.encode(), api_key):
        raise HTTPException(401, "Invalid API key")


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await engine.dispose()


app = FastAPI(
    title="Payment processing", dependencies=[Depends(authenticate)], lifespan=lifespan,
    openapi_url=None, docs_url=None, redoc_url=None,
)


@app.post("/api/v1/payments", response_model=PaymentAccepted, status_code=202)
async def create_payment(
    body: PaymentCreate,
    idempotency_key: Annotated[str, Header(min_length=1, max_length=255, pattern=r"\S")],
):
    payload = body.model_dump(mode="json")
    payload["amount"] = format(body.amount, ".2f")
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    request_hash = hashlib.sha256(canonical.encode()).hexdigest()
    create = (
        insert(Payment)
        .values(
            id=uuid4(), amount=body.amount, currency=body.currency, description=body.description,
            payload_metadata=body.metadata, webhook_url=str(body.webhook_url),
            idempotency_key=idempotency_key, request_hash=request_hash,
        )
        .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
        .returning(Payment)
    )
    async with sessions.begin() as session:
        payment = (await session.scalars(create)).one_or_none()
        if payment is None:
            payment = (await session.scalars(
                select(Payment).where(Payment.idempotency_key == idempotency_key)
            )).one()
            if payment.request_hash != request_hash:
                raise HTTPException(409, "Idempotency-Key already used for another request")
        else:
            session.add(Outbox(payment_id=payment.id))
    return payment


@app.get("/api/v1/payments/{payment_id}", response_model=PaymentDetail)
async def get_payment(payment_id: UUID):
    async with sessions() as session:
        payment = await session.get(Payment, payment_id)
        if payment is None:
            raise HTTPException(404, "Payment not found")
        return payment
