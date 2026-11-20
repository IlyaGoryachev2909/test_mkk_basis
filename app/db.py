import os
from datetime import datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import (
    CheckConstraint, DateTime, ForeignKey, Index, Numeric, SmallInteger, String, Text, func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Payment(Base):
    __tablename__ = "payments"
    __table_args__ = (
        CheckConstraint("amount > 0", name="ck_payments_amount"),
        CheckConstraint("currency IN ('RUB','USD','EUR')", name="ck_payments_currency"),
        CheckConstraint("status IN ('pending','succeeded','failed')", name="ck_payments_status"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    currency: Mapped[str] = mapped_column(String(3))
    description: Mapped[str] = mapped_column(Text)
    payload_metadata: Mapped[dict] = mapped_column("metadata", JSONB)
    status: Mapped[str] = mapped_column(String(9), server_default="pending")
    idempotency_key: Mapped[str] = mapped_column(String(255), unique=True)
    request_hash: Mapped[str] = mapped_column(String(64))
    webhook_url: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    webhook_delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    webhook_last_attempt: Mapped[int] = mapped_column(SmallInteger, server_default="0")


class Outbox(Base):
    __tablename__ = "outbox"

    # One payments.new event per payment. This is also the stable AMQP message_id.
    payment_id: Mapped[UUID] = mapped_column(ForeignKey("payments.id"), primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


Index("ix_outbox_unpublished", Outbox.created_at, postgresql_where=Outbox.published_at.is_(None))

engine = create_async_engine(os.environ["DATABASE_URL"], pool_pre_ping=True)
sessions = async_sessionmaker(engine, expire_on_commit=False)
