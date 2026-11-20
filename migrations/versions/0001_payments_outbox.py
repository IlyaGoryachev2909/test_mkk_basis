"""Create payments and transactional outbox."""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "payments",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("amount", sa.Numeric(18, 2), nullable=False),
        sa.Column("currency", sa.String(3), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("metadata", JSONB(), nullable=False),
        sa.Column("status", sa.String(9), server_default="pending", nullable=False),
        sa.Column("idempotency_key", sa.String(255), unique=True, nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("webhook_url", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True)),
        sa.Column("webhook_delivered_at", sa.DateTime(timezone=True)),
        sa.Column("webhook_last_attempt", sa.SmallInteger(), server_default="0", nullable=False),
        sa.CheckConstraint("amount > 0", name="ck_payments_amount"),
        sa.CheckConstraint("currency IN ('RUB','USD','EUR')", name="ck_payments_currency"),
        sa.CheckConstraint("status IN ('pending','succeeded','failed')", name="ck_payments_status"),
    )
    op.create_table(
        "outbox",
        sa.Column("payment_id", sa.Uuid(), sa.ForeignKey("payments.id"), primary_key=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True)),
    )
    op.create_index(
        "ix_outbox_unpublished", "outbox", ["created_at"], postgresql_where=sa.text("published_at IS NULL")
    )


def downgrade():
    op.drop_table("outbox")
    op.drop_table("payments")
