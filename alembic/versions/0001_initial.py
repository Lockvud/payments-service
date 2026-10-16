 

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001_initial"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("CREATE TYPE currency AS ENUM ('RUB', 'USD', 'EUR')")
    op.execute("CREATE TYPE payment_status AS ENUM ('pending', 'succeeded', 'failed')")
    op.execute("CREATE TYPE outbox_status AS ENUM ('pending', 'published')")

    op.create_table(
        "payments",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("amount", sa.Numeric(18, 2), nullable=False),
        sa.Column(
            "currency",
            postgresql.ENUM(name="currency", create_type=False),
            nullable=False,
        ),
        sa.Column(
            "description",
            sa.Text(),
            nullable=False,
            server_default=sa.text("''"),
        ),
        sa.Column(
            "metadata",
            postgresql.JSONB(),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "status",
            postgresql.ENUM(name="payment_status", create_type=False),
            nullable=False,
            server_default=sa.text("'pending'"),
        ),
        sa.Column("idempotency_key", sa.String(255), nullable=False),
        sa.Column("webhook_url", sa.String(2048), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("idempotency_key", name="uq_payments_idempotency_key"),
    )
    op.create_index("ix_payments_status_created_at", "payments", ["status", "created_at"])

    op.create_table(
        "outbox",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("aggregate_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_type", sa.String(128), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column(
            "status",
            postgresql.ENUM(name="outbox_status", create_type=False),
            nullable=False,
            server_default=sa.text("'pending'"),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_outbox_aggregate_id", "outbox", ["aggregate_id"])
    op.create_index("ix_outbox_status_created_at", "outbox", ["status", "created_at"])


def downgrade() -> None:
    op.drop_index("ix_outbox_status_created_at", table_name="outbox")
    op.drop_index("ix_outbox_aggregate_id", table_name="outbox")
    op.drop_table("outbox")

    op.drop_index("ix_payments_status_created_at", table_name="payments")
    op.drop_table("payments")

    op.execute("DROP TYPE outbox_status")
    op.execute("DROP TYPE payment_status")
    op.execute("DROP TYPE currency")