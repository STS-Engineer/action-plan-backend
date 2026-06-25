"""create action escalation notification table

Revision ID: 9cc1f7f3d2a4
Revises: e8b3f7a6d4c2
Create Date: 2026-06-25 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "9cc1f7f3d2a4"
down_revision: Union[str, Sequence[str], None] = "e8b3f7a6d4c2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "action_escalation_notification",
        sa.Column("id", sa.BigInteger(), primary_key=True, nullable=False),
        sa.Column("action_id", sa.BigInteger(), nullable=False),
        sa.Column("recipient_email", sa.Text(), nullable=False),
        sa.Column("cc_emails", postgresql.JSONB(), nullable=True),
        sa.Column("escalation_level", sa.Integer(), nullable=False),
        sa.Column("hierarchy_source_used", sa.Text(), nullable=False),
        sa.Column("responsible_chain", postgresql.JSONB(), nullable=True),
        sa.Column("requester_chain", postgresql.JSONB(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False, server_default="pending"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("seen_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_summary_email_sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["action_id"], ["action.id"], ondelete="CASCADE"),
        sa.CheckConstraint(
            "status IN ('pending', 'seen', 'resolved', 'dismissed')",
            name="ck_action_escalation_notification_status",
        ),
    )
    op.create_index(
        "ix_action_escalation_notification_action_id",
        "action_escalation_notification",
        ["action_id"],
        unique=False,
    )
    op.create_index(
        "ix_action_escalation_notification_recipient_email",
        "action_escalation_notification",
        ["recipient_email"],
        unique=False,
    )
    op.create_index(
        "ix_action_escalation_notification_status",
        "action_escalation_notification",
        ["status"],
        unique=False,
    )
    op.create_index(
        "uq_action_escalation_pending_recipient",
        "action_escalation_notification",
        ["action_id", "recipient_email", "escalation_level"],
        unique=True,
        postgresql_where=sa.text("status = 'pending'"),
    )


def downgrade() -> None:
    op.drop_index("uq_action_escalation_pending_recipient", table_name="action_escalation_notification")
    op.drop_index("ix_action_escalation_notification_status", table_name="action_escalation_notification")
    op.drop_index("ix_action_escalation_notification_recipient_email", table_name="action_escalation_notification")
    op.drop_index("ix_action_escalation_notification_action_id", table_name="action_escalation_notification")
    op.drop_table("action_escalation_notification")
