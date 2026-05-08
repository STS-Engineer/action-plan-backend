"""create action event log table

Revision ID: d1c3330fdc45
Revises: 21f5e51107c3
Create Date: 2026-05-06 08:55:32.006190

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd1c3330fdc45'
down_revision: Union[str, Sequence[str], None] = '21f5e51107c3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "action_event_log",
        sa.Column("id", sa.BigInteger(), primary_key=True, nullable=False),
        sa.Column("action_id", sa.BigInteger(), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("old_value", sa.Text(), nullable=True),
        sa.Column("new_value", sa.Text(), nullable=True),
        sa.Column("details", sa.Text(), nullable=True),
        sa.Column("created_by", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["action_id"], ["action.id"], ondelete="CASCADE"),
    )


def downgrade() -> None:
    op.drop_table("action_event_log")