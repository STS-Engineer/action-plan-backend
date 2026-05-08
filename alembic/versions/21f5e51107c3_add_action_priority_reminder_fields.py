"""add action priority reminder fields

Revision ID: 21f5e51107c3
Revises: 558fccbb20a0
Create Date: 2026-05-05 09:50:39.978873

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '21f5e51107c3'
down_revision: Union[str, Sequence[str], None] = '558fccbb20a0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("action", sa.Column("estimated_duration_days", sa.Integer(), nullable=True))
    op.add_column("action", sa.Column("importance", sa.Text(), nullable=True))
    op.add_column("action", sa.Column("urgency", sa.Text(), nullable=True))
    op.add_column("action", sa.Column("escalation_level", sa.Integer(), nullable=True))
    op.add_column("action", sa.Column("priority_index", sa.Integer(), nullable=True))
    op.add_column("action", sa.Column("last_reminder_sent_at", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_column("action", "last_reminder_sent_at")
    op.drop_column("action", "priority_index")
    op.drop_column("action", "escalation_level")
    op.drop_column("action", "urgency")
    op.drop_column("action", "importance")
    op.drop_column("action", "estimated_duration_days")