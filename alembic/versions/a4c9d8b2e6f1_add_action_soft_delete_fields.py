"""add action soft delete fields

Revision ID: a4c9d8b2e6f1
Revises: 6e2bf71d383f
Create Date: 2026-05-20 00:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "a4c9d8b2e6f1"
down_revision: Union[str, Sequence[str], None] = "6e2bf71d383f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "action",
        sa.Column(
            "is_deleted",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
    )
    op.add_column("action", sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("action", sa.Column("deleted_by", sa.Text(), nullable=True))
    op.create_index("ix_action_is_deleted", "action", ["is_deleted"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_action_is_deleted", table_name="action")
    op.drop_column("action", "deleted_by")
    op.drop_column("action", "deleted_at")
    op.drop_column("action", "is_deleted")
