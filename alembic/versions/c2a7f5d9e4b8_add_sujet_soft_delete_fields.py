"""add sujet soft delete fields

Revision ID: c2a7f5d9e4b8
Revises: f4b8a5c2d901
Create Date: 2026-07-14 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "c2a7f5d9e4b8"
down_revision: Union[str, None] = "f4b8a5c2d901"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "sujet",
        sa.Column(
            "is_deleted",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
    )
    op.add_column("sujet", sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("sujet", sa.Column("deleted_by", sa.Text(), nullable=True))
    op.create_index("ix_sujet_is_deleted", "sujet", ["is_deleted"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_sujet_is_deleted", table_name="sujet")
    op.drop_column("sujet", "deleted_by")
    op.drop_column("sujet", "deleted_at")
    op.drop_column("sujet", "is_deleted")
