"""add sujet source application

Revision ID: f4b8a5c2d901
Revises: 9cc1f7f3d2a4
Create Date: 2026-07-13 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "f4b8a5c2d901"
down_revision: Union[str, Sequence[str], None] = "9cc1f7f3d2a4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("sujet", sa.Column("source_application", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("sujet", "source_application")
