"""restore recursive foreign keys

Revision ID: 1d85d116dc65
Revises: b7ee528149d0
Create Date: 2026-03-23 11:39:26.808192

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '1d85d116dc65'
down_revision: Union[str, Sequence[str], None] = 'b7ee528149d0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_foreign_key(
        "action_parent_action_id_fkey",
        "action",
        "action",
        ["parent_action_id"],
        ["id"],
        ondelete="CASCADE",
    )

    op.create_foreign_key(
        "fk_sujet_parent",
        "sujet",
        "sujet",
        ["parent_sujet_id"],
        ["id"],
        onupdate="CASCADE",
        ondelete="SET NULL",
    )


def downgrade() -> None:
    op.drop_constraint("fk_sujet_parent", "sujet", type_="foreignkey")
    op.drop_constraint("action_parent_action_id_fkey", "action", type_="foreignkey")
