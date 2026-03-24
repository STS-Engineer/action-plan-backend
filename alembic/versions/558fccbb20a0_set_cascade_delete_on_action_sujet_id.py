"""set cascade delete on action.sujet_id

Revision ID: 558fccbb20a0
Revises: 1d85d116dc65
Create Date: 2026-03-23 16:48:07.080185

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '558fccbb20a0'
down_revision: Union[str, Sequence[str], None] = '1d85d116dc65'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    op.drop_constraint("action_sujet_id_fkey", "action", type_="foreignkey")
    op.create_foreign_key(
        "action_sujet_id_fkey",
        "action",
        "sujet",
        ["sujet_id"],
        ["id"],
        ondelete="CASCADE",
    )


def downgrade() -> None:
    op.drop_constraint("action_sujet_id_fkey", "action", type_="foreignkey")
    op.create_foreign_key(
        "action_sujet_id_fkey",
        "action",
        "sujet",
        ["sujet_id"],
        ["id"],
    )