"""harden app user roles

Revision ID: e8b3f7a6d4c2
Revises: a4c9d8b2e6f1
Create Date: 2026-06-08 00:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "e8b3f7a6d4c2"
down_revision: Union[str, Sequence[str], None] = "a4c9d8b2e6f1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE app_user
        SET role = CASE
            WHEN lower(coalesce(role, '')) = 'manager' THEN 'manager'
            WHEN lower(coalesce(role, '')) IN ('admin', 'global', 'superadmin', 'super_admin') THEN 'admin'
            ELSE 'user'
        END
        """
    )
    op.alter_column(
        "app_user",
        "role",
        existing_type=sa.Text(),
        nullable=False,
        server_default=sa.text("'user'"),
    )
    op.create_check_constraint(
        "ck_app_user_role_supported",
        "app_user",
        "role IN ('user', 'manager', 'admin')",
    )


def downgrade() -> None:
    op.drop_constraint("ck_app_user_role_supported", "app_user", type_="check")
    op.alter_column(
        "app_user",
        "role",
        existing_type=sa.Text(),
        nullable=True,
        server_default=None,
    )
