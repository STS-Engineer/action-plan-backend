"""add app user table

Revision ID: d7fb69b9ee63
Revises: d1c3330fdc45
Create Date: 2026-05-08 09:07:20.325753

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd7fb69b9ee63'
down_revision: Union[str, Sequence[str], None] = 'd1c3330fdc45'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""

    op.create_table(
        'app_user',

        sa.Column('id', sa.BigInteger(), nullable=False),

        sa.Column('email', sa.Text(), nullable=False),

        sa.Column('full_name', sa.Text(), nullable=True),

        sa.Column('hashed_password', sa.Text(), nullable=False),

        sa.Column('role', sa.Text(), nullable=True),

        sa.Column('is_active', sa.Boolean(), nullable=True),

        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('now()'),
            nullable=False
        ),

        sa.Column(
            'updated_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('now()'),
            nullable=False
        ),

        sa.PrimaryKeyConstraint('id')
    )

    op.create_index(
        op.f('ix_app_user_id'),
        'app_user',
        ['id'],
        unique=False
    )

    op.create_index(
        op.f('ix_app_user_email'),
        'app_user',
        ['email'],
        unique=True
    )


def downgrade() -> None:
    """Downgrade schema."""

    op.drop_index(
        op.f('ix_app_user_email'),
        table_name='app_user'
    )

    op.drop_index(
        op.f('ix_app_user_id'),
        table_name='app_user'
    )

    op.drop_table('app_user')