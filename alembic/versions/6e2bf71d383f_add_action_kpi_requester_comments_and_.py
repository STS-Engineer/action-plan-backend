"""add action kpi requester comments and attachments

Revision ID: 6e2bf71d383f
Revises: d7fb69b9ee63
Create Date: 2026-05-11 09:20:33.434812

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = '6e2bf71d383f'
down_revision: Union[str, Sequence[str], None] = 'd7fb69b9ee63'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('action', sa.Column('kpi', sa.Text(), nullable=True))
    op.add_column('action', sa.Column('demandeur', sa.Text(), nullable=True))
    op.add_column('action', sa.Column('email_demandeur', sa.Text(), nullable=True))

    op.create_table(
        'action_status_comment',
        sa.Column('id', sa.BigInteger(), primary_key=True),
        sa.Column('action_id', sa.BigInteger(), nullable=False),
        sa.Column('old_status', sa.Text(), nullable=True),
        sa.Column('new_status', sa.Text(), nullable=False),
        sa.Column('comment', sa.Text(), nullable=True),
        sa.Column('created_by', sa.Text(), nullable=True),
        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('now()'),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(['action_id'], ['action.id'], ondelete='CASCADE'),
    )

    op.create_table(
        'action_attachment',
        sa.Column('id', sa.BigInteger(), primary_key=True),
        sa.Column('action_id', sa.BigInteger(), nullable=False),
        sa.Column('file_name', sa.Text(), nullable=False),
        sa.Column('file_path', sa.Text(), nullable=False),
        sa.Column('uploaded_by', sa.Text(), nullable=True),
        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('now()'),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(['action_id'], ['action.id'], ondelete='CASCADE'),
    )


def downgrade() -> None:
    op.drop_table('action_attachment')
    op.drop_table('action_status_comment')

    op.drop_column('action', 'email_demandeur')
    op.drop_column('action', 'demandeur')
    op.drop_column('action', 'kpi')