"""add_related_transaction_id_to_transactions

Revision ID: a1b2c3d4e5f7
Revises: e6f7a8b9c0d1
Create Date: 2026-04-20 00:00:00.000000

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

revision: str = 'a1b2c3d4e5f7'
down_revision: Union[str, None] = 'd5e6f7a8b9c0'
branch_labels: Union[str, None] = None
depends_on: Union[str, None] = None


def upgrade() -> None:
    op.add_column(
        'transactions',
        sa.Column('related_transaction_id', sa.Integer(), nullable=True)
    )
    op.create_foreign_key(
        'fk_transaction_related_tx',
        'transactions', 'transactions',
        ['related_transaction_id'], ['id'],
        ondelete='SET NULL'
    )


def downgrade() -> None:
    op.drop_constraint('fk_transaction_related_tx', 'transactions', type_='foreignkey')
    op.drop_column('transactions', 'related_transaction_id')
