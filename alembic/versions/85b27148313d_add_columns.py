"""Add columns

Revision ID: 85b27148313d
Revises: 66c1785dda6e
Create Date: 2026-01-31 02:20:59.041086

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision: str = '85b27148313d'
down_revision: Union[str, Sequence[str], None] = '66c1785dda6e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # 1. Drop the constraint using the REAL name from your error log
    # MySQL requires dropping the FK before dropping the column
    op.drop_constraint('expenses_ibfk_7', 'expenses', type_='foreignkey')

    # 2. Drop the column now that it is unlinked
    op.drop_column('expenses', 'act_id')

    # 3. Add the new batch-logic column to service_acceptances
    op.add_column('service_acceptances', sa.Column('expense_id', sa.Integer(), nullable=True))

    # 4. Create the new foreign key
    op.create_foreign_key(
        'fk_service_acceptances_expense', 
        'service_acceptances', 'expenses', 
        ['expense_id'], ['id']
    )


def downgrade() -> None:
    """Downgrade schema."""
    # 1. Remove the new link
    op.drop_constraint('fk_service_acceptances_expense', 'service_acceptances', type_='foreignkey')
    op.drop_column('service_acceptances', 'expense_id')

    # 2. Re-add the old column to expenses
    op.add_column('expenses', sa.Column('act_id', sa.Integer(), nullable=True))

    # 3. Re-create the old constraint (using the name the DB expects)
    op.create_foreign_key(
        'expenses_ibfk_7', 
        'expenses', 'service_acceptances', 
        ['act_id'], ['id']
    )