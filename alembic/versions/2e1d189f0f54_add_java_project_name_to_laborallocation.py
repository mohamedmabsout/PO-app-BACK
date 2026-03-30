"""Add java_project_name to LaborAllocation

Revision ID: 2e1d189f0f54
Revises: 9a4c44653bbb
Create Date: 2026-03-27 16:48:50.471372

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision: str = '2e1d189f0f54'
down_revision: Union[str, Sequence[str], None] = '9a4c44653bbb'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # 1. Add the missing column to LaborAllocation
    op.add_column('pnl_labor_allocations', sa.Column('java_project_name', sa.String(length=255), nullable=True))
    
    # 2. Update the PnL structure to match your spreadsheet buckets
    op.add_column('project_pnl', sa.Column('working_trip_cost', sa.Float(), nullable=True))
    op.add_column('project_pnl', sa.Column('hosting_cost', sa.Float(), nullable=True))
    
    # Note: These drops are safe because project_pnl is a new table we just created
    op.drop_column('project_pnl', 'travel_cost')
    op.drop_column('project_pnl', 'other_service_cost')


def downgrade() -> None:
    """Downgrade schema."""
    op.add_column('project_pnl', sa.Column('other_service_cost', mysql.FLOAT(), nullable=True))
    op.add_column('project_pnl', sa.Column('travel_cost', mysql.FLOAT(), nullable=True))
    op.drop_column('project_pnl', 'hosting_cost')
    op.drop_column('project_pnl', 'working_trip_cost')
    op.drop_column('pnl_labor_allocations', 'java_project_name')