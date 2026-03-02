"""add_project_stakeholders

Revision ID: 745d63dcc430
Revises: 0fd2cb4a2da1
Create Date: 2026-03-02 ...

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect # <--- Important Import

# revision identifiers, used by Alembic.
revision: str = '745d63dcc430'
down_revision: Union[str, Sequence[str], None] = '0fd2cb4a2da1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Inspect database to see if table exists
    bind = op.get_bind()
    inspector = inspect(bind)
    tables = inspector.get_table_names()

    # 2. Only create if it doesn't exist
    if 'project_stakeholders' not in tables:
        op.create_table('project_stakeholders',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('project_id', sa.Integer(), nullable=False),
            sa.Column('user_id', sa.Integer(), nullable=False),
            sa.Column('role', sa.String(50), nullable=False), # Using String for MySQL safety
            sa.Column('is_lead', sa.Boolean(), default=False),
            sa.ForeignKeyConstraint(['project_id'], ['internal_projects.id'], ),
            sa.ForeignKeyConstraint(['user_id'], ['users.id'], ),
            sa.PrimaryKeyConstraint('id')
        )


def downgrade() -> None:
    # Safety check for downgrade too
    bind = op.get_bind()
    inspector = inspect(bind)
    tables = inspector.get_table_names()
    
    if 'project_stakeholders' in tables:
        op.drop_table('project_stakeholders')