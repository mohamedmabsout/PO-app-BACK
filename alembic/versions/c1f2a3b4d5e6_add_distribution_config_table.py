"""Add distribution config table

Revision ID: c1f2a3b4d5e6
Revises: 690549c8bd5b
Create Date: 2026-04-06 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = 'c1f2a3b4d5e6'
down_revision: Union[str, Sequence[str], None] = '690549c8bd5b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'pnl_distribution_config',
        sa.Column('id', sa.Integer(), nullable=False, autoincrement=True),
        sa.Column('period', sa.String(length=7), nullable=False),
        sa.Column('internal_project_id', sa.Integer(), nullable=False),
        sa.Column('percentage', sa.Float(), nullable=False),
        sa.Column('created_by_id', sa.Integer(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['internal_project_id'], ['internal_projects.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['created_by_id'], ['users.id'], ondelete='SET NULL'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('period', 'internal_project_id', name='uix_dist_period_project'),
    )
    op.create_index(op.f('ix_pnl_distribution_config_id'), 'pnl_distribution_config', ['id'], unique=False)
    op.create_index(op.f('ix_pnl_distribution_config_period'), 'pnl_distribution_config', ['period'], unique=False)


def downgrade() -> None:
    op.drop_index(op.f('ix_pnl_distribution_config_period'), table_name='pnl_distribution_config')
    op.drop_index(op.f('ix_pnl_distribution_config_id'), table_name='pnl_distribution_config')
    op.drop_table('pnl_distribution_config')
