"""add_category_rules_table

Revision ID: d798573da943
Revises: a1b2c3d4e5f7
Create Date: 2026-04-21

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'd798573da943'
down_revision = 'a1b2c3d4e5f7'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table('category_rules',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('item_description', sa.String(length=500), nullable=False),
    sa.Column('category', sa.String(length=100), nullable=False),
    sa.Column('created_at', sa.DateTime(), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('item_description')
    )
    op.create_index(op.f('ix_category_rules_id'), 'category_rules', ['id'], unique=False)
    op.create_index(op.f('ix_category_rules_item_description'), 'category_rules', ['item_description'], unique=True)
    op.create_index(op.f('ix_category_rules_created_at'), 'category_rules', ['created_at'], unique=False)


def downgrade() -> None:
    op.drop_index(op.f('ix_category_rules_created_at'), table_name='category_rules')
    op.drop_index(op.f('ix_category_rules_item_description'), table_name='category_rules')
    op.drop_index(op.f('ix_category_rules_id'), table_name='category_rules')
    op.drop_table('category_rules')
