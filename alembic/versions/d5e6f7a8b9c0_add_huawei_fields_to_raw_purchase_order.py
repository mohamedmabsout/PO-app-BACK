"""add_huawei_fields_to_raw_purchase_order

Revision ID: d5e6f7a8b9c0
Revises: 1a2b3c4d5e6f
Create Date: 2026-04-15 00:00:00.000000

Adds Huawei eSupplier B2B fields to raw_purchase_orders.
These are nullable — existing Excel-imported rows will have NULL, no impact on existing logic.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd5e6f7a8b9c0'
down_revision: Union[str, None] = '1a2b3c4d5e6f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('raw_purchase_orders', sa.Column('line_location_id', sa.String(200), nullable=True))
    op.add_column('raw_purchase_orders', sa.Column('task_id', sa.String(200), nullable=True))
    op.add_column('raw_purchase_orders', sa.Column('instance_id', sa.Integer(), nullable=True))
    op.add_column('raw_purchase_orders', sa.Column('org_id', sa.String(100), nullable=True))
    op.add_column('raw_purchase_orders', sa.Column('po_line_id', sa.String(200), nullable=True))
    op.add_column('raw_purchase_orders', sa.Column('vendor_code', sa.String(100), nullable=True))
    op.add_column('raw_purchase_orders', sa.Column('pr_number', sa.String(200), nullable=True))
    op.add_column('raw_purchase_orders', sa.Column('unit_code', sa.String(50), nullable=True))
    op.add_column('raw_purchase_orders', sa.Column('shipment_status', sa.String(100), nullable=True))
    op.add_column('raw_purchase_orders', sa.Column('purchase_type', sa.String(100), nullable=True))
    op.add_column('raw_purchase_orders', sa.Column('huawei_source', sa.Boolean(), nullable=True, server_default=sa.false()))
    # Index for fast lookup by Huawei-native identifiers
    op.create_index('ix_raw_po_line_location_id', 'raw_purchase_orders', ['line_location_id'])
    op.create_index('ix_raw_po_task_id', 'raw_purchase_orders', ['task_id'])


def downgrade() -> None:
    op.drop_index('ix_raw_po_task_id', table_name='raw_purchase_orders')
    op.drop_index('ix_raw_po_line_location_id', table_name='raw_purchase_orders')
    op.drop_column('raw_purchase_orders', 'huawei_source')
    op.drop_column('raw_purchase_orders', 'purchase_type')
    op.drop_column('raw_purchase_orders', 'shipment_status')
    op.drop_column('raw_purchase_orders', 'unit_code')
    op.drop_column('raw_purchase_orders', 'pr_number')
    op.drop_column('raw_purchase_orders', 'vendor_code')
    op.drop_column('raw_purchase_orders', 'po_line_id')
    op.drop_column('raw_purchase_orders', 'org_id')
    op.drop_column('raw_purchase_orders', 'instance_id')
    op.drop_column('raw_purchase_orders', 'task_id')
    op.drop_column('raw_purchase_orders', 'line_location_id')
