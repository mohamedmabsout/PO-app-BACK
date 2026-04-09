"""Add bc_line_cancellation_log; drop dead cancel columns from bc_items

Revision ID: b2c3d4e5f6a7
Revises: a1b2c3d4e5f6
Create Date: 2026-04-09 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'b2c3d4e5f6a7'
down_revision: Union[str, None] = 'a1b2c3d4e5f6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Create the audit log table (IF NOT EXISTS — safe to re-run)
    op.execute("""
        CREATE TABLE IF NOT EXISTS bc_line_cancellation_log (
            id INTEGER NOT NULL AUTO_INCREMENT,
            bc_id INTEGER NOT NULL,
            bc_number VARCHAR(50) NOT NULL,
            merged_po_id INTEGER,
            po_reference VARCHAR(100),
            item_description TEXT,
            quantity_sbc FLOAT,
            line_amount_sbc FLOAT,
            cancel_reason TEXT NOT NULL,
            cancelled_at DATETIME DEFAULT (now()),
            cancelled_by_id INTEGER,
            PRIMARY KEY (id),
            FOREIGN KEY (bc_id) REFERENCES bon_de_commandes (id) ON DELETE CASCADE,
            FOREIGN KEY (merged_po_id) REFERENCES merged_pos (id) ON DELETE SET NULL,
            FOREIGN KEY (cancelled_by_id) REFERENCES users (id) ON DELETE SET NULL
        )
    """)

    # 2. Drop the soft-delete columns from bc_items
    # cancelled_by_id has an FK constraint — must drop it first
    op.execute("ALTER TABLE bc_items DROP FOREIGN KEY IF EXISTS bc_items_ibfk_4")
    op.execute("ALTER TABLE bc_items DROP COLUMN IF EXISTS cancel_reason")
    op.execute("ALTER TABLE bc_items DROP COLUMN IF EXISTS cancelled_at")
    op.execute("ALTER TABLE bc_items DROP COLUMN IF EXISTS cancelled_by_id")


def downgrade() -> None:
    # Restore soft-delete columns
    op.add_column('bc_items', sa.Column('cancel_reason', sa.Text(), nullable=True))
    op.add_column('bc_items', sa.Column('cancelled_at', sa.DateTime(), nullable=True))
    op.add_column('bc_items', sa.Column('cancelled_by_id', sa.Integer(), sa.ForeignKey('users.id'), nullable=True))

    op.drop_table('bc_line_cancellation_log')
