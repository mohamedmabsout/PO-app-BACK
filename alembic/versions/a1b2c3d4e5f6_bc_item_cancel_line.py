"""Add cancel fields to bc_items

Revision ID: a1b2c3d4e5f6
Revises: f9a99ba23fb1
Create Date: 2026-04-07 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, None] = '690549c8bd5b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('bc_items', sa.Column('cancel_reason', sa.Text(), nullable=True))
    op.add_column('bc_items', sa.Column('cancelled_at', sa.DateTime(), nullable=True))
    op.add_column('bc_items', sa.Column('cancelled_by_id', sa.Integer(), sa.ForeignKey('users.id'), nullable=True))

    # Add CANCELLED to the global_status enum
    op.execute(
        "ALTER TABLE bc_items MODIFY COLUMN global_status ENUM("
        "'PENDING','PENDING_PD_APPROVAL','OPEN','POSTPONED',"
        "'READY_FOR_ACT','ACCEPTED','PERMANENTLY_REJECTED','CANCELLED'"
        ") NOT NULL DEFAULT 'PENDING'"
    )


def downgrade() -> None:
    op.drop_column('bc_items', 'cancel_reason')
    op.drop_column('bc_items', 'cancelled_at')
    op.drop_column('bc_items', 'cancelled_by_id')

    # Remove CANCELLED from enum
    op.execute(
        "ALTER TABLE bc_items MODIFY COLUMN global_status ENUM("
        "'PENDING','PENDING_PD_APPROVAL','OPEN','POSTPONED',"
        "'READY_FOR_ACT','ACCEPTED','PERMANENTLY_REJECTED'"
        ") NOT NULL DEFAULT 'PENDING'"
    )
