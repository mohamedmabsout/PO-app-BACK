"""fix_changed_at_server_default

Revision ID: 1a2b3c4d5e6f
Revises: fb8336c7b8d0
Create Date: 2026-04-11 00:00:00.000000

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

revision: str = '1a2b3c4d5e6f'
down_revision: Union[str, Sequence[str], None] = 'fb8336c7b8d0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add server_default CURRENT_TIMESTAMP so MySQL fills it automatically
    op.alter_column(
        'merged_po_change_logs',
        'changed_at',
        existing_type=sa.DateTime(),
        server_default=sa.text("CURRENT_TIMESTAMP"),
        nullable=True,
    )


def downgrade() -> None:
    op.alter_column(
        'merged_po_change_logs',
        'changed_at',
        existing_type=sa.DateTime(),
        server_default=None,
        nullable=True,
    )
