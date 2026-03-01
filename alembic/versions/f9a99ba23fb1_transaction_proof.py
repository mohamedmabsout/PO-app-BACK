"""Transaction PROOF

Revision ID: f9a99ba23fb1
Revises: 0fd2cb4a2da1
Create Date: 2026-02-26 12:26:32.486614

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect # <--- Needed for checking existing columns

# revision identifiers, used by Alembic.
revision: str = 'f9a99ba23fb1'
down_revision: Union[str, Sequence[str], None] = '0fd2cb4a2da1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # 1. Get database connection and inspector
    bind = op.get_bind()
    inspector = inspect(bind)
    
    # 2. Get list of existing columns in 'transactions' table
    columns = [c['name'] for c in inspector.get_columns('transactions')]
    
    # 3. Only add 'proof_file' if it doesn't exist
    if 'proof_file' not in columns:
        # FIX: Added length 255 to String to satisfy MySQL requirements
        op.add_column('transactions', sa.Column('proof_file', sa.String(255), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    # Safety check for downgrade as well
    bind = op.get_bind()
    inspector = inspect(bind)
    columns = [c['name'] for c in inspector.get_columns('transactions')]
    
    if 'proof_file' in columns:
        op.drop_column('transactions', 'proof_file')