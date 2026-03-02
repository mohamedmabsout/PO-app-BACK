"""Merge RAF and Contextual migrations

Revision ID: 8ebce2dd03b0
Revises: 6762c13149ab, 745d63dcc430
Create Date: 2026-03-02 10:55:40.566074

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8ebce2dd03b0'
down_revision: Union[str, Sequence[str], None] = ('6762c13149ab', '745d63dcc430')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
