"""Merge all heads into a single linear chain

Revision ID: c3d4e5f6a7b8
Revises: 2541e71105b9, b2c3d4e5f6a7
Create Date: 2026-04-09 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op

revision: str = 'c3d4e5f6a7b8'
down_revision: Union[str, Sequence[str], None] = ('2541e71105b9', 'b2c3d4e5f6a7')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # All schema changes are already present in the DB.
    # This migration only exists to join the two branches into one head.
    pass


def downgrade() -> None:
    pass
