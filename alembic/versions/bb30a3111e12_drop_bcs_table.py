"""drop bcs table

Revision ID: bb30a3111e12
Revises: e5534523fc8f
Create Date: 2026-01-06 10:32:42.196376

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'bb30a3111e12'
down_revision: Union[str, Sequence[str], None] = 'e5534523fc8f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
