"""add primary key to bcs

Revision ID: e5534523fc8f
Revises: ce6263ad171c
Create Date: 2026-01-06 10:08:51.475911

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e5534523fc8f'
down_revision: Union[str, Sequence[str], None] = 'ce6263ad171c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
