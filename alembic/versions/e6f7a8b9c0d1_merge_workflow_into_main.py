"""merge_workflow_into_main

Merges the orphan workflow_matrix branch (5c198094f3b5) into the main
migration chain (d5e6f7a8b9c0) to restore a single head.

Revision ID: e6f7a8b9c0d1
Revises: 5c198094f3b5, d5e6f7a8b9c0
Create Date: 2026-04-15 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op

revision: str = 'e6f7a8b9c0d1'
down_revision: Union[str, Sequence[str], None] = ('5c198094f3b5', 'd5e6f7a8b9c0')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
