"""merge_category_rules_into_main

Merges the category_rules branch (d798573da943) with the main chain (e6f7a8b9c0d1)
to restore a single head before deployment.

Revision ID: f1a2b3c4d5e6
Revises: d798573da943, e6f7a8b9c0d1
Create Date: 2026-04-22

"""
from typing import Sequence, Union
from alembic import op

revision: str = 'f1a2b3c4d5e6'
down_revision: Union[str, Sequence[str], None] = ('d798573da943', 'e6f7a8b9c0d1')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
