"""Add cluster_id to conversations.

Revision ID: 20260107_cluster
Revises:
Create Date: 2026-01-07
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = '20260107_cluster'
down_revision: Union[str, None] = '002_project_backup'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
  """Add cluster_id column to conversations table."""
  op.add_column('conversations', sa.Column('cluster_id', sa.String(100), nullable=True))


def downgrade() -> None:
  """Remove cluster_id column from conversations table."""
  op.drop_column('conversations', 'cluster_id')
