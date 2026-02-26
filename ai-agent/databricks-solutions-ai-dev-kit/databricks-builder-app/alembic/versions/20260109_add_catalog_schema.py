"""Add default_catalog and default_schema to conversations.

Revision ID: 20260109_catalog_schema
Revises: 20260107_cluster
Create Date: 2026-01-09
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = '20260109_catalog_schema'
down_revision: Union[str, None] = '20260107_cluster'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
  """Add default_catalog and default_schema columns to conversations table."""
  op.add_column('conversations', sa.Column('default_catalog', sa.String(255), nullable=True))
  op.add_column('conversations', sa.Column('default_schema', sa.String(255), nullable=True))


def downgrade() -> None:
  """Remove default_catalog and default_schema columns from conversations table."""
  op.drop_column('conversations', 'default_schema')
  op.drop_column('conversations', 'default_catalog')
