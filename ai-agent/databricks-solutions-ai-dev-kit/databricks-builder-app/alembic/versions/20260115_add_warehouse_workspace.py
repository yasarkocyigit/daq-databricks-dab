"""Add warehouse_id and workspace_folder to conversations.

Revision ID: 20260115_warehouse_workspace
Revises: 20260109_catalog_schema
Create Date: 2026-01-15
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = '20260115_warehouse_workspace'
down_revision: Union[str, None] = '20260109_catalog_schema'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
  """Add warehouse_id and workspace_folder columns to conversations table."""
  op.add_column('conversations', sa.Column('warehouse_id', sa.String(100), nullable=True))
  op.add_column('conversations', sa.Column('workspace_folder', sa.String(500), nullable=True))


def downgrade() -> None:
  """Remove warehouse_id and workspace_folder columns from conversations table."""
  op.drop_column('conversations', 'workspace_folder')
  op.drop_column('conversations', 'warehouse_id')
