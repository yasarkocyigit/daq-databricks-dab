"""Add project_backup table for storing zipped project files.

Revision ID: 002_project_backup
Revises: 001_initial
Create Date: 2025-01-06 12:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = '002_project_backup'
down_revision: Union[str, None] = '001_initial'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
  # Create project_backup table
  op.create_table(
    'project_backup',
    sa.Column(
      'project_id',
      sa.String(50),
      sa.ForeignKey('projects.id', ondelete='CASCADE'),
      primary_key=True,
    ),
    sa.Column('backup_data', sa.LargeBinary, nullable=False),
    sa.Column(
      'updated_at',
      sa.DateTime(timezone=True),
      server_default=sa.func.now(),
      nullable=False,
    ),
  )


def downgrade() -> None:
  op.drop_table('project_backup')
