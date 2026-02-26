"""Initial tables for projects, conversations, and messages.

Revision ID: 001_initial
Revises:
Create Date: 2025-01-06 00:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = '001_initial'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
  # Create projects table
  op.create_table(
    'projects',
    sa.Column('id', sa.String(50), primary_key=True),
    sa.Column('name', sa.String(255), nullable=False),
    sa.Column('user_email', sa.String(255), nullable=False, index=True),
    sa.Column(
      'created_at',
      sa.DateTime(timezone=True),
      server_default=sa.func.now(),
      nullable=False,
    ),
  )

  # Create composite index for user + created_at queries
  op.create_index(
    'ix_projects_user_created',
    'projects',
    ['user_email', 'created_at'],
  )

  # Create conversations table
  op.create_table(
    'conversations',
    sa.Column('id', sa.String(50), primary_key=True),
    sa.Column(
      'project_id',
      sa.String(50),
      sa.ForeignKey('projects.id', ondelete='CASCADE'),
      nullable=False,
      index=True,
    ),
    sa.Column('title', sa.String(255), server_default='New Conversation'),
    sa.Column(
      'created_at',
      sa.DateTime(timezone=True),
      server_default=sa.func.now(),
      nullable=False,
    ),
    sa.Column('session_id', sa.String(100), nullable=True),
  )

  # Create composite index for project + created_at queries
  op.create_index(
    'ix_conversations_project_created',
    'conversations',
    ['project_id', 'created_at'],
  )

  # Create messages table
  op.create_table(
    'messages',
    sa.Column('id', sa.String(50), primary_key=True),
    sa.Column(
      'conversation_id',
      sa.String(50),
      sa.ForeignKey('conversations.id', ondelete='CASCADE'),
      nullable=False,
      index=True,
    ),
    sa.Column('role', sa.String(20), nullable=False),
    sa.Column('content', sa.Text, nullable=False),
    sa.Column(
      'timestamp',
      sa.DateTime(timezone=True),
      server_default=sa.func.now(),
      nullable=False,
    ),
    sa.Column('is_error', sa.Boolean(), nullable=False, server_default='false'),
  )

  # Create composite index for conversation + timestamp queries
  op.create_index(
    'ix_messages_conversation_timestamp',
    'messages',
    ['conversation_id', 'timestamp'],
  )


def downgrade() -> None:
  op.drop_index('ix_messages_conversation_timestamp', table_name='messages')
  op.drop_table('messages')
  op.drop_index('ix_conversations_project_created', table_name='conversations')
  op.drop_table('conversations')
  op.drop_index('ix_projects_user_created', table_name='projects')
  op.drop_table('projects')
