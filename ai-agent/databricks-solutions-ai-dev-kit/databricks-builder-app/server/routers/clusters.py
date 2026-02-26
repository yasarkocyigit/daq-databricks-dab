"""Cluster management endpoints."""

import logging

from fastapi import APIRouter, Request
from databricks_tools_core.auth import set_databricks_auth, clear_databricks_auth

from ..services.clusters import list_clusters_async
from ..services.user import get_current_user, get_current_token, get_workspace_url

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get('/clusters')
async def get_clusters(request: Request):
  """Get available Databricks clusters.

  Returns clusters sorted by: running first, "shared" in name second, alphabetically.
  Results are cached for 5 minutes with background refresh.
  """
  # Validate user is authenticated and get Databricks auth
  await get_current_user(request)
  user_token = await get_current_token(request)
  workspace_url = get_workspace_url()

  # Set auth context for the request
  set_databricks_auth(workspace_url, user_token)

  try:
    # Get clusters (cached with async refresh)
    clusters = await list_clusters_async()
    return clusters
  finally:
    clear_databricks_auth()
