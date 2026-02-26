"""Project management endpoints.

All endpoints are scoped to the current authenticated user.
"""

import logging

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from ..services.storage import ProjectStorage
from ..services.user import get_current_user

logger = logging.getLogger(__name__)
router = APIRouter()


class CreateProjectRequest(BaseModel):
  """Request to create a new project."""

  name: str


class UpdateProjectRequest(BaseModel):
  """Request to update a project."""

  name: str


@router.get('/projects')
async def get_all_projects(request: Request):
  """Get all projects for the current user sorted by created_at (newest first)."""
  user_email = await get_current_user(request)
  storage = ProjectStorage(user_email)

  logger.info(f'Fetching all projects for user: {user_email}')
  projects = await storage.get_all()
  logger.info(f'Retrieved {len(projects)} projects for user: {user_email}')

  return [project.to_dict() for project in projects]


@router.get('/projects/{project_id}')
async def get_project(request: Request, project_id: str):
  """Get a specific project by ID."""
  user_email = await get_current_user(request)
  storage = ProjectStorage(user_email)

  logger.info(f'Fetching project {project_id} for user: {user_email}')

  project = await storage.get(project_id)
  if not project:
    logger.warning(f'Project not found: {project_id} for user: {user_email}')
    raise HTTPException(status_code=404, detail=f'Project {project_id} not found')

  return project.to_dict()


@router.post('/projects')
async def create_project(request: Request, body: CreateProjectRequest):
  """Create a new project."""
  user_email = await get_current_user(request)
  storage = ProjectStorage(user_email)

  logger.info(f"Creating project '{body.name}' for user: {user_email}")

  project = await storage.create(name=body.name)
  logger.info(f'Created project {project.id} for user: {user_email}')

  return project.to_dict()


@router.patch('/projects/{project_id}')
async def update_project(request: Request, project_id: str, body: UpdateProjectRequest):
  """Update a project's name."""
  user_email = await get_current_user(request)
  storage = ProjectStorage(user_email)

  logger.info(f'Updating project {project_id} for user: {user_email}')

  success = await storage.update_name(project_id, body.name)
  if not success:
    logger.warning(f'Project not found for update: {project_id} for user: {user_email}')
    raise HTTPException(status_code=404, detail=f'Project {project_id} not found')

  logger.info(f'Updated project {project_id} for user: {user_email}')
  return {'success': True, 'project_id': project_id}


@router.delete('/projects/{project_id}')
async def delete_project(request: Request, project_id: str):
  """Delete a project and all its conversations."""
  user_email = await get_current_user(request)
  storage = ProjectStorage(user_email)

  logger.info(f'Deleting project {project_id} for user: {user_email}')

  success = await storage.delete(project_id)
  if not success:
    logger.warning(f'Project not found for deletion: {project_id} for user: {user_email}')
    raise HTTPException(status_code=404, detail=f'Project {project_id} not found')

  logger.info(f'Deleted project {project_id} for user: {user_email}')
  return {'success': True, 'deleted_project_id': project_id}
