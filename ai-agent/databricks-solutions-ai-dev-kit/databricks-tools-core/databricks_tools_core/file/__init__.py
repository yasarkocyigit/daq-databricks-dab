"""
File - Workspace File Operations

Functions for uploading files and folders to Databricks Workspace.

Note: For Unity Catalog Volume file operations, use the unity_catalog module.
"""

from .workspace import (
    UploadResult,
    FolderUploadResult,
    upload_folder,
    upload_file,
)

__all__ = [
    # Workspace file operations
    "UploadResult",
    "FolderUploadResult",
    "upload_folder",
    "upload_file",
]
