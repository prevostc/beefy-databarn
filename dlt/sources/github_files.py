from __future__ import annotations
import logging
from typing import Any, AsyncIterator, Dict
import dlt
from .resources.github_files.ui_repo import get_github_files_ui_repo_resources

logger = logging.getLogger(__name__)

@dlt.source(
    name="github_files", 
    max_table_nesting=0, 
    parallelized=True
)
async def github_files_source() -> Any:
    """Expose GitHub files resources for use by dlt pipelines."""

    ui_repo_resources = await get_github_files_ui_repo_resources()

    return ui_repo_resources
