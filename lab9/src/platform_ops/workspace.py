import logging
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language

logger = logging.getLogger(__name__)

WORKSPACE_DIR_NAME = "lab9_platform_ops"


def upload_notebook(w: WorkspaceClient, local_path: Path) -> str:
    workspace_dir = f"/Users/{w.current_user.me().user_name}/{WORKSPACE_DIR_NAME}"
    workspace_path = f"{workspace_dir}/{local_path.stem}"
    w.workspace.mkdirs(workspace_dir)
    w.workspace.upload(
        workspace_path,
        local_path.read_bytes(),
        format=ImportFormat.SOURCE,
        language=Language.PYTHON,
        overwrite=True,
    )
    logger.info("Notebook uploaded to %s", workspace_path)
    return workspace_path
