import logging

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import NotebookTask, SubmitTask

logger = logging.getLogger(__name__)

RUN_TIMEOUT_SECONDS = 1800


def submit_notebook(
    w: WorkspaceClient,
    cluster_id: str,
    notebook_path: str,
    parameters: dict[str, str],
    run_name: str,
) -> int:
    """Run a notebook once on an existing cluster without creating a job object"""
    run = w.jobs.submit(
        run_name=run_name,
        timeout_seconds=RUN_TIMEOUT_SECONDS,
        tasks=[
            SubmitTask(
                task_key="notebook",
                existing_cluster_id=cluster_id,
                notebook_task=NotebookTask(
                    notebook_path=notebook_path,
                    base_parameters=parameters,
                ),
            )
        ],
    )
    logger.info("Notebook %s submitted, run id=%s", notebook_path, run.run_id)
    return run.run_id


def find_job_id(w: WorkspaceClient, job_name: str) -> int:
    matches = [job.job_id for job in w.jobs.list(name=job_name)]
    if len(matches) != 1:
        raise LookupError(f"Expected one job named '{job_name}', found {len(matches)}")
    return matches[0]


def run_job(w: WorkspaceClient, job_id: int) -> int:
    run = w.jobs.run_now(job_id=job_id)
    logger.info("Job %s started, run id=%s", job_id, run.run_id)
    return run.run_id
