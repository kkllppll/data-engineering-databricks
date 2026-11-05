import logging
import time
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError
from databricks.sdk.service.jobs import Run, RunLifeCycleState, RunResultState, RunTask

logger = logging.getLogger(__name__)

TERMINAL_STATES = {
    RunLifeCycleState.TERMINATED,
    RunLifeCycleState.SKIPPED,
    RunLifeCycleState.INTERNAL_ERROR,
}
POLL_INTERVAL_SECONDS = 15


@dataclass(frozen=True)
class RunResult:
    run_id: int
    result_state: str
    url: str
    failures: list[str]

    @property
    def succeeded(self) -> bool:
        return self.result_state == RunResultState.SUCCESS.value


def _state_name(task_or_run: Run | RunTask) -> str:
    # result_state exists only after the run ends; until then show where it is
    state = task_or_run.state
    return (state.result_state or state.life_cycle_state).value


def _describe(run: Run) -> str:
    tasks = ", ".join(f"{t.task_key}={_state_name(t)}" for t in run.tasks or [])
    return f"{_state_name(run)} [{tasks}]" if tasks else _state_name(run)


def _collect_failures(w: WorkspaceClient, run: Run) -> list[str]:
    failures = []
    for task in run.tasks or []:
        if task.state.result_state == RunResultState.SUCCESS:
            continue
        message = task.state.state_message or ""
        try:
            message = w.jobs.get_run_output(task.run_id).error or message
        except DatabricksError:
            logger.warning("Could not read the output of task %s", task.task_key)
        failures.append(f"{task.task_key}: {message or 'no details'}")
    return failures


def wait_for_run(w: WorkspaceClient, run_id: int, timeout_seconds: int) -> RunResult:
    deadline = time.monotonic() + timeout_seconds
    last_description = None
    while True:
        run = w.jobs.get_run(run_id)
        description = _describe(run)
        if description != last_description:
            logger.info("Run %s: %s", run_id, description)
            last_description = description

        if run.state.life_cycle_state in TERMINAL_STATES:
            result = RunResult(
                run_id=run_id,
                result_state=_state_name(run),
                url=run.run_page_url,
                failures=(
                    []
                    if run.state.result_state == RunResultState.SUCCESS
                    else _collect_failures(w, run)
                ),
            )
            log_result(result)
            return result

        if time.monotonic() > deadline:
            raise TimeoutError(
                f"Run {run_id} did not finish in {timeout_seconds} s: {run.run_page_url}"
            )
        time.sleep(POLL_INTERVAL_SECONDS)


def log_result(result: RunResult) -> None:
    if result.succeeded:
        logger.info("Run %s succeeded: %s", result.run_id, result.url)
        return
    logger.error(
        "Run %s finished with %s: %s", result.run_id, result.result_state, result.url
    )
    for failure in result.failures:
        logger.error("  %s", failure)
