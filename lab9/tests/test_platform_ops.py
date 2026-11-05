from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from databricks.sdk.service.jobs import (
    Run,
    RunLifeCycleState,
    RunResultState,
    RunState,
    RunTask,
)

from platform_ops import cli, runs
from platform_ops.clusters import temporary_cluster
from platform_ops.config import ClusterConfig
from platform_ops.jobs import find_job_id
from platform_ops.runs import RunResult, wait_for_run

SUCCESS = RunResult(
    run_id=1, result_state="SUCCESS", url="https://host/run/1", failures=[]
)
FAILED = RunResult(
    run_id=2, result_state="FAILED", url="https://host/run/2", failures=["load: boom"]
)


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(runs.time, "sleep", lambda _: None)


@pytest.fixture
def w():
    return MagicMock()


def make_run(life_cycle_state, result_state=None, tasks=None) -> Run:
    """Build a Run as the Jobs API returns it; tasks map task_key to its result state."""
    return Run(
        run_id=1,
        run_page_url="https://host/run/1",
        state=RunState(life_cycle_state=life_cycle_state, result_state=result_state),
        tasks=[
            RunTask(
                task_key=key,
                run_id=10,
                state=RunState(
                    life_cycle_state=life_cycle_state, result_state=task_result
                ),
            )
            for key, task_result in (tasks or {}).items()
        ],
    )


def test_wait_for_run_polls_until_success(w):
    w.jobs.get_run.side_effect = [
        make_run(RunLifeCycleState.PENDING),
        make_run(RunLifeCycleState.TERMINATED, RunResultState.SUCCESS),
    ]

    assert wait_for_run(w, 1, timeout_seconds=60).succeeded


def test_terminated_is_not_success_and_failed_task_is_reported(w):
    w.jobs.get_run.return_value = make_run(
        RunLifeCycleState.TERMINATED,
        RunResultState.FAILED,
        tasks={"load": RunResultState.SUCCESS, "data_quality": RunResultState.FAILED},
    )
    w.jobs.get_run_output.return_value.error = "3 rows failed"

    result = wait_for_run(w, 1, timeout_seconds=60)

    assert not result.succeeded
    assert result.failures == ["data_quality: 3 rows failed"]


def test_wait_for_run_times_out(w):
    w.jobs.get_run.return_value = make_run(RunLifeCycleState.RUNNING)

    with pytest.raises(TimeoutError):
        wait_for_run(w, 1, timeout_seconds=-1)


@pytest.mark.parametrize("job_ids", [[], [1, 2]], ids=["no job", "two jobs"])
def test_find_job_id_refuses_to_guess(w, job_ids):
    w.jobs.list.return_value = [SimpleNamespace(job_id=job_id) for job_id in job_ids]

    with pytest.raises(LookupError):
        find_job_id(w, "lab8_daily")


def test_cluster_is_deleted_when_the_work_inside_fails(w):
    w.cluster_policies.list.return_value = [
        SimpleNamespace(policy_id="p-1", name="Personal Compute")
    ]
    w.clusters.create.return_value.cluster_id = "c-1"
    config = ClusterConfig("Personal Compute", "Standard_D4ds_v5", "lab9-smoke", 20)

    with pytest.raises(RuntimeError):
        with temporary_cluster(w, config):
            raise RuntimeError("notebook failed")

    w.clusters.permanent_delete.assert_called_once_with("c-1")


def test_e2e_skips_the_smoke_check_when_the_job_fails(monkeypatch, w):
    monkeypatch.setattr(cli, "WorkspaceClient", lambda profile: w)
    monkeypatch.setattr(cli, "run_daily_job", lambda w, config: FAILED)
    smoke_check = MagicMock(return_value=SUCCESS)
    monkeypatch.setattr(cli, "run_smoke_check", smoke_check)

    assert cli.main(["e2e"]) == cli.EXIT_FAILED
    smoke_check.assert_not_called()
