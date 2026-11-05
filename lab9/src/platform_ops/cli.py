import argparse
import logging
import sys
from dataclasses import asdict
from pathlib import Path

from databricks.sdk import WorkspaceClient

from platform_ops.clusters import temporary_cluster
from platform_ops.config import TargetConfig, load_config
from platform_ops.jobs import RUN_TIMEOUT_SECONDS, find_job_id, run_job, submit_notebook
from platform_ops.runs import RunResult, wait_for_run
from platform_ops.workspace import upload_notebook

logger = logging.getLogger("platform_ops")

PROJECT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = PROJECT_DIR / "config.yml"
SMOKE_NOTEBOOK = PROJECT_DIR / "notebooks" / "smoke_check.py"
SMOKE_RUN_NAME = "lab9-smoke-check"

JOB_WAIT_TIMEOUT_SECONDS = 3600
# The local wait outlasts the run timeout, so Databricks stops a hanging notebook first
# and the real TIMEDOUT state is reported instead of a local timeout
SMOKE_WAIT_TIMEOUT_SECONDS = RUN_TIMEOUT_SECONDS + 300

EXIT_OK = 0
EXIT_FAILED = 1

Steps = list[tuple[str, RunResult]]


def run_daily_job(w: WorkspaceClient, config: TargetConfig) -> RunResult:
    job_id = find_job_id(w, config.job_name)
    return wait_for_run(w, run_job(w, job_id), JOB_WAIT_TIMEOUT_SECONDS)


def run_smoke_check(w: WorkspaceClient, config: TargetConfig) -> RunResult:
    notebook_path = upload_notebook(w, SMOKE_NOTEBOOK)
    with temporary_cluster(w, config.cluster) as cluster_id:
        run_id = submit_notebook(
            w, cluster_id, notebook_path, asdict(config.smoke_check), SMOKE_RUN_NAME
        )
        return wait_for_run(w, run_id, SMOKE_WAIT_TIMEOUT_SECONDS)


def cmd_e2e(
    w: WorkspaceClient, config: TargetConfig, args: argparse.Namespace
) -> Steps:
    steps = [("daily job", run_daily_job(w, config))]
    if not steps[0][1].succeeded:
        logger.error("Daily job failed, the smoke check is skipped")
        return steps
    steps.append(("smoke check", run_smoke_check(w, config)))
    return steps


def cmd_run_job(
    w: WorkspaceClient, config: TargetConfig, args: argparse.Namespace
) -> Steps:
    return [("daily job", run_daily_job(w, config))]


def cmd_smoke_check(
    w: WorkspaceClient, config: TargetConfig, args: argparse.Namespace
) -> Steps:
    return [("smoke check", run_smoke_check(w, config))]


def cmd_status(
    w: WorkspaceClient, config: TargetConfig, args: argparse.Namespace
) -> Steps:
    return [
        (f"run {args.run_id}", wait_for_run(w, args.run_id, JOB_WAIT_TIMEOUT_SECONDS))
    ]


def log_summary(steps: Steps) -> None:
    logger.info("Summary:")
    for name, result in steps:
        logger.info("  %-12s %-10s %s", name, result.result_state, result.url)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="platform-ops", description="Databricks platform automation (Lab 9)"
    )
    parser.add_argument(
        "--target", default="dev", help="config section to use (default: dev)"
    )
    parser.add_argument(
        "--profile",
        help="Databricks CLI profile; environment variables are used when omitted",
    )
    parser.add_argument(
        "--config", type=Path, default=DEFAULT_CONFIG, help="path to config.yml"
    )

    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser(
        "e2e", help="run the daily job, then the smoke check on a temporary cluster"
    ).set_defaults(handler=cmd_e2e)
    commands.add_parser(
        "run-job", help="run the daily job and wait for it"
    ).set_defaults(handler=cmd_run_job)
    commands.add_parser(
        "smoke-check", help="run the smoke check notebook on a temporary cluster"
    ).set_defaults(handler=cmd_smoke_check)
    status = commands.add_parser(
        "status", help="report the final state of a run, waiting if it is still running"
    )
    status.add_argument("run_id", type=int)
    status.set_defaults(handler=cmd_status)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    # Root stays at WARNING so SDK and HTTP library logs do not flood the output
    logging.basicConfig(
        level=logging.WARNING, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    logger.setLevel(logging.INFO)

    try:
        config = load_config(args.config, args.target)
        w = WorkspaceClient(profile=args.profile)
        logger.info(
            "Workspace %s, user %s, target %s",
            w.config.host,
            w.current_user.me().user_name,
            args.target,
        )
        steps = args.handler(w, config, args)
    except Exception:
        logger.exception("Command '%s' failed", args.command)
        return EXIT_FAILED

    log_summary(steps)
    return EXIT_OK if all(result.succeeded for _, result in steps) else EXIT_FAILED


if __name__ == "__main__":
    sys.exit(main())
