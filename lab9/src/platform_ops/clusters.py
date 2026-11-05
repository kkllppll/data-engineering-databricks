import logging
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.compute import (
    ClusterDetails,
    DataSecurityMode,
    RuntimeEngine,
)

from platform_ops.config import ClusterConfig

logger = logging.getLogger(__name__)

START_TIMEOUT = timedelta(minutes=20)
CLUSTER_TAGS = {"ManagedBy": "lab9-platform-ops"}


def find_policy_id(w: WorkspaceClient, policy_name: str) -> str:
    matches = [p.policy_id for p in w.cluster_policies.list() if p.name == policy_name]
    if len(matches) != 1:
        raise LookupError(
            f"Expected one cluster policy named '{policy_name}', found {len(matches)}"
        )
    return matches[0]


def delete_cluster(w: WorkspaceClient, cluster_id: str) -> None:
    # permanent_delete removes the cluster; a plain delete only terminates it
    # and keeps it in the list for 30 days
    try:
        w.clusters.permanent_delete(cluster_id)
    except NotFound:
        logger.info("Cluster %s is already deleted", cluster_id)
        return
    logger.info("Cluster %s deleted", cluster_id)


@contextmanager
def temporary_cluster(w: WorkspaceClient, config: ClusterConfig) -> Iterator[str]:
    """Create a single-node cluster, yield its id once RUNNING, always delete it afterwards"""
    name = f"{config.name_prefix}-{datetime.now():%Y%m%d-%H%M%S}"
    waiter = w.clusters.create(
        cluster_name=name,
        spark_version=w.clusters.select_spark_version(
            long_term_support=True, scala="2.13"
        ),
        node_type_id=config.node_type_id,
        policy_id=find_policy_id(w, config.policy_name),
        num_workers=0,
        spark_conf={
            "spark.databricks.cluster.profile": "singleNode",
            "spark.master": "local[*]",
        },
        custom_tags={"ResourceClass": "SingleNode", **CLUSTER_TAGS},
        data_security_mode=DataSecurityMode.SINGLE_USER,
        single_user_name=w.current_user.me().user_name,
        runtime_engine=RuntimeEngine.STANDARD,
        autotermination_minutes=config.autotermination_minutes,
    )
    cluster_id = waiter.cluster_id
    logger.info("Cluster '%s' requested, id=%s", name, cluster_id)

    last_state = None

    def log_state(cluster: ClusterDetails) -> None:
        nonlocal last_state
        if cluster.state != last_state:
            logger.info("Cluster %s: %s", cluster_id, cluster.state.value)
            last_state = cluster.state

    try:
        waiter.result(timeout=START_TIMEOUT, callback=log_state)
        logger.info("Cluster %s is running", cluster_id)
        yield cluster_id
    finally:
        try:
            delete_cluster(w, cluster_id)
        # broad on purpose: a failed cleanup must not hide the original error;
        # autotermination is the safety net
        except Exception:
            logger.exception("Could not delete cluster %s", cluster_id)
