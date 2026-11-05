from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True)
class ClusterConfig:
    policy_name: str
    node_type_id: str
    name_prefix: str
    autotermination_minutes: int


@dataclass(frozen=True)
class SmokeCheckConfig:
    catalog: str
    schema_prefix: str


@dataclass(frozen=True)
class TargetConfig:
    job_name: str
    cluster: ClusterConfig
    smoke_check: SmokeCheckConfig


def load_config(path: Path, target: str) -> TargetConfig:
    targets = yaml.safe_load(path.read_text(encoding="utf-8"))
    if target not in targets:
        raise ValueError(
            f"Unknown target '{target}', expected one of: {', '.join(targets)}"
        )
    section = targets[target]
    return TargetConfig(
        job_name=section["job_name"],
        cluster=ClusterConfig(**section["cluster"]),
        smoke_check=SmokeCheckConfig(**section["smoke_check"]),
    )
