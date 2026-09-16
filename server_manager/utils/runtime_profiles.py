"""Immutable FL Runtime profiles selected by the owning FedOps Web Task.

The API accepts a profile name, never a caller-supplied Git revision or package
version. This prevents an existing Task from silently moving to the newest FedOps
transport when its aggregation server Pod is recreated.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class RuntimeProfile:
    name: str
    schema_version: int
    fedops_version: str
    source_revision: str


LEGACY_V1 = RuntimeProfile(
    name="legacy-v1",
    schema_version=1,
    fedops_version="1.1.30.13",
    source_revision="7cdd9840d7cacdef7ce3be96248a206fd06d496b",
)

FEDERATED_TASK_V2 = RuntimeProfile(
    name="federated-task-v2",
    schema_version=2,
    fedops_version="1.1.30.14",
    source_revision="b88732b4c75194034ba651df514cf5a32334150d",
)

FEDERATED_TASK_V3 = RuntimeProfile(
    name="federated-task-v3",
    schema_version=3,
    fedops_version="1.1.30.15",
    source_revision="fde3137f6e94bc4558352b109a8c87186d20208c",
)

_PROFILES = {
    LEGACY_V1.name: LEGACY_V1,
    FEDERATED_TASK_V2.name: FEDERATED_TASK_V2,
    FEDERATED_TASK_V3.name: FEDERATED_TASK_V3,
}

# Same Task schema, a new explicitly pinned runtime. Never move existing v3
# Releases to this revision just because their Pod is recreated.
FEDERATED_TASK_V3_EVALUATION = RuntimeProfile(
    name="federated-task-v3",
    schema_version=3,
    fedops_version="1.1.30.18",
    source_revision="733f1696edc234073f0c1cd1a96e6580bfbcffeb",
)


def resolve_runtime_profile(name: Optional[str], source_revision: Optional[str] = None) -> RuntimeProfile:
    """Resolve only a trusted profile; omitted callers remain legacy-compatible."""
    selected = name or LEGACY_V1.name
    if source_revision:
        candidates = list(_PROFILES.values()) + [FEDERATED_TASK_V3_EVALUATION]
        for profile in candidates:
            if profile.name == selected and profile.source_revision == source_revision:
                return profile
        raise ValueError("Unsupported immutable FedOps Runtime revision")
    try:
        return _PROFILES[selected]
    except KeyError as error:
        raise ValueError(f"Unsupported FedOps Runtime contract: {selected}") from error
