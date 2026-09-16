"""Configuration boundary for node-local task storage."""

from __future__ import annotations

import os
import posixpath
import re


HOSTPATH_STORAGE_NODE_ENV = "FEDOPS_HOSTPATH_STORAGE_NODE"


def hostpath_storage_node() -> str:
    """Return the Kubernetes node that owns task-local hostPath storage."""

    node_name = os.environ.get(HOSTPATH_STORAGE_NODE_ENV, "").strip()
    if not node_name:
        raise RuntimeError(
            f"{HOSTPATH_STORAGE_NODE_ENV} must identify the node that owns "
            "the task hostPath storage"
        )
    return node_name


def task_storage_path(task_id):
    base = os.getenv('FEDOPS_TASK_STORAGE_BASE_PATH', '').strip()
    if not base.startswith('/') or base == '/':
        raise ValueError('FEDOPS_TASK_STORAGE_BASE_PATH must be a dedicated absolute POSIX directory')
    return posixpath.join(base.rstrip('/'), task_id)


def task_storage_capacity():
    value = os.getenv('FEDOPS_TASK_STORAGE_CAPACITY', '20Gi')
    if not re.fullmatch(r'[1-9][0-9]*(?:Ki|Mi|Gi|Ti)', value):
        raise ValueError('FEDOPS_TASK_STORAGE_CAPACITY must be a positive Ki/Mi/Gi/Ti quantity')
    return value
