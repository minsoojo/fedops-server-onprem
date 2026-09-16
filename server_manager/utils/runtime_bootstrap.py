"""Build the classic FedOps aggregation runtime on the task PVC.

Legacy/v2 servers use a container image as a compute base, but their Python
packages must survive a Kubernetes container restart. The task PVC is the
runtime-state boundary, so keep the virtual environment under /app/data rather
than in the container writable layer.
"""

from __future__ import annotations

import shlex


RUNTIME_ROOT = "/app/data/runtime"
RUNTIME_PYTHON = f"{RUNTIME_ROOT}/venv/bin/python"
CONTAINER_READY_MARKER = "/tmp/fedops-runtime.ready"

# Proven versions from the healthy legacy aggregation Pods on 2026-08-14.
# Model-specific packages remain owned by each task's requirements.txt.
CLASSIC_RUNTIME_PACKAGES = (
    "flwr==1.30.0",
    "hydra-core==1.3.5",
    "omegaconf==2.3.1",
    "boto3==1.43.67",
    "optuna==4.9.0",
    "scikit-learn==1.7.2",
    "numpy==1.26.4",
)

_IMPORT_SMOKE_TEST = "import fedops,hydra,flwr,omegaconf,numpy,boto3,optuna,sklearn"
_PIP_NETWORK_OPTIONS = (
    "--disable-pip-version-check --no-cache-dir "
    "--default-timeout 180 --retries 10"
)


def classic_runtime_bootstrap_shell() -> str:
    """Return a fail-closed shell fragment for a persistent classic runtime."""

    packages = " ".join(shlex.quote(package) for package in CLASSIC_RUNTIME_PACKAGES)
    return (
        "echo 'Preparing persistent classic FedOps Runtime...' && "
        f"rm -f {shlex.quote(CONTAINER_READY_MARKER)} && "
        f"RUNTIME_ROOT={shlex.quote(RUNTIME_ROOT)} && "
        f"VENV_PY={shlex.quote(RUNTIME_PYTHON)} && "
        "mkdir -p \"$RUNTIME_ROOT\" && "
        "rm -f \"$RUNTIME_ROOT/runtime.ready\" && "
        "REQ_SHA=none && "
        "if [ -f /app/code/requirements.txt ]; then "
        "REQ_SHA=$(sha256sum /app/code/requirements.txt | awk '{print $1}'); "
        "fi && "
        "PYTHON_ABI=$(python3 -c 'import sys; print(\"%s.%s\" % sys.version_info[:2])') && "
        "RUNTIME_ID=\"contract=$FEDOPS_RUNTIME_CONTRACT;fedops=$FEDOPS_PACKAGE_VERSION;"
        "source=$FEDOPS_SOURCE_REVISION;python=$PYTHON_ABI;requirements=$REQ_SHA\" && "
        "if [ -x \"$VENV_PY\" ] "
        "&& [ \"$(cat \"$RUNTIME_ROOT/runtime.identity\" 2>/dev/null || true)\" = \"$RUNTIME_ID\" ] "
        f"&& \"$VENV_PY\" -c {shlex.quote(_IMPORT_SMOKE_TEST)}; then "
        "echo 'Reusing persistent classic FedOps Runtime'; "
        "else "
        "rm -rf \"$RUNTIME_ROOT/venv\" \"$RUNTIME_ROOT/runtime.identity\" && "
        "python3 -m venv --system-site-packages \"$RUNTIME_ROOT/venv\" && "
        f"\"$VENV_PY\" -m pip install {_PIP_NETWORK_OPTIONS} --ignore-installed --no-deps "
        "\"fedops==$FEDOPS_PACKAGE_VERSION\" && "
        f"\"$VENV_PY\" -m pip install {_PIP_NETWORK_OPTIONS} --ignore-installed {packages} && "
        "if [ -f /app/code/requirements.txt ]; then "
        "(grep -Eiv '^[[:space:]]*fedops([[:space:]]|[<>=!~]|$)' "
        "/app/code/requirements.txt || true) > /tmp/fedops-task-requirements.txt; "
        "if [ -s /tmp/fedops-task-requirements.txt ]; then "
        f"\"$VENV_PY\" -m pip install {_PIP_NETWORK_OPTIONS} "
        "-r /tmp/fedops-task-requirements.txt; "
        "fi; "
        "fi && "
        f"\"$VENV_PY\" -c {shlex.quote(_IMPORT_SMOKE_TEST)} && "
        "printf '%s' \"$RUNTIME_ID\" > \"$RUNTIME_ROOT/runtime.identity\"; "
        "fi && "
        "touch \"$RUNTIME_ROOT/runtime.ready\" && "
        f"touch {shlex.quote(CONTAINER_READY_MARKER)} && "
        "echo 'Persistent classic FedOps Runtime is ready' && "
    )


def classic_runtime_readiness_shell() -> str:
    """Return the Kubernetes readiness check for a classic runtime."""

    return (
        f"test -f {shlex.quote(CONTAINER_READY_MARKER)} "
        f"&& test -f {shlex.quote(RUNTIME_ROOT + '/runtime.ready')} "
        f"&& test -x {shlex.quote(RUNTIME_PYTHON)} "
        f"&& {shlex.quote(RUNTIME_PYTHON)} -c {shlex.quote(_IMPORT_SMOKE_TEST)}"
    )
