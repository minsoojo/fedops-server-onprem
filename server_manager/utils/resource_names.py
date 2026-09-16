"""Stable Kubernetes resource names derived from the full FedOps Task ID."""

from __future__ import annotations

import hashlib


KUBERNETES_DNS_LABEL_MAX_LENGTH = 63
FL_SERVER_SERVICE_PREFIX = "fl-server-service-"


def kubernetes_dns_label(prefix: str, identity: str) -> str:
    """Return a deterministic DNS label while preserving legacy short names.

    Kubernetes Service names are limited to 63 characters. Existing FedOps
    Tasks below that limit keep their exact historical resource name; only a
    name that would exceed the limit keeps the Task-oriented trailing portion
    instead of the Owner-oriented prefix, then receives a stable hash.
    The full Task ID remains the Kubernetes selector and FedOps identity.
    """
    candidate = f"{prefix}{identity}"
    if len(candidate) <= KUBERNETES_DNS_LABEL_MAX_LENGTH:
        return candidate

    digest = hashlib.sha256(candidate.encode("utf-8")).hexdigest()[:10]
    identity_length = (
        KUBERNETES_DNS_LABEL_MAX_LENGTH - len(prefix) - len(digest) - 1
    )
    shortened_identity = identity[-identity_length:].strip("-")
    return f"{prefix}{shortened_identity}-{digest}"


def fl_server_service_name(task_id: str) -> str:
    """Return the compatible Kubernetes Service name for a FedOps Task."""
    return kubernetes_dns_label(FL_SERVER_SERVICE_PREFIX, task_id)
