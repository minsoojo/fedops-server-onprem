"""Deployment inputs shared by Manager's current and legacy Task paths."""
import os
from urllib.parse import urlsplit


def required_url(key):
    value = os.getenv(key, '').strip().rstrip('/')
    parsed = urlsplit(value)
    if parsed.scheme not in ('http', 'https') or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError(f'{key} must be an HTTP(S) base URL without credentials, query, or fragment')
    try:
        parsed.port
    except ValueError:
        raise ValueError(f'{key} has an invalid port') from None
    return value


TARGET_NAMESPACE = os.getenv('TARGET_NAMESPACE', 'fedops')
VIRTUAL_SERVICE_NAME = os.getenv('FEDOPS_ISTIO_VIRTUAL_SERVICE', 'fedops-virtualservice')
ISTIO_GATEWAY = os.getenv('FEDOPS_ISTIO_GATEWAY', 'istio-system/istio-ingressgateway')
ISTIO_HOST = os.getenv('FEDOPS_ISTIO_HOST', f'{TARGET_NAMESPACE}.svc.cluster.local')
TASK_IMAGE = os.getenv('FEDOPS_TASK_IMAGE', 'docker.io/tpah20/torch_cpu_amd:latest')
TASK_S3_SECRET_NAME = os.getenv('FEDOPS_TASK_S3_SECRET_NAME', 's3secret')


def task_connection_env():
    """Ordinary values only: access keys remain SecretKeyRef in the operators."""
    manager = required_url('FEDOPS_SERVER_MANAGER_URL')
    result = {'SERVER_MANAGER_URL': manager, 'FEDOPS_SERVER_MANAGER_URL': manager,
              'FEDOPS_CLIENT_PERFORMANCE_URL': required_url('FEDOPS_CLIENT_PERFORMANCE_URL')}
    for key in ('S3_ENDPOINT_URL', 'S3_PUBLIC_ENDPOINT_URL', 'S3_FORCE_PATH_STYLE', 'REGION_NAME'):
        if os.getenv(key):
            result[key] = required_url(key) if key.endswith('ENDPOINT_URL') else os.environ[key]
    if result.get('S3_FORCE_PATH_STYLE') not in (None, 'true', 'false'):
        raise ValueError('S3_FORCE_PATH_STYLE must be true or false')
    return result
