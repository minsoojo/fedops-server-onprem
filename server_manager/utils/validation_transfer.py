"""Internal Web-to-Task-Pod validation transfer. Web authenticates the Owner."""
from utils.deployment_config import TARGET_NAMESPACE
import base64
import json
from pathlib import Path
import re
import time

from kubernetes import client, config
from kubernetes.stream import stream


def transfer(task_id, archive=None, digest=None):
    if not re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9_.-]{0,62}', task_id):
        raise ValueError('Invalid Task runtime key')
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config('config.txt')
    api = client.CoreV1Api()
    pods = api.list_namespaced_pod(TARGET_NAMESPACE, label_selector=f'task_id={task_id}').items
    ready = [p for p in pods if p.status.phase == 'Running' and not p.metadata.deletion_timestamp
             and any(c.type == 'Ready' and c.status == 'True' for c in (p.status.conditions or []))]
    if len(ready) != 1:
        raise ValueError('Prepare exactly one Ready Task server before uploading validation data')
    script = Path(__file__).with_name('validation_store.py').read_text()
    command = ['python', '-c', script, 'upload' if archive else 'list']
    if archive:
        command.append(digest)
    response = stream(api.connect_get_namespaced_pod_exec, ready[0].metadata.name, TARGET_NAMESPACE,
                      command=command, stdin=bool(archive), stdout=True, stderr=True,
                      tty=False, _preload_content=False)
    output = ''
    deadline = time.monotonic() + 300
    try:
        if archive:
            with open(archive, 'rb') as source:
                while chunk := source.read(48 * 1024):
                    if time.monotonic() > deadline or not response.is_open():
                        raise ValueError('Validation upload interrupted; retry with the same files')
                    response.write_stdin(base64.b64encode(chunk).decode() + '\n')
                response.write_stdin('.\n')
        while response.is_open():
            if time.monotonic() > deadline:
                raise ValueError('Validation server check timed out')
            response.update(timeout=1)
            if response.peek_stdout():
                output += response.read_stdout()
            # Drain but never return runtime logs or data content.
            if response.peek_stderr():
                response.read_stderr()
            if len(output) > 1024 * 1024:
                raise ValueError('Invalid validation server response')
        output += response.read_stdout()
    finally:
        response.close()
    line = next((line for line in output.splitlines() if line.startswith('FEDOPS_DATA_RESULT=')), None)
    if line is None:
        raise ValueError('Validation receiver unavailable; prepare the Task server')
    result = json.loads(line.split('=', 1)[1])
    if not result.get('success'):
        raise ValueError(result.get('error', 'Validation transfer failed'))
    return result
