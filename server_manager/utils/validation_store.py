"""Stdlib-only receiver executed in the Task Pod; never extracts arbitrary paths."""
import base64
import fcntl
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import sys
import tempfile
import zipfile

MAX_BYTES = 512 * 1024 * 1024
MAX_ARCHIVE = MAX_BYTES + 8 * 1024 * 1024
MAX_FILES = 10000


def store(archive_path, digest, root):
    root = Path(root)
    if root.is_symlink():
        raise ValueError('Validation root cannot be a symlink')
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    # Serialize per-Task publication/quota checks, including retries.
    with os.fdopen(os.open(root / '.upload.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600), 'w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        return _store_locked(archive_path, digest, root)


def _store_locked(archive_path, digest, root):
    root = Path(root)
    if not re.fullmatch(r'[a-f0-9]{64}', digest):
        raise ValueError('Invalid dataset checksum')
    if root.is_symlink():
        raise ValueError('Validation root cannot be a symlink')
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    data_path = 'validation-' + digest
    metadata = root / '.metadata'
    if metadata.is_symlink():
        raise ValueError('Invalid metadata directory')
    metadata.mkdir(mode=0o700, exist_ok=True)
    destination = root / data_path
    manifest = metadata / (data_path + '.json')
    hasher = hashlib.sha256()
    with open(archive_path, 'rb') as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b''):
            hasher.update(chunk)
    if hasher.hexdigest() != digest:
        raise ValueError('Validation archive checksum mismatch')
    # Retry after an interrupted response returns the same dataset, never overwrites it.
    if destination.exists():
        if destination.is_symlink() or not manifest.is_file() or manifest.is_symlink():
            raise ValueError('Existing dataset is incomplete; contact the operator')
        return json.loads(manifest.read_text())
    # Bound retained Task data as well as each request.
    used = sum(p.stat().st_size for p in root.rglob('*') if p.is_file() and not p.is_symlink())
    with tempfile.TemporaryDirectory(prefix='.upload-', dir=root) as temporary:
        staging = Path(temporary) / 'data'
        staging.mkdir()
        count = total = 0
        seen = set()
        with zipfile.ZipFile(archive_path) as archive:
            for info in archive.infolist():
                name = info.filename
                parts = name.split('/')
                mode = info.external_attr >> 16
                if (info.is_dir() or name != info.orig_filename or name.startswith('/') or '\\' in name
                        or any(p in ('', '.', '..') or p.startswith('.') for p in parts)
                        or any(ord(c) < 32 for c in name) or name in seen
                        or (stat.S_IFMT(mode) not in (0, stat.S_IFREG))
                        or info.flag_bits & 1):
                    raise ValueError('Unsafe validation archive entry')
                seen.add(name)
                count += 1
                total += info.file_size
                if count > MAX_FILES or total > MAX_BYTES or used + total > 5 * 1024**3:
                    raise ValueError('Validation dataset or Task storage limit exceeded')
                target = staging.joinpath(*PurePosixPath(name).parts)
                target.parent.mkdir(parents=True, exist_ok=True)
                copied = 0
                with archive.open(info) as source, target.open('xb') as output:
                    while chunk := source.read(1024 * 1024):
                        copied += len(chunk)
                        if copied > info.file_size:
                            raise ValueError('Invalid validation entry size')
                        output.write(chunk)
                if copied != info.file_size:
                    raise ValueError('Truncated validation file')
                target.chmod(0o400)
        if not count:
            raise ValueError('Validation archive is empty')
        result = {'dataPath': data_path, 'sha256': digest, 'fileCount': count, 'totalBytes': total}
        staging.rename(destination)
        manifest.write_text(json.dumps(result))
        manifest.chmod(0o600)
        return result


def inventory(root):
    root = Path(root)
    meta = root / '.metadata'
    if root.is_symlink() or meta.is_symlink():
        raise ValueError('Invalid validation directory')
    if not meta.exists():
        return {'items': []}
    return {'items': [json.loads(p.read_text()) for p in sorted(meta.glob('validation-*.json'))
                      if not p.is_symlink() and (root / p.stem).is_dir()]}


def main():
    root = Path('/app/data/server-validation')
    try:
        if sys.argv[1] == 'list':
            result = inventory(root)
        else:
            # Stream line-framed base64; no binary payload is placed in commands/logs.
            with tempfile.TemporaryFile() as received:
                total = 0
                while True:
                    line = sys.stdin.buffer.readline(100000)
                    if line == b'.\n':
                        break
                    if not line or len(line) >= 100000:
                        raise ValueError('Incomplete validation upload')
                    data = base64.b64decode(line.strip(), validate=True)
                    total += len(data)
                    if total > MAX_ARCHIVE:
                        raise ValueError('Validation archive too large')
                    received.write(data)
                received.seek(0)
                with tempfile.NamedTemporaryFile() as bundle:
                    shutil.copyfileobj(received, bundle)
                    bundle.flush()
                    result = store(bundle.name, sys.argv[2], root)
        print('FEDOPS_DATA_RESULT=' + json.dumps({'success': True, **result}), flush=True)
    except Exception as error:
        print('FEDOPS_DATA_RESULT=' + json.dumps({'success': False, 'error': str(error)}), flush=True)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
