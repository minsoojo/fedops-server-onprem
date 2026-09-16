import hashlib
import json
from pathlib import Path
import tempfile
import unittest
import zipfile
from unittest.mock import patch
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from utils.validation_store import store, inventory


class ValidationStoreTests(unittest.TestCase):
    def bundle(self, path, entries):
        with zipfile.ZipFile(path, 'w') as archive:
            for name, content in entries:
                archive.writestr(name, content)
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def test_real_zip_retry_and_task_isolation(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            archive = root / 'a.zip'
            digest = self.bundle(archive, [('holdout.csv', 'x,y\n1,2')])
            result = store(archive, digest, root / 'task-a')
            self.assertEqual(result, store(archive, digest, root / 'task-a'))
            self.assertEqual(inventory(root / 'task-a')['items'], [result])
            self.assertEqual(inventory(root / 'task-b'), {'items': []})
            self.assertEqual((root / 'task-a' / result['dataPath'] / 'holdout.csv').read_text(), 'x,y\n1,2')

    def test_bad_checksum_unsafe_files_and_limits_leave_no_dataset(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            archive = root / 'a.zip'
            target = root / 'server'
            for name in ('../escape', '/absolute', 'a\\b', '.secret', 'a//b', 'a\nb'):
                digest = self.bundle(archive, [(name, 'x')])
                with self.subTest(name=name), self.assertRaises(ValueError):
                    store(archive, digest, target)
                self.assertEqual(inventory(target), {'items': []})
            link = zipfile.ZipInfo('link')
            link.external_attr = 0o120777 << 16
            digest = self.bundle(archive, [(link, '/etc/passwd')])
            with self.assertRaises(ValueError):
                store(archive, digest, target)
            digest = self.bundle(archive, [('data.csv', '123')])
            with self.assertRaisesRegex(ValueError, 'checksum'):
                store(archive, '0' * 64, target)
            with patch('utils.validation_store.MAX_BYTES', 2), self.assertRaisesRegex(ValueError, 'limit'):
                store(archive, digest, target)
            self.assertEqual(inventory(target), {'items': []})
