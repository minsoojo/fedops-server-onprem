import unittest
from unittest.mock import patch

from utils.storage_config import (
    HOSTPATH_STORAGE_NODE_ENV,
    hostpath_storage_node,
)
from utils.runtime_bootstrap import (
    CLASSIC_RUNTIME_PACKAGES,
    CONTAINER_READY_MARKER,
    RUNTIME_PYTHON,
    classic_runtime_bootstrap_shell,
    classic_runtime_readiness_shell,
)


class RuntimeBootstrapTest(unittest.TestCase):
    @patch.dict("os.environ", {HOSTPATH_STORAGE_NODE_ENV: "ccl-e-server"}, clear=False)
    def test_hostpath_storage_node_is_explicit(self):
        self.assertEqual(hostpath_storage_node(), "ccl-e-server")

    @patch.dict("os.environ", {}, clear=True)
    def test_hostpath_storage_node_cannot_be_implicit(self):
        with self.assertRaisesRegex(RuntimeError, HOSTPATH_STORAGE_NODE_ENV):
            hostpath_storage_node()

    def test_classic_runtime_is_persisted_on_task_pvc(self):
        command = classic_runtime_bootstrap_shell()

        self.assertIn("/app/data/runtime/venv", command)
        self.assertIn("runtime.identity", command)
        self.assertIn("runtime.ready", command)
        self.assertIn(CONTAINER_READY_MARKER, command)
        self.assertIn("--default-timeout 180 --retries 10", command)
        self.assertIn("--ignore-installed --no-deps", command)
        self.assertIn('fedops==$FEDOPS_PACKAGE_VERSION', command)
        self.assertIn("grep -Eiv", command)

    def test_server_import_contract_is_checked_before_ready(self):
        command = classic_runtime_bootstrap_shell()
        readiness = classic_runtime_readiness_shell()

        for package in ("fedops", "hydra", "flwr", "omegaconf"):
            self.assertIn(package, command)
            self.assertIn(package, readiness)
        self.assertIn(RUNTIME_PYTHON, readiness)
        self.assertIn(CONTAINER_READY_MARKER, readiness)

    def test_runtime_packages_match_proven_legacy_versions(self):
        self.assertIn("flwr==1.30.0", CLASSIC_RUNTIME_PACKAGES)
        self.assertIn("hydra-core==1.3.5", CLASSIC_RUNTIME_PACKAGES)
        self.assertIn("numpy==1.26.4", CLASSIC_RUNTIME_PACKAGES)


if __name__ == "__main__":
    unittest.main()
