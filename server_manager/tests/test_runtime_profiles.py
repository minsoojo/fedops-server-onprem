import unittest

from utils.runtime_profiles import (
    FEDERATED_TASK_V3,
    FEDERATED_TASK_V3_EVALUATION,
    FEDERATED_TASK_V3_ONPREM,
    FEDERATED_TASK_V2,
    LEGACY_V1,
    resolve_runtime_profile,
)


class RuntimeProfilesTest(unittest.TestCase):
    def test_omitted_contract_is_legacy(self):
        self.assertEqual(resolve_runtime_profile(None), LEGACY_V1)
        self.assertEqual(LEGACY_V1.fedops_version, "1.1.30.13")
        self.assertEqual(
            LEGACY_V1.source_revision,
            "7cdd9840d7cacdef7ce3be96248a206fd06d496b",
        )

    def test_federated_task_v2_is_explicit(self):
        self.assertEqual(resolve_runtime_profile("federated-task-v2"), FEDERATED_TASK_V2)
        self.assertEqual(FEDERATED_TASK_V2.fedops_version, "1.1.30.14")

    def test_federated_task_v3_pins_agent_studio_participation_runtime(self):
        self.assertEqual(resolve_runtime_profile("federated-task-v3"), FEDERATED_TASK_V3)
        self.assertEqual(FEDERATED_TASK_V3.schema_version, 3)
        self.assertEqual(FEDERATED_TASK_V3.fedops_version, "1.1.30.15")
        self.assertEqual(
            FEDERATED_TASK_V3.source_revision,
            "fde3137f6e94bc4558352b109a8c87186d20208c",
        )

    def test_onprem_revision_is_explicit_and_keeps_old_profiles(self):
        profile = FEDERATED_TASK_V3_ONPREM
        self.assertEqual(resolve_runtime_profile(profile.name, profile.source_revision), profile)
        self.assertEqual(profile.fedops_version, "1.1.30.19+onprem.20260916")
        self.assertEqual(resolve_runtime_profile(profile.name), FEDERATED_TASK_V3)
        with self.assertRaises(ValueError):
            resolve_runtime_profile("legacy-v1", profile.source_revision)

    def test_arbitrary_version_or_revision_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "Unsupported FedOps Runtime contract"):
            resolve_runtime_profile("main")

    def test_evaluation_revision_is_explicit_and_old_v3_stays_pinned(self):
        new = FEDERATED_TASK_V3_EVALUATION
        self.assertEqual(resolve_runtime_profile(new.name, new.source_revision), new)
        self.assertEqual(resolve_runtime_profile(new.name), FEDERATED_TASK_V3)
        self.assertEqual(resolve_runtime_profile(new.name, FEDERATED_TASK_V3.source_revision), FEDERATED_TASK_V3)
        with self.assertRaises(ValueError):
            resolve_runtime_profile(new.name, "arbitrary-commit")
        with self.assertRaises(ValueError):
            resolve_runtime_profile("legacy-v1", new.source_revision)


if __name__ == "__main__":
    unittest.main()
