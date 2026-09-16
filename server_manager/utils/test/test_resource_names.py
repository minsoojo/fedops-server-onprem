import unittest

from utils.resource_names import fl_server_service_name


class ResourceNamesTest(unittest.TestCase):
    def test_preserves_existing_short_service_name(self):
        self.assertEqual(
            fl_server_service_name("sbastepsfl"),
            "fl-server-service-sbastepsfl",
        )

    def test_shortens_long_service_name_without_changing_task_identity(self):
        task_id = "jinyong-jeong-legacy-exercise-calorie-predictor-53c96d"

        service_name = fl_server_service_name(task_id)

        self.assertLessEqual(len(service_name), 63)
        self.assertTrue(
            service_name.startswith(
                "fl-server-service-exercise-calorie-predictor-53c96d"
            )
        )
        self.assertNotIn("jinyong-jeong-legacy", service_name)
        self.assertEqual(service_name, fl_server_service_name(task_id))
        self.assertNotEqual(
            service_name,
            fl_server_service_name(f"{task_id}-different"),
        )
