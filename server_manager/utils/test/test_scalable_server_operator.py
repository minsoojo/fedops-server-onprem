import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from utils import scalable_server_operator
from utils.runtime_profiles import FEDERATED_TASK_V3


class CampaignConfig:
    def dict(self):
        return {
            "schemaVersion": 1,
            "rounds": 2,
            "clientsPerRound": 1,
            "strategy": {"name": "FedAvg", "parameters": {}},
        }


class ScalableServerOperatorTest(unittest.TestCase):
    @patch.object(scalable_server_operator, "load_config")
    @patch.object(scalable_server_operator.client, "CoreV1Api")
    def test_long_task_id_creates_a_dns_label_service_with_full_task_selector(
        self,
        core_api_factory,
        _load_config,
    ):
        task_id = "jinyong-jeong-legacy-exercise-calorie-predictor-53c96d"

        scalable_server_operator.create_service_for_deployment(task_id)

        service = core_api_factory.return_value.create_namespaced_service.call_args.args[1]
        self.assertLessEqual(len(service.metadata.name), 63)
        self.assertEqual(service.metadata.labels["task_id"], task_id)
        self.assertEqual(service.spec.selector["task_id"], task_id)

    @patch.object(scalable_server_operator.time, "sleep")
    @patch.object(scalable_server_operator.time, "monotonic", side_effect=[0.0, 3.0])
    @patch.object(scalable_server_operator, "load_config")
    @patch.object(scalable_server_operator.client, "CoreV1Api")
    def test_external_ip_wait_has_a_bounded_timeout(
        self,
        core_api_factory,
        _load_config,
        _monotonic,
        sleep,
    ):
        core_api_factory.return_value.read_namespaced_service.return_value = SimpleNamespace(
            status=SimpleNamespace(load_balancer=SimpleNamespace(ingress=None))
        )

        with self.assertRaisesRegex(TimeoutError, "Timed out waiting for external IP"):
            scalable_server_operator.wait_for_external_ip(
                "fl-server-service-mnist",
                "fedops",
                timeout_seconds=2.0,
                poll_seconds=0.0,
            )

        sleep.assert_not_called()

    @patch.object(scalable_server_operator, "update_virtual_service")
    @patch.object(scalable_server_operator, "get_unused_port")
    @patch.object(
        scalable_server_operator,
        "wait_for_external_ip",
        side_effect=TimeoutError("external IP unavailable"),
    )
    @patch.object(scalable_server_operator, "create_service_for_deployment")
    @patch.object(
        scalable_server_operator,
        "create_persistent_volume_and_claim",
        return_value="fl-data-mnist",
    )
    @patch.object(scalable_server_operator, "load_config")
    @patch.object(scalable_server_operator.client, "AppsV1Api")
    def test_network_allocation_failure_preserves_created_resource_metadata(
        self,
        apps_api_factory,
        _load_config,
        _create_storage,
        _create_service,
        _wait_for_ip,
        get_port,
        update_route,
    ):
        status = {}
        with self.assertRaisesRegex(TimeoutError, "external IP unavailable"):
            scalable_server_operator.create_scalable_fl_server(
                task_id="mnist",
                fl_server_status=status,
                server_repo_addr="",
            )

        self.assertEqual(status["mnist"]["status"], "FL Server Error")
        self.assertEqual(status["mnist"]["deployment"], "fl-server-deploy-mnist")
        self.assertEqual(status["mnist"]["service_name"], "fl-server-service-mnist")
        self.assertEqual(status["mnist"]["pvc"], "fl-data-mnist")
        self.assertEqual(status["mnist"]["cpu"], "1")
        self.assertEqual(status["mnist"]["memory"], "2Gi")
        get_port.assert_not_called()
        update_route.assert_not_called()
        apps_api_factory.return_value.create_namespaced_deployment.assert_called_once()

    @patch.object(scalable_server_operator.time, "sleep")
    def test_deployment_creation_waits_for_terminating_predecessor(self, _sleep):
        apps_api = Mock()
        conflict = scalable_server_operator.ApiException(
            status=409,
            reason="object is being deleted",
        )
        apps_api.create_namespaced_deployment.side_effect = [conflict, None]
        apps_api.read_namespaced_deployment.return_value = SimpleNamespace(
            metadata=SimpleNamespace(deletion_timestamp="2026-08-14T00:00:00Z")
        )

        created = scalable_server_operator._create_deployment_after_prior_deletion(
            apps_api,
            "fedops",
            "fl-server-deploy-mnist",
            object(),
            poll_seconds=0,
        )

        self.assertTrue(created)
        self.assertEqual(apps_api.create_namespaced_deployment.call_count, 2)
        apps_api.read_namespaced_deployment.assert_called_once()

    @patch.object(scalable_server_operator, "load_config")
    @patch.object(scalable_server_operator.client, "CustomObjectsApi")
    @patch.object(scalable_server_operator.client, "CoreV1Api")
    @patch.object(scalable_server_operator.client, "AppsV1Api")
    def test_runtime_reset_preserves_pvc_and_pv_when_delete_pv_is_false(
        self,
        apps_api_factory,
        core_api_factory,
        custom_api_factory,
        _load_config,
    ):
        apps_api = Mock()
        apps_api_factory.return_value = apps_api
        core_api = Mock()
        core_api_factory.return_value = core_api
        core_api.read_namespaced_persistent_volume_claim.return_value = SimpleNamespace(
            metadata=SimpleNamespace(name="fl-data-mnist"),
            spec=SimpleNamespace(volume_name="fl-pv-mnist"),
        )
        custom_api = Mock()
        custom_api_factory.return_value = custom_api
        custom_api.get_namespaced_custom_object.side_effect = (
            scalable_server_operator.client.exceptions.ApiException(status=404)
        )

        report = scalable_server_operator.delete_fl_stack(
            task_id="mnist",
            namespace="fedops",
            delete_pv=False,
        )

        apps_api.delete_namespaced_deployment.assert_called_once()
        core_api.delete_namespaced_service.assert_called_once()
        core_api.delete_namespaced_persistent_volume_claim.assert_not_called()
        core_api.delete_persistent_volume.assert_not_called()
        self.assertIn({"pvc": "fl-data-mnist"}, report["preserved"])

    @patch.object(scalable_server_operator, "update_virtual_service")
    @patch.object(scalable_server_operator, "get_unused_port", return_value=40026)
    @patch.object(scalable_server_operator, "wait_for_external_ip", return_value="192.168.10.6")
    @patch.object(scalable_server_operator, "create_service_for_deployment")
    @patch.object(
        scalable_server_operator,
        "create_persistent_volume_and_claim",
        return_value="fl-data-mnist",
    )
    @patch.object(scalable_server_operator, "load_config")
    @patch.object(scalable_server_operator.client, "AppsV1Api")
    def test_v3_campaign_is_serialized_before_deployment_creation(
        self,
        apps_api_factory,
        _load_config,
        _create_storage,
        _create_service,
        _wait_for_ip,
        _get_port,
        _update_virtual_service,
    ):
        apps_api = Mock()
        apps_api_factory.return_value = apps_api
        release = SimpleNamespace(
            release_id="release-mnist",
            archive_url="https://example.invalid/release.zip",
            archive_sha256="a" * 64,
            model_url="https://example.invalid/model.safetensors",
            model_sha256="b" * 64,
            model_format="safetensors",
            fedops_version=FEDERATED_TASK_V3.fedops_version,
            source_revision=FEDERATED_TASK_V3.source_revision,
        )
        task_data = SimpleNamespace(
            runtime_contract=FEDERATED_TASK_V3.name,
            runtime_release=release,
            campaign_config=CampaignConfig(),
            data_type="Image",
            model_type="AI",
            learning_rate="",
            num_epochs="",
            batch_size="",
            num_rounds="2",
            client_per_round="1",
            strategy="FedAvg",
            strategy_params={},
            xai_enabled="false",
            sba_fl_target="",
            llm_params={},
            dataset_params={},
            yaml_config=None,
        )
        status = {}

        deployment_name = scalable_server_operator.create_scalable_fl_server(
            task_id="mnist",
            fl_server_status=status,
            server_repo_addr="",
            task_data=task_data,
        )

        self.assertEqual(deployment_name, "fl-server-deploy-mnist")
        deployment = apps_api.create_namespaced_deployment.call_args.args[1]
        env = {
            variable.name: variable.value
            for variable in deployment.spec.template.spec.containers[0].env
            if variable.value is not None
        }
        self.assertEqual(
            env["FEDOPS_CAMPAIGN_CONFIG"],
            '{"schemaVersion":1,"rounds":2,"clientsPerRound":1,"strategy":{"name":"FedAvg","parameters":{}}}',
        )
        container = deployment.spec.template.spec.containers[0]
        bootstrap = container.args[0]
        self.assertIn("uv sync --frozen --link-mode copy", bootstrap)
        self.assertNotIn("--extra participate", bootstrap)
        self.assertIn("torch.version.cuda is None", bootstrap)
        self.assertIn("torchvision.extension._has_ops()", bootstrap)
        self.assertIn("FedOps 1.3 Runtime Release bootstrap failed", bootstrap)
        self.assertLess(
            bootstrap.index("FedOps 1.3 Runtime Release bootstrap failed"),
            bootstrap.index("Setting up Pytorch (AI) FL code"),
        )
        readiness = " ".join(container.readiness_probe._exec.command)
        self.assertIn("release.identity", readiness)
        self.assertIn("torch.version.cuda is None", readiness)
        self.assertIn("torchvision.extension._has_ops()", readiness)
        self.assertEqual(status["mnist"]["campaign"]["rounds"], 2)


if __name__ == "__main__":
    unittest.main()
