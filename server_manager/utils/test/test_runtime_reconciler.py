import unittest
from types import SimpleNamespace

from utils.runtime_reconciler import (
    build_runtime_status,
    find_service_route_port,
    deployment_release_identity,
    merge_runtime_status,
)


def namespace(**kwargs):
    return SimpleNamespace(**kwargs)


class RuntimeReconcilerTest(unittest.TestCase):
    def test_refreshes_gateway_route_without_losing_live_server_state(self):
        current = {
            "status": "FL Server Running",
            "port": 40032,
            "task_data": {"task_id": "mnist"},
        }
        recovered = {
            "status": "FL Server created",
            "port": 40028,
            "service_name": "fl-server-service-mnist",
        }

        merged = merge_runtime_status(current, recovered)

        self.assertEqual(merged["port"], 40028)
        self.assertEqual(merged["status"], "FL Server Running")
        self.assertEqual(merged["task_data"], {"task_id": "mnist"})

    def test_recovers_exact_release_identity_from_pod_template(self):
        deployment = SimpleNamespace(
            spec=SimpleNamespace(
                template=SimpleNamespace(
                    metadata=SimpleNamespace(annotations={
                        "fedops.io/release-id": "release-7",
                        "fedops.io/release-sha256": "a" * 64,
                    })
                )
            )
        )
        self.assertEqual(deployment_release_identity(deployment), {
            "release_id": "release-7",
            "release_sha256": "a" * 64,
        })

    def test_finds_gateway_port_for_task_service(self):
        virtual_service = {
            "spec": {
                "tcp": [
                    {
                        "match": [{"port": 40026}],
                        "route": [{
                            "destination": {
                                "host": "fl-server-service-sbastepsfl.fedops.svc.cluster.local",
                                "port": {"number": 80},
                            },
                        }],
                    },
                    {
                        "match": [{"port": 40027}],
                        "route": [{
                            "destination": {
                                "host": "fl-server-service-sbaweightfl.fedops.svc.cluster.local",
                                "port": {"number": 80},
                            },
                        }],
                    },
                ],
            },
        }

        self.assertEqual(
            find_service_route_port(
                virtual_service,
                "fl-server-service-sbaweightfl",
                "fedops",
            ),
            40027,
        )

    def test_builds_allocated_runtime_status_from_kubernetes_objects(self):
        deployment = namespace(
            metadata=namespace(name="fl-server-deploy-sbastepsfl"),
            spec=namespace(
                replicas=1,
                template=namespace(
                    spec=namespace(
                        containers=[
                            namespace(
                                resources=namespace(
                                    requests={"cpu": "1", "memory": "2Gi"},
                                    limits={"cpu": "1", "memory": "2Gi"},
                                ),
                                env=[
                                    namespace(name="FL_YAML_CONFIG", value="task_id: sbastepsfl"),
                                    namespace(
                                        name="FEDOPS_CAMPAIGN_CONFIG",
                                        value='{"schemaVersion":1,"rounds":5,"clientsPerRound":2,"strategy":{"name":"FedAvg","parameters":{}}}',
                                    ),
                                ],
                            ),
                        ],
                    ),
                ),
            ),
            status=namespace(ready_replicas=1),
        )
        service = namespace(
            metadata=namespace(name="fl-server-service-sbastepsfl"),
            status=namespace(
                load_balancer=namespace(
                    ingress=[namespace(ip="192.168.10.6", hostname=None)],
                ),
            ),
        )
        pvc = namespace(metadata=namespace(name="fl-data-sbastepsfl"))

        status = build_runtime_status(
            task_id="sbastepsfl",
            deployment=deployment,
            service=service,
            pvc=pvc,
            port=40026,
        )

        self.assertEqual(status["status"], "FL Server created")
        self.assertEqual(status["port"], 40026)
        self.assertEqual(status["external_ip"], "192.168.10.6")
        self.assertEqual(status["cpu"], "1")
        self.assertEqual(status["memory"], "2Gi")
        self.assertEqual(status["pvc"], "fl-data-sbastepsfl")
        self.assertTrue(status["yaml_saved"])
        self.assertEqual(status["campaign"]["rounds"], 5)
        self.assertEqual(status["campaign"]["clientsPerRound"], 2)
        self.assertEqual(status["recovered_from"], "kubernetes")


if __name__ == "__main__":
    unittest.main()
