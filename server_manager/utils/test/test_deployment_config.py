import os
import unittest
from unittest.mock import Mock, patch
from utils import deployment_config as settings
from utils import scalable_server_operator as operator
from utils.storage_config import task_storage_path, task_storage_capacity


class DeploymentConfigurationTests(unittest.TestCase):
    def test_missing_address_and_invalid_style_fail(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(ValueError): settings.task_connection_env()
        with patch.dict(os.environ, {'FEDOPS_SERVER_MANAGER_URL': 'http://manager.invalid',
            'FEDOPS_CLIENT_PERFORMANCE_URL': 'http://perf.invalid', 'S3_FORCE_PATH_STYLE': 'yes'}, clear=True):
            with self.assertRaises(ValueError): settings.task_connection_env()

    def test_generated_deployment_forwards_addresses_and_preserves_secret_references(self):
        for name in ('a', 'b'):
            values = {'FEDOPS_SERVER_MANAGER_URL': f'http://manager-{name}.invalid:8000',
                      'FEDOPS_CLIENT_PERFORMANCE_URL': f'http://perf-{name}.invalid:8001',
                      'REGION_NAME': 'ap-northeast-2'}
            apps = Mock()
            with patch.dict(os.environ, values), patch.object(operator, 'load_config'), \
                patch.object(operator.client, 'AppsV1Api', return_value=apps), \
                patch.object(operator, 'create_persistent_volume_and_claim', return_value='test-pvc'), \
                patch.object(operator, 'create_service_for_deployment'), \
                patch.object(operator, 'wait_for_external_ip', return_value='127.0.0.1'), \
                patch.object(operator, 'get_unused_port', return_value=40026), \
                patch.object(operator, 'update_virtual_service', return_value=40026):
                operator.create_scalable_fl_server('test', {}, '', namespace='isolated-test')
            namespace, deployment = apps.create_namespaced_deployment.call_args.args
            self.assertEqual(namespace, 'isolated-test')
            container = deployment.spec.template.spec.containers[0]
            env = {entry.name: entry for entry in container.env}
            self.assertEqual(env['SERVER_MANAGER_URL'].value, values['FEDOPS_SERVER_MANAGER_URL'])
            self.assertEqual(env['FEDOPS_CLIENT_PERFORMANCE_URL'].value, values['FEDOPS_CLIENT_PERFORMANCE_URL'])
            self.assertIsNone(env['ACCESS_KEY_ID'].value)
            self.assertEqual(env['ACCESS_KEY_ID'].value_from.secret_key_ref.name, settings.TASK_S3_SECRET_NAME)
            command = ' '.join(container.args)
            self.assertIn('${SERVER_MANAGER_URL}/FLSe/ScalableServerReady/${TASK_ID}', command)
            self.assertNotIn('ccl.gachon.ac.kr:40019', command)

    def test_pv_and_pvc_consume_the_same_storage_settings(self):
        with patch.dict(os.environ, {'FEDOPS_TASK_STORAGE_BASE_PATH': '/isolated/data',
            'FEDOPS_TASK_STORAGE_CAPACITY': '24Gi', 'FEDOPS_HOSTPATH_STORAGE_NODE': 'test-node'}), \
            patch.object(operator, 'load_config'), patch.object(operator.client, 'CoreV1Api') as factory:
            operator.create_persistent_volume_and_claim('test', 'isolated-test')
            pv = factory.return_value.create_persistent_volume.call_args.args[0]
            pvc = factory.return_value.create_namespaced_persistent_volume_claim.call_args.args[1]
            self.assertEqual(pv.spec.host_path.path, '/isolated/data/test')
            self.assertEqual(pv.spec.capacity['storage'], pvc.spec.resources.requests['storage'])
            self.assertEqual(pv.spec.capacity['storage'], '24Gi')


if __name__ == '__main__': unittest.main()
