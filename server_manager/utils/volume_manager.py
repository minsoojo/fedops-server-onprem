from utils.deployment_config import TARGET_NAMESPACE
from kubernetes import client, config
import logging

def create_persistent_volume_claim(task_id: str, namespace: str = TARGET_NAMESPACE):
    """태스크별 데이터 저장을 위한 PVC 생성"""
    load_config()
    
    pvc_name = f"fl-data-{task_id}"
    
    pvc = client.V1PersistentVolumeClaim(
        metadata=client.V1ObjectMeta(
            name=pvc_name,
            namespace=namespace,
            labels={"task_id": task_id, "type": "fl-data"}
        ),
        spec=client.V1PersistentVolumeClaimSpec(
            access_modes=["ReadWriteOnce"],
            resources=client.V1ResourceRequirements(
                requests={"storage": "10Gi"}
            ),
            storage_class="standard"  # 클러스터에 맞게 조정
        )
    )
    
    v1 = client.CoreV1Api()
    try:
        v1.create_namespaced_persistent_volume_claim(namespace, pvc)
        logging.info(f"Created PVC: {pvc_name}")
        return pvc_name
    except client.exceptions.ApiException as e:
        if e.status == 409:  # Already exists
            logging.info(f"PVC {pvc_name} already exists")
            return pvc_name
        raise e

def load_config():
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config('config.txt')
