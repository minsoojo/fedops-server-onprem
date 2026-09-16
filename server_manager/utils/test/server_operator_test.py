from utils.deployment_config import TARGET_NAMESPACE, VIRTUAL_SERVICE_NAME, ISTIO_GATEWAY, ISTIO_HOST, TASK_IMAGE, TASK_S3_SECRET_NAME, task_connection_env, required_url
from kubernetes import client, config, watch
from kubernetes.client import V1EnvVar, V1EnvVarSource, V1SecretKeySelector
import time
from requests.exceptions import RequestException
import logging
import asyncio
import requests

from utils.network_config import FL_SERVER_PORT_MIN, iter_fl_server_ports

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)8.8s] %(message)s",
                    handlers=[logging.StreamHandler()])

def load_config():
    try:
        # Try loading the in-cluster configuration
        config.load_incluster_config()
        print("Using in-cluster config")
    except config.ConfigException:
        # Fallback to loading the kubeconfig file
        config.load_kube_config('config.txt')
        print("Using kubeconfig file")

    return config


def get_running_tasks(namespace: str = TARGET_NAMESPACE):
    load_config()

    # Initialize the Kubernetes client
    api_instance = client.CoreV1Api()

    # List all pods in the namespace
    pods = api_instance.list_namespaced_pod(namespace)

    running_tasks = []

    # 파드네임 중단점
    for pod in pods.items:
        # Check if the pod is a 'fl-server-job-' pod and if it is running
        # if pod.metadata.name.startswith('fl-server-job-') and pod.status.phase == 'Running':
        if pod.metadata.name.startswith('fl-server-job-') and pod.status.phase != 'Succeeded':
            # The task_id is the third part of the pod name when split by '-'
            task_id = pod.metadata.name.split('-')[3]
            running_tasks.append(task_id)

    return running_tasks


def retry_with_backoff(func, max_retries=3, backoff_factor=0.1):
    retries = 0
    while retries < max_retries:
        try:
            return func()
        except RequestException:
            retries += 1
            sleep_duration = backoff_factor * (2 ** retries)
            time.sleep(sleep_duration)
    raise Exception("Max retries reached, operation failed")

def get_unused_port(service_name: str, namespace: str = TARGET_NAMESPACE):
    load_config()

    api_instance = client.CustomObjectsApi()
    virtual_service_name = VIRTUAL_SERVICE_NAME

    try:
        virtual_service = api_instance.get_namespaced_custom_object(
            group="networking.istio.io",
            version="v1alpha3",
            namespace=namespace,
            plural="virtualservices",
            name=virtual_service_name
        )
    except client.exceptions.ApiException as e:
        if e.status == 404:
            return FL_SERVER_PORT_MIN
        else:
            raise e

    if isinstance(virtual_service["spec"]["tcp"], list):
        for route in virtual_service["spec"]["tcp"]:
            destination_host = route["route"][0]["destination"]["host"]
            port = route["match"][0]["port"]
            if destination_host == f"{service_name}.{namespace}.svc.cluster.local":
                return port  # 일치하는 host 발견 시 해당 포트 반환

    # 일치하는 host가 없는 경우, 사용 가능한 다음 포트 반환
    for port in iter_fl_server_ports():
        if port not in [route["match"][0]["port"] for route in virtual_service["spec"]["tcp"]]:
            return port

    raise Exception("No unused ports available")


def update_virtual_service(task_id: str, service_name: str, port: int, namespace: str, external_ip: str):
    load_config()

    api_instance = client.CustomObjectsApi()

    virtual_service_name = VIRTUAL_SERVICE_NAME
    try:
        # Try to get the existing VirtualService
        virtual_service = api_instance.get_namespaced_custom_object(
            group="networking.istio.io",
            version="v1alpha3",
            namespace=namespace,
            plural="virtualservices",
            name=virtual_service_name
        )
    except client.exceptions.ApiException as e:
        if e.status == 404:
            # If the VirtualService does not exist, create a new one
            virtual_service = {
                "apiVersion": "networking.istio.io/v1alpha3",
                "kind": "VirtualService",
                "metadata": {
                    "name": virtual_service_name,
                    "namespace": namespace
                },
                "spec": {
                    "gateways": [ISTIO_GATEWAY],
                    "hosts": [ISTIO_HOST],
                    # "hosts": ["*"],
                    "tcp": []
                }
            }
        else:
            raise e

    # Add the new route to the VirtualService
    # Create a new route
    new_host = f"{service_name}.{namespace}.svc.cluster.local"
    new_route = {
        "match": [{
            "port": port
        }],
        "route": [
            {
                "destination": {
                    "host": new_host,
                    # "host": f"{external_ip}",
                    "port": {"number": 80}
                }
            }
        ]
    }
    # Check if the route with the same host already exists
    host_exists = False
    if isinstance(virtual_service["spec"]["tcp"], list):
        for route in virtual_service["spec"]["tcp"]:
            logging.info(f"check route: {route}")
            destinations = route["route"]
            for destination in destinations:
                logging.info(f"check destination: {destination}")
                host = destination["destination"]["host"]
                logging.info(f"check host: {host}")
                logging.info(f'new_host: {new_host}')
                if host == new_host:
                    host_exists = True
                    break

        if not host_exists:
            virtual_service["spec"]["tcp"].append(new_route)
    else:
        virtual_service["spec"]["tcp"] = [new_route]

    # Update or create the VirtualService
    try:
        api_instance.patch_namespaced_custom_object(
            group="networking.istio.io",
            version="v1alpha3",
            namespace=namespace,
            plural="virtualservices",
            name=virtual_service_name,
            body=virtual_service,
        )
        print(f"Updated Istio VirtualService for task_id: {task_id}")
    except client.exceptions.ApiException as e:
        if e.status == 404:
            try:
                api_instance.create_namespaced_custom_object(
                    group="networking.istio.io",
                    version="v1alpha3",
                    namespace=namespace,
                    plural="virtualservices",
                    body=virtual_service,
                )
                print(f"Created Istio VirtualService for task_id: {task_id}")
            except client.exceptions.ApiException as e_create:
                raise e_create
        else:
            raise e


def create_fl_server(task_id: str, fl_server_status: dict, server_repo_addr, task_data=None):
    load_config()

    logging.info(f"Start create_fl_server function")
    logging.info(f"Task ID: {task_id}")
    logging.info(f"Server repo address: {server_repo_addr}")
    
    # Log FL configuration if provided
    if task_data:
        logging.info(f"FL Configuration received:")
        logging.info(f"  - Data type: {task_data.data_type}")
        logging.info(f"  - Model type: {task_data.model_type}")
        logging.info(f"  - Strategy: {task_data.strategy}")
        logging.info(f"  - Learning rate: {task_data.learning_rate}")
        logging.info(f"  - Epochs: {task_data.num_epochs}")
        logging.info(f"  - Batch size: {task_data.batch_size}")
        logging.info(f"  - Rounds: {task_data.num_rounds}")
        logging.info(f"  - Clients per round: {task_data.client_per_round}")
        if task_data.yaml_config:
            logging.info(f"  - YAML config provided: {len(task_data.yaml_config)} characters")
    
    # fl_server_status[task_id]["status"] = "Creating"
    fl_server_status[task_id]["status"] = "FL Server Creating"

    # try:
    #     # Notify the client that the pod creation is complete
    #     notify_client(task_id, "creating")
    # except Exception as error:
    #     logging.error(f"send API to BACKEND SERVER ERROR : {error}")

    job_name = "fl-server-job-" + task_id
    pod_name_prefix = "fl-server-"

    # Initialize the Kubernetes client for batch jobs
    api_instance = client.BatchV1Api()
    # logging.info(f"Create Client BatchV1Api: {api_instance}")

    # Initialize the Kubernetes client for custom objects
    custom_api_instance = client.CustomObjectsApi()
    # logging.info(f"Create Client CustomObjectsApi: {custom_api_instance}")


    # Check if a job with the same name already exists
    namespace = TARGET_NAMESPACE
    existing_jobs = api_instance.list_namespaced_job(
        namespace,
        field_selector=f"metadata.name={job_name}"
    )
    
    # logging.info(f"Create exsting_jobs: {existing_jobs}")
    

    if len(existing_jobs.items) > 0:
        # A job with the same name exists
        existing_job = existing_jobs.items[0]
        job_status = existing_job.status.conditions[-1].type if existing_job.status.conditions else None
        if job_status in ['Complete', 'Failed']:
            # If the job is complete or failed, delete it before creating a new one
            logging.info(f"Deleting existing job with name {job_name} because its status is {job_status}.")
            api_instance.delete_namespaced_job(
                name=job_name,
                namespace=namespace,
                body=client.V1DeleteOptions(propagation_policy='Foreground')
            )
            # Wait for the job to be deleted
            w = watch.Watch()
            for event in w.stream(api_instance.list_namespaced_job, namespace=namespace):
                if event['object'].metadata.name == job_name and event['type'] == 'DELETED':
                    logging.info(f"Job {job_name} deleted.")
                    w.stop()
                    break
        else:
            logging.error(f"Job with name {job_name} already exists and is not Complete or Failed. Skipping job creation.")
            return

    if server_repo_addr == '':
        server_repo_addr = 'https://github.com/gachon-CCLab/FedOps-Training-Server.git'

    env_vars = [V1EnvVar(name="REPO_URL", value=server_repo_addr),
                V1EnvVar(name="GIT_TAG", value="main"),
                V1EnvVar(name="ENV", value="init"),
                V1EnvVar(name="TASK_ID", value=task_id),

                # Use existing Kubernetes secrets for environment variables
                V1EnvVar(name="ACCESS_KEY_ID",
                         value_from=V1EnvVarSource(
                             secret_key_ref=V1SecretKeySelector(name=TASK_S3_SECRET_NAME, key='ACCESS_KEY_ID'))),
                V1EnvVar(name="ACCESS_SECRET_KEY",
                         value_from=V1EnvVarSource(
                             secret_key_ref=V1SecretKeySelector(name=TASK_S3_SECRET_NAME, key='ACCESS_SECRET_KEY'))),
                V1EnvVar(name="BUCKET_NAME",
                         value_from=V1EnvVarSource(
                             secret_key_ref=V1SecretKeySelector(name=TASK_S3_SECRET_NAME, key='BUCKET_NAME')))]
    env_vars.extend(V1EnvVar(name=key, value=value) for key, value in task_connection_env().items())

    # Add FL configuration environment variables if task_data is provided
    if task_data:
        # model_type 변환: AI -> Pytorch, LLM -> Huggingface
        converted_model_type = ""
        if task_data.model_type:
            if task_data.model_type.upper() == "AI":
                converted_model_type = "Pytorch"
            elif task_data.model_type.upper() == "LLM":
                converted_model_type = "Huggingface"
            else:
                converted_model_type = task_data.model_type  # 기존 값 유지
        
        fl_env_vars = [
            V1EnvVar(name="FL_DATA_TYPE", value=task_data.data_type or ""),
            V1EnvVar(name="FL_MODEL_TYPE", value=converted_model_type),
            V1EnvVar(name="FL_LEARNING_RATE", value=task_data.learning_rate or ""),
            V1EnvVar(name="FL_NUM_EPOCHS", value=task_data.num_epochs or ""),
            V1EnvVar(name="FL_BATCH_SIZE", value=task_data.batch_size or ""),
            V1EnvVar(name="FL_NUM_ROUNDS", value=task_data.num_rounds or ""),
            V1EnvVar(name="FL_CLIENT_PER_ROUND", value=task_data.client_per_round or ""),
            V1EnvVar(name="FL_STRATEGY", value=task_data.strategy or ""),
            V1EnvVar(name="FL_XAI_ENABLED", value=task_data.xai_enabled or ""),
        ]
        
        logging.info(f"Model type conversion: {task_data.model_type} -> {converted_model_type}")
        
        # Add strategy parameters as JSON string
        if task_data.strategy_params:
            import json
            fl_env_vars.append(V1EnvVar(name="FL_STRATEGY_PARAMS", value=json.dumps(task_data.strategy_params)))
        
        # Add LLM parameters as JSON string
        if task_data.llm_params:
            import json
            fl_env_vars.append(V1EnvVar(name="FL_LLM_PARAMS", value=json.dumps(task_data.llm_params)))
        
        # Add dataset parameters as JSON string
        if task_data.dataset_params:
            import json
            fl_env_vars.append(V1EnvVar(name="FL_DATASET_PARAMS", value=json.dumps(task_data.dataset_params)))
        
        # Add YAML config as environment variable
        if task_data.yaml_config:
            fl_env_vars.append(V1EnvVar(name="FL_YAML_CONFIG", value=task_data.yaml_config))
            fl_env_vars.append(V1EnvVar(name="FL_YAML_CONFIG_PATH", value="/app/conf/config.yaml"))
            
        env_vars.extend(fl_env_vars)
        logging.info(f"Added {len(fl_env_vars)} FL environment variables")

    job = client.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=client.V1ObjectMeta(
            name=job_name
        ),
        spec=client.V1JobSpec(
            template=client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(
                    generate_name=pod_name_prefix,
                    labels={"run": "fl-server", "task_id": task_id}
                ),
                spec=client.V1PodSpec(
                    containers=[
                        client.V1Container(
                            name="fl-server",
                            # image="docker.io/hoo0681/gitclone_python:0.1",
                            # image="docker.io/tpah20/mytorch-amd64:latest",
                            image=TASK_IMAGE,
                            ports=[
                                client.V1ContainerPort(container_port=8080)
                            ],
                            command=["/bin/sh", "-c"],
                            args=[
                                "echo '=== Starting FL Server Setup ===' && "
                                "echo 'Current directory:' && pwd && "
                                "echo 'Environment variables:' && env | grep FL_ && "
                                "echo 'FL_MODEL_TYPE: '$FL_MODEL_TYPE && "
                                "echo 'Cloning FL Server repository...' && "
                                "git clone -b ${GIT_TAG} ${REPO_URL} /app && "
                                "echo 'FL Server repository cloned successfully' && "
                                "echo 'Listing /app after clone:' && ls -la /app && "
                                
                                # FL_MODEL_TYPE에 따른 추가 코드 준비
                                "if [ \"$FL_MODEL_TYPE\" = \"Pytorch\" ]; then "
                                    "echo 'Setting up Pytorch (AI) FL examples...' && "
                                    "git clone https://github.com/gachon-CCLab/FedOps.git /tmp/fedops && "
                                    "mkdir -p /app/examples && "
                                    "cp -r /tmp/fedops/silo/examples/torch/MNIST /app/examples && "
                                    "rm -rf /tmp/fedops && "
                                    "echo 'Pytorch FL examples setup complete'; "
                                "elif [ \"$FL_MODEL_TYPE\" = \"Huggingface\" ]; then "
                                    "echo 'Setting up Huggingface (LLM) FL examples...' && "
                                    "git clone https://github.com/gachon-CCLab/FedOps.git /tmp/fedops && "
                                    "mkdir -p /app/examples && "
                                    "cp -r /tmp/fedops/llm/usecase/finetune /app/examples && "
                                    "rm -rf /tmp/fedops && "
                                    "echo 'Huggingface FL examples setup complete'; "
                                "else "
                                    "echo 'Unknown or empty FL_MODEL_TYPE: '$FL_MODEL_TYPE && "
                                    "echo 'Skipping examples setup...'; "
                                "fi; "
                                
                                "if [ ! -z \"$FL_YAML_CONFIG\" ]; then echo 'Creating config directory and file...' && mkdir -p /app/conf && echo \"$FL_YAML_CONFIG\" > /app/conf/config.yaml && echo 'Config file created'; fi; "
                                "echo 'Installing requirements...' && "
                                "if [ -f /app/requirements.txt ]; then python3 -m pip install -r /app/requirements.txt && echo 'Requirements installed successfully'; else echo 'ERROR: requirements.txt not found'; fi; "
                                "echo 'Starting FL Server...' && "
                                "if [ -f /app/server_main.py ]; then python3 /app/server_main.py; else echo 'ERROR: server_main.py not found' && echo 'Available files in /app:' && ls -la /app && sleep 3600; fi;"
                            ],
                            env=env_vars,
                            resources=client.V1ResourceRequirements(
                                requests={"cpu": "10", "memory": "10Gi"},
                                limits={"cpu": "10", "memory": "10Gi"}
                            )
                        )
                    ],
                    restart_policy="OnFailure"
                )
            ),
            backoff_limit=1
        )
    )

    api_instance = client.BatchV1Api()
    namespace = TARGET_NAMESPACE

    logging.info("🚀 Creating Kubernetes Job for FL Server...")
    logging.info(f"📝 Job Name: {job_name}")
    logging.info(f"📦 Container Image: docker.io/tpah20/torch_cpu_amd:latest")
    logging.info(f"🌐 Namespace: {namespace}")
    
    api_response = api_instance.create_namespaced_job(namespace, job)
    logging.info("✅ Kubernetes Job created successfully!")
    logging.info(f"📊 Job Status: {str(api_response.status)}")
    
    # get_pod_logs(namespace, job_name + "-", container_name=None)

    # Update the status in the shared dictionary
    # 서버 파드 생성이후 fl_server코드에서 api를 전송 해야 한다.
    fl_server_status[task_id]["status"] = "Created"
    logging.info(f"🔄 FL Server status updated to: Created")

    # Create a service for the job's pod
    service_name = "fl-server-service-" + task_id
    
    logging.info("🌐 Creating LoadBalancer Service for FL Server...")
    logging.info(f"🏷️  Service Name: {service_name}")

    service = client.V1Service(
        api_version="v1",
        kind="Service",
        metadata=client.V1ObjectMeta(name=service_name),
        spec=client.V1ServiceSpec(
            selector={"run": "fl-server", "task_id": task_id},
            ports=[client.V1ServicePort(port=80, target_port=8080)],
            type="LoadBalancer",  # Change the service type to LoadBalancer
        ),
    )

    core_v1_api = client.CoreV1Api()

    # Check if the service already exists
    try:
        existing_service = core_v1_api.read_namespaced_service(namespace=namespace, name=service_name)
    except client.exceptions.ApiException as e:
        if e.status == 404:
            existing_service = None
        else:
            raise e

    if existing_service:
        # Update the existing service
        service.metadata.resource_version = existing_service.metadata.resource_version
        core_v1_api.replace_namespaced_service(name=service_name, namespace=namespace, body=service)
        logging.info(f"🔄 Updated existing service: {service_name}")
    else:
        # Create a new service
        core_v1_api.create_namespaced_service(namespace=namespace, body=service)
        logging.info(f"✅ Created new LoadBalancer service: {service_name}")

    # Wait until the load balancer has assigned an external IP to the service
    logging.info("⏳ Waiting for LoadBalancer to assign external IP...")
    while True:
        service = core_v1_api.read_namespaced_service(namespace=namespace, name=service_name)
        if service.status.load_balancer.ingress:
            external_ip = service.status.load_balancer.ingress[0].ip
            logging.info(f"🌍 External IP assigned: {external_ip}")
            break
        else:
            logging.info("⏳ Still waiting for external IP assignment...")
            time.sleep(1)  # Wait for 1 seconds before checking again

    port = get_unused_port(service_name=service_name)
    fl_server_status[task_id]["port"] = port
    logging.info(f"🔌 Assigned port: {port}")

    update_virtual_service(task_id, service_name, port, namespace, external_ip)
    logging.info(f"🔗 Istio VirtualService updated for external access")

    # Start watching for the job status
    w = watch.Watch()
    logging.info("👀 Starting Job status monitoring...")

    try:
        for event in w.stream(api_instance.list_namespaced_job, namespace=namespace):
            current_job = event['object']
            current_job_name = current_job.metadata.name

            if current_job_name == job_name:
                # Save the generated pod name in fl_server_status
                # pod_name = current_job.spec.template.metadata.labels.job-name
                # pod_name = job_name
                # fl_server_status[task_id]["pod_name"] = pod_name

                if current_job.status.succeeded == 1:
                    logging.info("🎉 FL Server Job completed successfully!")
                    fl_server_status[task_id]["status"] = "Finished"
                    
                    # Notify the client that the pod creation is complete
                    # notify_client(task_id, "Finish")
                    
                    w.stop()
                elif current_job.status.failed:
                    logging.info("❌ FL Server Job failed!")
                    fl_server_status[task_id]["status"] = "Failed"
                    w.stop()
                elif current_job.status.active:
                    logging.info("🏃 FL Server Job is running...")
                    # fl_server_status[task_id]["status"] = "Running"
                else:
                    logging.info("❓ FL Server Job status unknown")
                    fl_server_status[task_id]["status"] = "Unknown"

                # When the job has completed or failed, delete the job
                if current_job.status.succeeded == 1 or current_job.status.failed:
                    logging.info("🧹 Starting cleanup process...")
                    # api_instance.delete_namespaced_job(
                    #     name=job_name,
                    #     namespace=namespace,
                    #     body=client.V1DeleteOptions(propagation_policy='Foreground')
                    # )
                    # Notify the client that the pod creation is complete
                    # notify_client(task_id, "not_start")

                    # Delete the corresponding service
                    logging.info("🗑️  Deleting LoadBalancer service...")
                    core_v1_api.delete_namespaced_service(
                        name=service_name,
                        namespace=namespace,
                        body=client.V1DeleteOptions()
                    )

                    # Remove the route from the VirtualService
                    logging.info("🔗 Removing route from Istio VirtualService...")
                    virtual_service = custom_api_instance.get_namespaced_custom_object(
                        group="networking.istio.io",
                        version="v1alpha3",
                        namespace=namespace,
                        plural="virtualservices",
                        name=VIRTUAL_SERVICE_NAME
                    )
                    # 삭제할 host와 port 정의
                    delete_host = f"{service_name}.{namespace}.svc.cluster.local"
                    delete_port = port  # port는 삭제하고자 하는 라우트의 포트

                    # 완료된 host,port외에 기존에 존재하던 주소들
                    existing_routes = [
                        route for route in virtual_service["spec"]["tcp"]
                        if not (route["route"][0]["destination"]["host"] == delete_host and 
                                route["match"][0]["port"] == delete_port)
                    ]
                    logging.info(f"existing_routes: {existing_routes}")
                    # 존재하던 것이 있으면 존재하는 것만 Patch(수정)
                    if existing_routes:
                        virtual_service["spec"]["tcp"] = existing_routes
                        logging.info(f"Removing virtual_service: {virtual_service}")
                        custom_api_instance.patch_namespaced_custom_object(
                            group="networking.istio.io",
                            version="v1alpha3",
                            namespace=namespace,
                            plural="virtualservices",
                            name=VIRTUAL_SERVICE_NAME,
                            body=virtual_service,
                        )
                    else: # 비어있을 경우 virtual_service 삭제
                        custom_api_instance.delete_namespaced_custom_object(
                            group="networking.istio.io",
                            version="v1alpha3",
                            namespace=namespace,
                            plural="virtualservices",
                            name=VIRTUAL_SERVICE_NAME,
                        )
                    break
        # Clear fl_server_status for matching task_id
        if task_id in fl_server_status:
            del fl_server_status[task_id]
            logging.info(f"🧹 Cleaned up FL server status for task: {task_id}")
    except Exception as e:
        logging.error(f"❌ Error while monitoring job status: {e}")
        fl_server_status[task_id]["status"] = "Error"
    finally:
        logging.info("🏁 FL Server creation process completed")
        w.stop()

def web_backend_server():
    return required_url("WEB_BACKEND_SERVER")

def notify_client(task_id, status):
    url = f'{web_backend_server()}/fedops/api/tasks/notify'  # 서버에 데이터를 전송할 엔드포인트

    data = {
        'task_id': task_id,
        'status': status
    }

    try:
        response = requests.post(url, json=data)
        logging.info(f"send API to BACKEND SERVER in message : {status}")
        logging.info(f"BACKEND RESPONSE is : {response}")
        if not response.ok:
            logging.error(f'Failed to notify server. Status: {response.status_code}')

    except Exception as error:
        logging.error('Error notifying server:', error)

def get_pod_logs(namespace, pod_name, container_name=None):
    # Kubernetes 클러스터 설정 로드
    config.load_kube_config()

    # CoreV1Api 객체 생성
    v1 = client.CoreV1Api()

    url = f'{web_backend_server()}/fedops/api/tasks/logs'  # 서버에 데이터를 전송할 엔드포인트
    try:
        # 파드 로그 가져오기
        if container_name:
            response = v1.read_namespaced_pod_log(pod_name=pod_name, namespace=namespace, container=container_name)
        else:
            response = v1.read_namespaced_pod_log(pod_name=pod_name, namespace=namespace)

        return  requests.post(url, json=response)
    except Exception as e:
        return f"Error retrieving logs: {str(e)}"
