from pydantic import BaseModel

import uvicorn
from fastapi import FastAPI, BackgroundTasks, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
import asyncio
from typing import Optional
import json
import logging
import datetime
from typing import List

from utils import server_operator
from utils.test import server_operator_test

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)8.8s] %(message)s",
                    handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)

# [FLTask(FL_task_ID='', Device_mac='', Device_hostname='', Device_online=, Device_training=, last_request_time=5))]
FL_task_list = []

# Create a dictionary to store the statuses of the tasks
# {'656926c2b942e2e83ec6c3e1': {'status': 'Error', 'port': 40026}}
fl_server_status = {}

# Create a dictionary to store the ServerStatus objects for each task
FLSe_dict = {}


class FLTask(BaseModel):
    FL_task_ID: str = ''
    Device_mac: str = ''
    Device_hostname: str = ''
    Device_online: bool = False
    Device_training: bool = False
    last_request_time: str = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    # Device_time: str = ''


# Server Status Object
class ServerStatus(BaseModel):
    S3_bucket: str = 'fl-gl-model'
    Last_GL_Model: str = ''  # 모델 가중치 파일 이름
    FLServer_start: str = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    FLSeReady: bool = False
    GL_Model_V: int = 0  # 모델버전
    Task_status: FLTask = None

    def to_json(self):
        return jsonable_encoder(self)


class StartingTaskData(BaseModel):
    task_id: str
    devices: List[str]
    server_repo_addr: str


class TestStartingTaskData(BaseModel):
    task_id: str
    devices: List[str]
    server_repo_addr: str
    # YAML 설정 및 연합학습 파라미터들 추가
    yaml_config: Optional[str] = None
    data_type: Optional[str] = None
    model_type: Optional[str] = None
    learning_rate: Optional[str] = None
    num_epochs: Optional[str] = None
    batch_size: Optional[str] = None
    num_rounds: Optional[str] = None
    client_per_round: Optional[str] = None
    strategy: Optional[str] = None
    strategy_params: Optional[dict] = None
    xai_enabled: Optional[str] = None
    llm_params: Optional[dict] = None
    dataset_params: Optional[dict] = None

# create App
app = FastAPI()

# create Object
FLSe = ServerStatus()


# Create a function to get or create a ServerStatus object for a task
def get_or_create_FLSe(task_id: str):
    if task_id not in FLSe_dict:
        FLSe_dict[task_id] = ServerStatus()
    return FLSe_dict[task_id]


@app.get("/FLSe/info/{task_id}/{device_mac}")
def read_status(task_id: str, device_mac: str):
    try:
        global FLSe, FL_task_list
        FLSe = get_or_create_FLSe(task_id)

        # Filter the FL_task_list based on task_id and device_mac
        matching_tasks = [task for task in FL_task_list if task.FL_task_ID == task_id and task.Device_mac == device_mac]

        if not matching_tasks:
            server_status_result = {
                "FLServer_start": FLSe.FLServer_start,
                "FLSeReady": FLSe.FLSeReady,
                "GL_Model_V": FLSe.GL_Model_V,
                "Task_Status": None
            }
            logging.info(f'server_status - {server_status_result}')
            FLSe.Task_status = None
        else:
            # Get the first matching task
            matching_task = matching_tasks[0]
            matching_task.last_request_time = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')

            # Convert the matching_task to a JSON-serializable format
            matching_task_json = jsonable_encoder(matching_task)

            server_status_result = {
                "FLServer_start": FLSe.FLServer_start,
                "FLSeReady": FLSe.FLSeReady,
                "GL_Model_V": FLSe.GL_Model_V,
                "Task_Status": matching_task_json
            }

            logging.info(f'server_status - {server_status_result}')

            FLSe.Task_status = matching_task

        return JSONResponse(content={"Server_Status": FLSe.to_json()})
    except Exception as e:
        logging.error(f"Error in read_status: {str(e)}")
        return {"error": str(e)}


def update_or_append_task(new_task):
    global FL_task_list
    found = False
    for task in FL_task_list:
        if task.FL_task_ID == new_task.FL_task_ID and task.Device_mac == new_task.Device_mac:
            task.Device_hostname = new_task.Device_hostname
            task.Device_online = new_task.Device_online
            task.Device_training = new_task.Device_training
            found = True
            break

    if not found:
        FL_task_list.append(new_task)


@app.put("/FLSe/RegisterFLTask")
def register_fl_task(task: FLTask, request: Request):
    client_ip = request.client.host
    global FL_task_list
    update_or_append_task(task)

    logging.info(f"Client IP: {client_ip}")
    logging.info(f"registered_fl_task_list: {task}")
    logging.info(f"registered_fl_task_lists: {FL_task_list}")

    return FL_task_list


@app.get("/FLSe/GetFLTask/{task_id}")
def get_fl_task(task_id: str):
    global FL_task_list
    matching_tasks = []

    # Find the matching FLTask instances
    for task in FL_task_list:
        if task.FL_task_ID == task_id:
            matching_tasks.append(task)

    # Convert the matching FLTask instances to a JSON-compatible list
    tasks_json = jsonable_encoder(matching_tasks)

    # If no matching instances are found, return an error message
    if not tasks_json:
        return {"error": "No matching FLTask found for the provided FL_task_ID"}

    return tasks_json


# {
#   "task_id": "some_task_id",
#   "devices": [
#     "device_mac_1",
#     "device_mac_2",
#     "device_mac_3"
#   ]
# }
@app.post("/FLSe/startTask")
def start_task(task_data: StartingTaskData, background_tasks: BackgroundTasks):
    global fl_server_status

    # Check if the task with the same task_id is already running
    if task_data.task_id in fl_server_status:
        if "Error" in fl_server_status[task_data.task_id]["status"]:
            return {"status": "Error"}
        else:
            return {"status": "already started"}
            
    fl_server_status[task_data.task_id] = {"status": "Initializing"}

    # Start the task and create a background task to manage its status
    background_tasks.add_task(
        server_operator.create_fl_server,
        task_data.task_id,
        fl_server_status,
        task_data.server_repo_addr,
    )

    return {"status": "Task started."}

@app.post("/FLSe/teststartTask")
def start_task(task_data: TestStartingTaskData, background_tasks: BackgroundTasks):
    global fl_server_status

    # Check if the task with the same task_id is already running
    if task_data.task_id in fl_server_status:
        if "Error" in fl_server_status[task_data.task_id]["status"]:
            return {"status": "Error"}
        else:
            return {"status": "already started"}
            
    fl_server_status[task_data.task_id] = {"status": "Initializing"}

    # Log the received FL configuration
    logging.info(f"Starting FL task with ID: {task_data.task_id}")
    logging.info(f"Data type: {task_data.data_type}")
    logging.info(f"Model type: {task_data.model_type}")
    logging.info(f"Strategy: {task_data.strategy}")
    
    # YAML 설정 파일 생성 및 저장
    if task_data.yaml_config:
        try:
            import os
            # 태스크별 디렉토리 생성
            task_dir = f"/tmp/fl_tasks/{task_data.task_id}"
            os.makedirs(task_dir, exist_ok=True)
            
            # YAML 설정 파일 저장
            yaml_file_path = f"{task_dir}/config.yaml"
            with open(yaml_file_path, 'w', encoding='utf-8') as yaml_file:
                yaml_file.write(task_data.yaml_config)
            
            logging.info(f"YAML config saved to: {yaml_file_path}")
            
            # FL 파라미터 로그
            if task_data.strategy_params:
                logging.info(f"Strategy parameters: {task_data.strategy_params}")
            if task_data.llm_params:
                logging.info(f"LLM parameters: {task_data.llm_params}")
            if task_data.dataset_params:
                logging.info(f"Dataset parameters: {task_data.dataset_params}")
                
        except Exception as e:
            logging.error(f"Failed to save YAML config: {str(e)}")
            fl_server_status[task_data.task_id] = {"status": "Error - YAML save failed"}
            return {"status": "Error - YAML save failed"}

    # Start the task and create a background task to manage its status
    background_tasks.add_task(
        server_operator_test.create_fl_server,
        task_data.task_id,
        fl_server_status,
        task_data.server_repo_addr,
        task_data  # 전체 task_data를 전달하여 FL 설정 활용
    )

    return {"status": "Task started.", "yaml_saved": task_data.yaml_config is not None}

@app.post("/FLSe/scalableStartTask")
def start_scalable_task(task_data: TestStartingTaskData, background_tasks: BackgroundTasks):
    """스케일링 가능한 FL 서버를 사용한 태스크 시작"""
    global fl_server_status

    # Check if the task with the same task_id is already running
    if task_data.task_id in fl_server_status:
        if "Error" in fl_server_status[task_data.task_id]["status"]:
            return {"status": "Error"}
        else:
            return {"status": "already started"}
            
    fl_server_status[task_data.task_id] = {"status": "Initializing"}

    # Log the received FL configuration
    logging.info(f"Starting scalable FL task with ID: {task_data.task_id}")
    logging.info(f"Data type: {task_data.data_type}")
    logging.info(f"Model type: {task_data.model_type}")
    logging.info(f"Strategy: {task_data.strategy}")
    
    # YAML 설정 파일 생성 및 저장
    if task_data.yaml_config:
        try:
            import os
            # 태스크별 디렉토리 생성
            task_dir = f"/tmp/fl_tasks/{task_data.task_id}"
            os.makedirs(task_dir, exist_ok=True)
            
            # YAML 설정 파일 저장
            yaml_file_path = f"{task_dir}/config.yaml"
            with open(yaml_file_path, 'w', encoding='utf-8') as yaml_file:
                yaml_file.write(task_data.yaml_config)
            
            logging.info(f"YAML config saved to: {yaml_file_path}")
            
            # FL 파라미터 로그
            if task_data.strategy_params:
                logging.info(f"Strategy parameters: {task_data.strategy_params}")
            if task_data.llm_params:
                logging.info(f"LLM parameters: {task_data.llm_params}")
            if task_data.dataset_params:
                logging.info(f"Dataset parameters: {task_data.dataset_params}")
                
        except Exception as e:
            logging.error(f"Failed to save YAML config: {str(e)}")
            fl_server_status[task_data.task_id] = {"status": "Error - YAML save failed"}
            return {"status": "Error - YAML save failed"}

    # Start the scalable task using scalable_server_operator
    from utils.scalable_server_operator import create_scalable_fl_server
    background_tasks.add_task(
        create_scalable_fl_server,
        task_id=task_data.task_id,
        fl_server_status=fl_server_status,
        server_repo_addr=task_data.server_repo_addr,
        initial_cpu="1",
        initial_memory="2Gi", 
        namespace="fedops",
        task_data=task_data  # 전체 task_data를 전달하여 FL 설정 활용
    )

    return {
        "status": "Scalable task started.", 
        "yaml_saved": task_data.yaml_config is not None,
        "deployment_type": "scalable"
    }


@app.get("/FLSe/status/{task_id}")
def get_fl_server_status(task_id: str):
    global fl_server_status

    logging.info(f"fl-server-status: {fl_server_status}")

    status = fl_server_status.get(task_id)
    if status:
        return {"task_id": task_id, "status": status}
    else:
        return {"error": f"No task with id {task_id} found test test test test"}


@app.get("/FLSe/getPort/{task_id}")
async def get_fl_server_port(task_id: str):
    global fl_server_status

    if task_id in fl_server_status:
        port = fl_server_status[task_id].get("port", None)
        if port:
            return {"task_id": task_id, "port": port}
        else:
            return {"error": f"No port assigned for task_id {task_id}"}
    else:
        return {"error": f"Task ID {task_id} not found"}


@app.put("/FLSe/FLSeUpdate/{task_id}")
def update_status(task_id: str, Se: ServerStatus):
    global fl_server_status, FLSe_dict
    fl_server_status[task_id]["status"] = "FL Server Running"
    
    FLSe = get_or_create_FLSe(task_id)
    FLSe.S3_bucket = Se.S3_bucket if Se.S3_bucket is not None else FLSe.S3_bucket
    FLSe.Last_GL_Model = Se.Last_GL_Model if Se.Last_GL_Model is not None else FLSe.Last_GL_Model
    FLSe.FLServer_start = Se.FLServer_start
    FLSe.FLSeReady = Se.FLSeReady
    FLSe.GL_Model_V = Se.GL_Model_V if Se.GL_Model_V is not None else FLSe.GL_Model_V
    FLSe.Task_status = Se.Task_status
    return {"Server_Status": FLSe}


@app.put("/FLSe/FLRoundFin/{task_id}")
def update_ready(task_id: str, FLSeReady: bool):
    global fl_server_status, FLSe_dict
    fl_server_status[task_id]["status"] = "FL Server Finished"
    
    FLSe = get_or_create_FLSe(task_id)
    FLSe.FLSeReady = FLSeReady
    if FLSeReady==False:
        FLSe.GL_Model_V += 1
    return {"Server_Status": FLSe}


@app.put("/FLSe/FLSeClosed/{task_id}")
def server_closed(task_id: str, FLSeReady: bool):
    global FLSe, FL_task_list, fl_server_status, FLSe_dict
    FLSe = get_or_create_FLSe(task_id)

    # Clear FL_task_list for matching task_id
    FL_task_list = [task for task in FL_task_list if task.FL_task_ID != task_id]

    # Clear FLSe_dict for matching task_id
    # if task_id in FLSe_dict:
    #     del FLSe_dict[task_id]

    print('server closed')
    FLSe.FLSeReady = FLSeReady
    return {"Server_Status": FLSe}

@app.post("/FLSe/server_status")

# ============= 웹 컨트롤을 위한 새로운 API 엔드포인트 =============

class ScaleRequest(BaseModel):
    cpu: str
    memory: str

class FileRequest(BaseModel):
    file_path: str
    content: Optional[str] = None

class CommandRequest(BaseModel):
    command: str

@app.post("/web-control/create-scalable-server/{task_id}")
def create_scalable_server_web(task_id: str, background_tasks: BackgroundTasks, 
                              server_repo_addr: str = ''):
    """웹에서 스케일링 가능한 FL 서버 생성"""
    try:
        from utils.scalable_server_operator import create_scalable_fl_server
        
        background_tasks.add_task(
            create_scalable_fl_server,
            task_id=task_id,
            fl_server_status=fl_server_status,
            server_repo_addr=server_repo_addr,
            initial_cpu="1",
            initial_memory="2Gi"
        )
        
        return {"message": f"Scalable FL server creation started for task {task_id}"}
    except Exception as e:
        logger.error(f"Error creating scalable server: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/web-control/create-scalable-server-with-config/{task_id}")
def create_scalable_server_with_config_web(task_id: str, task_data: TestStartingTaskData, 
                                          background_tasks: BackgroundTasks):
    """FL config를 포함한 스케일링 가능한 FL 서버 생성"""
    try:
        from utils.scalable_server_operator import create_scalable_fl_server
        
        logging.info(f"Creating scalable FL server with config for task: {task_id}")
        logging.info(f"FL Configuration: {task_data.data_type}, {task_data.strategy}")
        
        # YAML 설정 파일 생성 및 저장
        if task_data.yaml_config:
            try:
                import os
                # 태스크별 디렉토리 생성
                task_dir = f"/tmp/fl_tasks/{task_data.task_id}"
                os.makedirs(task_dir, exist_ok=True)
                
                # YAML 설정 파일 저장
                yaml_file_path = f"{task_dir}/config.yaml"
                with open(yaml_file_path, 'w', encoding='utf-8') as yaml_file:
                    yaml_file.write(task_data.yaml_config)
                
                logging.info(f"YAML config saved to: {yaml_file_path}")
                
            except Exception as e:
                logging.error(f"Failed to save YAML config: {str(e)}")
        
        background_tasks.add_task(
            create_scalable_fl_server,
            task_id=task_id,
            fl_server_status=fl_server_status,
            server_repo_addr=task_data.server_repo_addr,
            initial_cpu="1",
            initial_memory="2Gi",
            namespace="fedops",
            task_data=task_data  # FL config 데이터 전달
        )
        
        return {
            "message": f"Scalable FL server with config creation started for task {task_id}",
            "config_applied": True,
            "yaml_saved": task_data.yaml_config is not None
        }
    except Exception as e:
        logger.error(f"Error creating scalable server with config: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/web-control/scale-resources/{task_id}")
def scale_server_resources_web(task_id: str, scale_request: ScaleRequest):
    """웹에서 FL 서버 리소스 스케일링"""
    try:
        from utils.scalable_server_operator import scale_fl_server_resources
        
        success = scale_fl_server_resources(
            task_id=task_id,
            cpu=scale_request.cpu,
            memory=scale_request.memory
        )
        
        if success:
            return {"message": f"Resources scaled for task {task_id}", 
                   "cpu": scale_request.cpu, "memory": scale_request.memory}
        else:
            raise HTTPException(status_code=500, detail="Failed to scale resources")
    except Exception as e:
        logger.error(f"Error scaling resources: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/web-control/pause/{task_id}")
def pause_server_web(task_id: str):
    """웹에서 FL 서버 일시정지 (리소스 해제, 데이터 보존)"""
    try:
        from utils.scalable_server_operator import pause_fl_server
        
        success = pause_fl_server(task_id=task_id)
        
        if success:
            return {"message": f"FL server paused for task {task_id}"}
        else:
            raise HTTPException(status_code=500, detail="Failed to pause server")
    except Exception as e:
        logger.error(f"Error pausing server: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/web-control/resume/{task_id}")
def resume_server_web(task_id: str):
    """웹에서 FL 서버 재개"""
    try:
        from utils.scalable_server_operator import resume_fl_server
        
        success = resume_fl_server(task_id=task_id)
        
        if success:
            return {"message": f"FL server resumed for task {task_id}"}
        else:
            raise HTTPException(status_code=500, detail="Failed to resume server")
    except Exception as e:
        logger.error(f"Error resuming server: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/web-control/status/{task_id}")
def get_server_status_web(task_id: str):
    """웹에서 FL 서버 상태 확인"""
    try:
        from kubernetes import client, config
        
        # Kubernetes 설정 로드
        try:
            config.load_incluster_config()
        except config.ConfigException:
            config.load_kube_config('config.txt')
        
        deployment_name = f"fl-server-deploy-{task_id}"
        namespace = "fedops"
        
        apps_v1 = client.AppsV1Api()
        core_v1 = client.CoreV1Api()
        
        # Deployment 상태 확인
        try:
            deployment = apps_v1.read_namespaced_deployment(deployment_name, namespace)
            deployment_status = {
                "replicas": deployment.status.replicas or 0,
                "ready_replicas": deployment.status.ready_replicas or 0,
                "available_replicas": deployment.status.available_replicas or 0
            }
        except client.exceptions.ApiException as e:
            if e.status == 404:
                deployment_status = {"error": "Deployment not found"}
            else:
                raise e
        
        # Pod 상태 확인
        pods = core_v1.list_namespaced_pod(
            namespace=namespace,
            label_selector=f"task_id={task_id}"
        )
        
        pod_status = []
        for pod in pods.items:
            pod_status.append({
                "name": pod.metadata.name,
                "phase": pod.status.phase,
                "ready": all(condition.status == "True" for condition in pod.status.conditions or [])
            })
        
        # PVC 상태 확인
        pvc_name = f"fl-data-{task_id}"
        try:
            pvc = core_v1.read_namespaced_persistent_volume_claim(pvc_name, namespace)
            pvc_status = {
                "name": pvc.metadata.name,
                "phase": pvc.status.phase,
                "capacity": pvc.status.capacity.get("storage") if pvc.status.capacity else None
            }
        except client.exceptions.ApiException as e:
            if e.status == 404:
                pvc_status = {"error": "PVC not found"}
            else:
                raise e
        
        return {
            "task_id": task_id,
            "deployment": deployment_status,
            "pods": pod_status,
            "pvc": pvc_status,
            "fl_server_status": fl_server_status.get(task_id, {})
        }
        
    except Exception as e:
        logger.error(f"Error getting server status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/web-control/execute-command/{task_id}")
def execute_command_web(task_id: str, command_request: CommandRequest):
    """웹에서 컨테이너 내부 명령 실행"""
    try:
        from kubernetes import client, config
        from kubernetes.stream import stream
        
        # Kubernetes 설정 로드
        try:
            config.load_incluster_config()
        except config.ConfigException:
            config.load_kube_config('config.txt')
        
        namespace = "fedops"
        core_v1 = client.CoreV1Api()
        
        # Pod 찾기
        pods = core_v1.list_namespaced_pod(
            namespace=namespace,
            label_selector=f"task_id={task_id}"
        )
        
        if not pods.items:
            raise HTTPException(status_code=404, detail="No pods found for this task")
        
        pod_name = pods.items[0].metadata.name
        
        # 명령 실행
        exec_command = ['/bin/sh', '-c', command_request.command]
        
        resp = stream(
            core_v1.connect_get_namespaced_pod_exec,
            pod_name,
            namespace,
            command=exec_command,
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False
        )
        
        return {"output": resp, "command": command_request.command}
        
    except Exception as e:
        logger.error(f"Error executing command: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/web-control/logs/{task_id}")
def get_logs_web(task_id: str, lines: int = 100):
    """웹에서 FL 서버 로그 확인"""
    try:
        from kubernetes import client, config
        
        # Kubernetes 설정 로드
        try:
            config.load_incluster_config()
        except config.ConfigException:
            config.load_kube_config('config.txt')
        
        namespace = "fedops"
        core_v1 = client.CoreV1Api()
        
        # Pod 찾기
        pods = core_v1.list_namespaced_pod(
            namespace=namespace,
            label_selector=f"task_id={task_id}"
        )
        
        if not pods.items:
            raise HTTPException(status_code=404, detail="No pods found for this task")
        
        pod_name = pods.items[0].metadata.name
        
        # 로그 가져오기
        logs = core_v1.read_namespaced_pod_log(
            name=pod_name,
            namespace=namespace,
            tail_lines=lines
        )
        
        return {"logs": logs, "pod_name": pod_name, "lines": lines}
        
    except Exception as e:
        logger.error(f"Error getting logs: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/web-control/files/{task_id}")
def list_files_web(task_id: str, path: str = "/app/data"):
    """웹에서 컨테이너 내부 파일 목록 확인"""
    try:
        command_request = CommandRequest(command=f"ls -la {path}")
        result = execute_command_web(task_id, command_request)
        return {"path": path, "files": result["output"]}
    except Exception as e:
        logger.error(f"Error listing files: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/web-control/file-content/{task_id}")
def get_file_content_web(task_id: str, file_path: str):
    """웹에서 파일 내용 확인"""
    try:
        command_request = CommandRequest(command=f"cat {file_path}")
        result = execute_command_web(task_id, command_request)
        return {"file_path": file_path, "content": result["output"]}
    except Exception as e:
        logger.error(f"Error getting file content: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/web-control/save-file/{task_id}")
def save_file_web(task_id: str, file_request: FileRequest):
    """웹에서 파일 저장"""
    try:
        # 파일 내용을 임시로 저장하고 컨테이너에 복사
        escaped_content = file_request.content.replace("'", "'\"'\"'")
        command_request = CommandRequest(
            command=f"echo '{escaped_content}' > {file_request.file_path}"
        )
        result = execute_command_web(task_id, command_request)
        return {"message": f"File saved to {file_request.file_path}"}
    except Exception as e:
        logger.error(f"Error saving file: {e}")
        raise HTTPException(status_code=500, detail=str(e))

async def monitor_task():
    while True:
        current_time = datetime.datetime.today()
        global FL_task_list
        FL_task_list = [task for task in FL_task_list if (current_time - datetime.datetime.strptime(task.last_request_time, '%Y-%m-%d %H:%M:%S')).total_seconds() <= 300]
        await asyncio.sleep(60)  # wait for 60 seconds


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(monitor_task())
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
