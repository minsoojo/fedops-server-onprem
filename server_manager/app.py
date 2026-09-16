from utils.deployment_config import TARGET_NAMESPACE, VIRTUAL_SERVICE_NAME, ISTIO_GATEWAY, ISTIO_HOST, TASK_IMAGE, TASK_S3_SECRET_NAME, task_connection_env, required_url
#app.py

from pydantic import BaseModel, Field, validator

import uvicorn
from fastapi import FastAPI, BackgroundTasks, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
import asyncio
from typing import Literal, Optional
import json
import logging
import datetime
from typing import List, Dict, Any
import contextlib

from utils import server_operator
from utils import campaign_lifecycle
from utils.test import server_operator_test
from utils.runtime_reconciler import (
    merge_runtime_status,
    recover_runtime_status,
    reconcile_runtime_statuses,
)

from fastapi.responses import StreamingResponse

import asyncio, json, logging, os, posixpath
from kubernetes import client, config
from kubernetes.config.config_exception import ConfigException
from kubernetes.stream import stream


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


# Create Cluster map key = (task_id, mac) -> cluster_id
CLUSTER_MAP = {}


class FLTask(BaseModel):
    FL_task_ID: str = ''
    Device_mac: str = ''
    Device_hostname: str = ''
    Device_online: bool = False
    Device_training: bool = False
    last_request_time: str = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    # Device_time: str = ''
    clusterId: Optional[int] = Field(default=None, alias="cluster_id")# ✨
    class Config:# ✨
        allow_population_by_field_name = True# ✨

# Server Status Object
class ServerStatus(BaseModel):
    S3_bucket: str = 'fl-gl-model'
    Last_GL_Model: str = ''  # 모델 가중치 파일 이름
    FLServer_start: str = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    FLSeReady: bool = False
    GL_Model_V: int = 0  # 모델버전
    Task_status: FLTask = None
    # FedOps 1.3 Campaign Run metadata. Legacy clients omit these fields and
    # retain the original version-increment behaviour.
    Campaign_run_id: Optional[str] = None
    Base_GL_Model_V: Optional[int] = None
    Target_GL_Model_V: Optional[int] = None
    Campaign_config: Optional[Dict[str, Any]] = None
    Campaign_started_at: Optional[str] = None
    Campaign_ended_at: Optional[str] = None
    Campaign_status: Optional[str] = None

    def to_json(self):
        return jsonable_encoder(self)

# ✨ 추가: 클러스터 업서트 요청 바디
class ClusterAssign(BaseModel):  # ✨
    client_mac: str              # ✨
    cluster_id: Optional[int] = None  # ✨ (노이즈/미지정 시 None)


class StartingTaskData(BaseModel):
    task_id: str
    devices: List[str]
    server_repo_addr: str


class RuntimeRelease(BaseModel):
    release_id: str
    archive_url: str
    archive_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    model_url: str
    model_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    model_format: str = "safetensors"
    fedops_version: str
    source_revision: str = Field(pattern=r"^[a-f0-9]{40}$")


class CampaignStrategy(BaseModel):
    name: str
    parameters: Dict[str, float] = Field(default_factory=dict)

    @validator("parameters")
    def validate_parameters(cls, value: Dict[str, float]) -> Dict[str, float]:
        allowed = {"fraction_fit", "fraction_evaluate"}
        unknown = sorted(set(value) - allowed)
        if unknown:
            raise ValueError(f"Unsupported Campaign parameters: {', '.join(unknown)}")
        if any(parameter < 0 or parameter > 1 for parameter in value.values()):
            raise ValueError("Campaign fractions must be between 0 and 1")
        return value


class ServerEvaluationConfig(BaseModel):
    enabled: bool
    dataPath: Optional[str] = None

    @validator("enabled", pre=True)
    def strict_enabled(cls, value):
        if type(value) is not bool:
            raise ValueError("Server Validation enabled must be boolean")
        return value

    @validator("dataPath", always=True)
    def safe_data_path(cls, value, values):
        if not values.get("enabled"):
            return None
        if (not isinstance(value, str) or not value or value != value.strip()
                or "\\" in value or any(ord(c) < 32 for c in value)
                or any(part in ("", ".", "..") for part in value.split("/"))):
            raise ValueError("Server Validation needs a relative directory without traversal")
        return value


class CampaignConfig(BaseModel):
    schemaVersion: Literal[1] = 1
    rounds: int = Field(ge=1)
    clientsPerRound: int = Field(ge=1)
    strategy: CampaignStrategy
    serverEvaluation: Optional[ServerEvaluationConfig] = None


class BeginCampaignRequest(BaseModel):
    runId: str = Field(min_length=8, max_length=128)
    releaseId: str = Field(min_length=1, max_length=128)
    baseGlobalModelVersion: int = Field(ge=0)
    targetGlobalModelVersion: int = Field(ge=1)
    campaign: CampaignConfig


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
    sba_fl_target: Optional[str] = None
    # Omitted callers are existing FedOps 1.2/legacy clients.
    runtime_contract: Literal['legacy-v1', 'federated-task-v2', 'federated-task-v3'] = 'legacy-v1'
    runtime_release: Optional[RuntimeRelease] = None
    campaign_config: Optional[CampaignConfig] = None

# create App
app = FastAPI()


@app.get("/healthz", include_in_schema=False)
def healthz():
    return {"status": "ok"}


# create Object
FLSe = ServerStatus()


# Create a function to get or create a ServerStatus object for a task
def get_or_create_FLSe(task_id: str):
    if task_id not in FLSe_dict:
        FLSe_dict[task_id] = ServerStatus()
    return FLSe_dict[task_id]


def ensure_runtime_status(task_id: str, refresh_allocation: bool = False):
    """Lazily restore allocation metadata after a Server Manager restart."""
    if task_id in fl_server_status and not refresh_allocation:
        return fl_server_status[task_id]
    try:
        recovered = recover_runtime_status(task_id)
    except Exception:
        logger.exception("Failed to recover runtime metadata for task %s", task_id)
        return fl_server_status.get(task_id)
    if recovered:
        current = fl_server_status.get(task_id)
        fl_server_status[task_id] = (
            merge_runtime_status(current, recovered) if current else recovered
        )
        logger.info(
            "Recovered runtime metadata for task %s (port=%s, service=%s)",
            task_id,
            recovered.get("port"),
            recovered.get("service_name"),
        )
    return fl_server_status.get(task_id)


# @app.get("/FLSe/info/{task_id}/{device_mac}")
# def read_status(task_id: str, device_mac: str):
#     try:
#         global FLSe, FL_task_list
#         FLSe = get_or_create_FLSe(task_id)

#         # Filter the FL_task_list based on task_id and device_mac
#         matching_tasks = [task for task in FL_task_list if task.FL_task_ID == task_id and task.Device_mac == device_mac]

#         if not matching_tasks:
#             server_status_result = {
#                 "FLServer_start": FLSe.FLServer_start,
#                 "FLSeReady": FLSe.FLSeReady,
#                 "GL_Model_V": FLSe.GL_Model_V,
#                 "Task_Status": None
#             }
#             logging.info(f'server_status - {server_status_result}')
#             FLSe.Task_status = None
#         else:
#             # Get the first matching task
#             matching_task = matching_tasks[0]
#             matching_task.last_request_time = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')

#             # Convert the matching_task to a JSON-serializable format
#             matching_task_json = jsonable_encoder(matching_task)

#             server_status_result = {
#                 "FLServer_start": FLSe.FLServer_start,
#                 "FLSeReady": FLSe.FLSeReady,
#                 "GL_Model_V": FLSe.GL_Model_V,
#                 "Task_Status": matching_task_json
#             }

#             logging.info(f'server_status - {server_status_result}')

#             FLSe.Task_status = matching_task

#         return JSONResponse(content={"Server_Status": FLSe.to_json()})
#     except Exception as e:
#         logging.error(f"Error in read_status: {str(e)}")
#         return {"error": str(e)}

@app.get("/FLSe/info/{task_id}/{device_mac}")
def read_status(task_id: str, device_mac: str):
    try:
        global FLSe, FL_task_list, CLUSTER_MAP
        FLSe = get_or_create_FLSe(task_id)

        matching_tasks = [t for t in FL_task_list
                          if t.FL_task_ID == task_id and t.Device_mac == device_mac]

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
            m = matching_tasks[0]
            m.last_request_time = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')

            # clusterId가 비어있다면 영속 맵으로 보충
            if m.clusterId is None:
                m.clusterId = CLUSTER_MAP.get((task_id, device_mac))

            matching_task_json = jsonable_encoder(m)  # 응답 형식 변화 없음

            server_status_result = {
                "FLServer_start": FLSe.FLServer_start,
                "FLSeReady": FLSe.FLSeReady,
                "GL_Model_V": FLSe.GL_Model_V,
                "Task_Status": matching_task_json
            }
            logging.info(f'server_status - {server_status_result}')
            FLSe.Task_status = m

        return JSONResponse(content={"Server_Status": FLSe.to_json()})
    except Exception as e:
        logging.error(f"Error in read_status: {str(e)}")
        return {"error": str(e)}



def update_or_append_task(new_task):
    global FL_task_list, CLUSTER_MAP
    now = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    for t in FL_task_list:
        if t.FL_task_ID == new_task.FL_task_ID and t.Device_mac == new_task.Device_mac:
            t.Device_hostname = new_task.Device_hostname
            t.Device_online = new_task.Device_online
            t.Device_training = new_task.Device_training
            t.last_request_time = now
            if new_task.clusterId is not None:
                t.clusterId = new_task.clusterId
            elif t.clusterId is None:
                t.clusterId = CLUSTER_MAP.get((t.FL_task_ID, t.Device_mac))   # 추가
            return
    # 신규
    new_task.last_request_time = now
    if new_task.clusterId is None:
        new_task.clusterId = CLUSTER_MAP.get((new_task.FL_task_ID, new_task.Device_mac))  # 추가
    FL_task_list.append(new_task)


@app.put("/FLSe/RegisterFLTask")
def register_fl_task(task: FLTask, request: Request):
    client_ip = request.client.host
    global FL_task_list
    update_or_append_task(task)

    logging.info(f"Client IP: {client_ip}")
    logging.info(f"registered_fl_task_list: {task}")
    logging.info(f"registered_fl_task_lists: {FL_task_list}")
    # 👇 클러스터 ID 로깅 (None이면 아직 미지정)
    logging.info(f"clusterId for {task.Device_mac}: {task.clusterId}")

    return FL_task_list

# @app.put("/FLSe/RegisterFLTask")
# def register_fl_task(task: FLTask, request: Request):
#     global FL_task_list, CLUSTER_MAP
#     client_ip = request.client.host

#     # 기존 업데이트/추가 로직 그대로 사용
#     update_or_append_task(task)

#     # 저장된 최신 상태를 찾아서 로그
#     saved = next(
#         (t for t in FL_task_list
#          if t.FL_task_ID == task.FL_task_ID and t.Device_mac == task.Device_mac),
#         None
#     )

#     # ★ clusterId 비어있으면 CLUSTER_MAP로 보충 (기존 FL엔 영향 없음)
#     if saved and (saved.clusterId is None):
#         saved.clusterId = CLUSTER_MAP.get((saved.FL_task_ID, saved.Device_mac))

#     logging.info(f"Client IP: {client_ip}")
#     logging.info(f"registered_fl_task_list(saved): {saved}")
#     logging.info(f"registered_fl_task_lists: {FL_task_list}")
#     logging.info(f"clusterId(saved) for {task.Device_mac}: {saved.clusterId if saved else None}")

#     # 기존 리턴 그대로 (리스트 객체)
#     return FL_task_list



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

    # (주의) clusterId는 FLTask에 포함되어 자동 직렬화됨
    return tasks_json


# @app.get("/FLSe/GetFLTask/{task_id}")
# def get_fl_task(task_id: str):
#     global FL_task_list, CLUSTER_MAP

#     # 해당 task_id만 추출 (기존 로직 유지)
#     matching_tasks = [t for t in FL_task_list if t.FL_task_ID == task_id]

#     if not matching_tasks:
#         return {"error": "No matching FLTask found for the provided FL_task_ID"}

#     # ★ 각 항목에 대해 clusterId 비어있으면 CLUSTER_MAP로 보충
#     for t in matching_tasks:
#         if t.clusterId is None:
#             t.clusterId = CLUSTER_MAP.get((t.FL_task_ID, t.Device_mac))

#     # (주의) FLTask는 pydantic 모델이라 clusterId가 자동 직렬화됨 (alias: cluster_id 유지)
#     return jsonable_encoder(matching_tasks)


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

# 추가 특징 이모지로 표시

# ✨ 추가: 특정 task_id에 대해 (client_mac -> cluster_id) 업서트
# @app.put("/FLSe/cluster/{task_id}")  # ✨
# def set_cluster(task_id: str, payload: ClusterAssign):  # ✨
#     global FL_task_list, CLUSTER_MAP  # ✨
#     # 해당 task_id & mac 찾으면 갱신, 없으면 최소 정보로 신규 추가  # ✨
#     updated = False  # ✨
#     for t in FL_task_list:  # ✨
#         if t.FL_task_ID == task_id and t.Device_mac == payload.client_mac:  # ✨
#             t.clusterId = payload.cluster_id  # ✨
#             updated = True  # ✨
#             break  # ✨
#     if not updated:  # ✨
#         FL_task_list.append(  # ✨
#             FLTask(  # ✨
#                 FL_task_ID=task_id,  # ✨
#                 Device_mac=payload.client_mac,  # ✨
#                 Device_hostname='',  # ✨
#                 Device_online=False,  # ✨
#                 Device_training=False,  # ✨
#                 clusterId=payload.cluster_id,  # ✨
#             )  # ✨
#         )  # ✨
#     logging.info(f"[CLUSTER-UPsert] task={task_id}, mac={payload.client_mac}, cluster_id={payload.cluster_id}")  # ✨
#     return {"ok": True}  # ✨

@app.put("/FLSe/cluster/{task_id}")
def set_cluster(task_id: str, payload: ClusterAssign):
    global FL_task_list, CLUSTER_MAP
    # 1) 영속 맵에 먼저 업서트 (핵심)
    CLUSTER_MAP[(task_id, payload.client_mac)] = payload.cluster_id

    # 2) 런타임 리스트에도 반영(있으면 갱신, 없으면 최소정보로 추가)
    for t in FL_task_list:
        if t.FL_task_ID == task_id and t.Device_mac == payload.client_mac:
            t.clusterId = payload.cluster_id
            t.last_request_time = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
            break

    # if not updated:
    #     FL_task_list.append(
    #         FLTask(
    #             FL_task_ID=task_id,
    #             Device_mac=payload.client_mac,
    #             Device_hostname='',
    #             Device_online=False,
    #             Device_training=False,
    #             clusterId=payload.cluster_id,
    #             last_request_time=now                   
    #         )
    #     )

    logging.info(
        f"[CLUSTER-UPsert] task={task_id}, mac={payload.client_mac}, cluster_id={payload.cluster_id}"
    )
    return {"ok": True, "cluster_id": payload.cluster_id}


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
    logging.info(f"SBA-FL target: {task_data.sba_fl_target}")
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
    """스케일링 가능한 FL 태스크 초기화 (서버 생성은 별도)"""
    global fl_server_status

    # Check if the task with the same task_id is already running
    if task_data.task_id in fl_server_status:
        if "Error" in fl_server_status[task_data.task_id]["status"]:
            return {"status": "Error"}
        else:
            return {"status": "already started"}
            
    # Task 초기화만 수행 (서버 생성하지 않음)
    fl_server_status[task_data.task_id] = {"status": "Task Initialized - Server Not Created"}

    # Log the received FL configuration
    logging.info(f"Initializing scalable FL task with ID: {task_data.task_id}")
    logging.info(f"Data type: {task_data.data_type}")
    logging.info(f"Model type: {task_data.model_type}")
    logging.info(f"Strategy: {task_data.strategy}")
    
    # YAML 설정 파일 생성 및 저장 (나중에 서버 생성 시 사용)
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

    # Task 설정을 저장하여 나중에 서버 생성 시 사용
    fl_server_status[task_data.task_id].update({
        "task_data": task_data.__dict__,  # Task 데이터 저장
        "yaml_saved": task_data.yaml_config is not None
    })

    return {
        "status": "Task initialized. Use 'Create Server' button to start FL server.", 
        "yaml_saved": task_data.yaml_config is not None,
        "deployment_type": "scalable"
    }


@app.get("/FLSe/status/{task_id}")
def get_fl_server_status(task_id: str):
    global fl_server_status

    logging.info(f"fl-server-status: {fl_server_status}")

    status = ensure_runtime_status(task_id)
    if status:
        return {"task_id": task_id, "status": status}
    else:
        return {"error": f"No task with id {task_id} found test test test test"}


@app.get("/FLSe/getPort/{task_id}")
async def get_fl_server_port(task_id: str):
    global fl_server_status

    # Resolve by Task ID and refresh the address from the authoritative
    # Kubernetes Service + VirtualService before returning it to a client.
    status_info = ensure_runtime_status(task_id, refresh_allocation=True)
    if status_info:
        port = status_info.get("port", None)
        external_ip = status_info.get("external_ip", None)
        if port:
            response = {"task_id": task_id, "port": port}
            if external_ip:
                response["external_ip"] = external_ip
            return response
        else:
            return {"error": f"No port assigned for task_id {task_id}"}
    else:
        return {"error": f"Task ID {task_id} not found"}

@app.get("/FLSe/getConnectionInfo/{task_id}")
async def get_fl_server_connection_info(task_id: str):
    """FL 서버 연결 정보 반환 (포트, 외부 IP 포함)"""
    global fl_server_status

    status_info = ensure_runtime_status(task_id, refresh_allocation=True)
    if status_info:
        connection_info = {
            "task_id": task_id,
            "status": status_info.get("status", "Unknown"),
            "port": status_info.get("port", None),
            "external_ip": status_info.get("external_ip", None)
        }
        
        # Deployment 타입 정보 추가
        if "deployment" in status_info:
            connection_info["server_type"] = "scalable"
            connection_info["deployment"] = status_info["deployment"]
            connection_info["service_name"] = status_info.get("service_name", None)
        else:
            connection_info["server_type"] = "job"
            
        # 클라이언트가 연결할 주소 정보 추가
        if connection_info["external_ip"] and connection_info["port"]:
            connection_info["server_address"] = f"{connection_info['external_ip']}:{connection_info['port']}"
            
        return connection_info
    else:
        return {"error": f"Task ID {task_id} not found"}


@app.post("/FLSe/BeginCampaign/{task_id}")
def begin_campaign(task_id: str, request: BeginCampaignRequest):
    """Start one isolated FedOps 1.3 Campaign without changing legacy Tasks.

    The Task ID remains the transport identity. This endpoint only resets the
    per-Campaign control state associated with that Task ID.
    """
    global fl_server_status, FLSe_dict, FL_task_list
    FLSe = get_or_create_FLSe(task_id)
    runtime = ensure_runtime_status(task_id) or {}
    try:
        campaign_lifecycle.begin(FLSe, runtime, request, FL_task_list, task_id)
    except ValueError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    fl_server_status[task_id] = runtime
    return {
        "task_id": task_id,
        "campaign_run_id": request.runId,
        "server_status": FLSe.to_json(),
    }


@app.put("/FLSe/FLSeUpdate/{task_id}")
def update_status(task_id: str, Se: ServerStatus):
    global fl_server_status, FLSe_dict
    FLSe = get_or_create_FLSe(task_id)
    try:
        campaign_lifecycle.assert_campaign(FLSe, Se.Campaign_run_id)
    except ValueError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    if FLSe.Campaign_ended_at or FLSe.Campaign_status == "stopping":
        # Only BeginCampaign may reopen a terminal Campaign, not a late Ready callback.
        FLSe.FLSeReady = False
        return {"Server_Status": FLSe}
    if not ensure_runtime_status(task_id):
        fl_server_status[task_id] = {"status": "FL Server Running"}
    fl_server_status[task_id]["status"] = "FL Server Running"
    
    FLSe = get_or_create_FLSe(task_id)
    FLSe.S3_bucket = Se.S3_bucket if Se.S3_bucket is not None else FLSe.S3_bucket
    FLSe.Last_GL_Model = Se.Last_GL_Model if Se.Last_GL_Model is not None else FLSe.Last_GL_Model
    FLSe.FLServer_start = Se.FLServer_start
    FLSe.FLSeReady = Se.FLSeReady
    FLSe.GL_Model_V = Se.GL_Model_V if Se.GL_Model_V is not None else FLSe.GL_Model_V
    FLSe.Task_status = Se.Task_status
    FLSe.Campaign_run_id = Se.Campaign_run_id or FLSe.Campaign_run_id
    FLSe.Base_GL_Model_V = (
        Se.Base_GL_Model_V if Se.Base_GL_Model_V is not None else FLSe.Base_GL_Model_V
    )
    FLSe.Target_GL_Model_V = (
        Se.Target_GL_Model_V if Se.Target_GL_Model_V is not None else FLSe.Target_GL_Model_V
    )
    FLSe.Campaign_config = Se.Campaign_config or FLSe.Campaign_config
    if FLSe.Campaign_run_id:
        FLSe.Campaign_status = "running"
        fl_server_status[task_id]["campaign_status"] = "running"
    return {"Server_Status": FLSe}


@app.put("/FLSe/FLRoundFin/{task_id}")
def update_ready(task_id: str, FLSeReady: bool, campaign_run_id: Optional[str] = None):
    global fl_server_status, FLSe_dict
    runtime = ensure_runtime_status(task_id) or {}
    FLSe = get_or_create_FLSe(task_id)
    try:
        campaign_lifecycle.assert_campaign(FLSe, campaign_run_id)
        if not FLSeReady:
            campaign_lifecycle.finish(FLSe, runtime, FL_task_list, task_id, run_id=campaign_run_id)
        elif not FLSe.Campaign_ended_at and FLSe.Campaign_status != "stopping":
            FLSe.FLSeReady = True
    except ValueError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    fl_server_status[task_id] = runtime
    return {"Server_Status": FLSe}


class EndCampaignRequest(BaseModel):
    runId: str = Field(min_length=8, max_length=128)
    phase: Literal["request", "finished"] = "finished"


@app.post("/FLSe/EndCampaign/{task_id}")
def end_campaign(task_id: str, request: EndCampaignRequest):
    """Scope stop intent and completion to the owning Web request's Campaign."""
    FLSe = get_or_create_FLSe(task_id)
    runtime = ensure_runtime_status(task_id) or {}
    try:
        campaign_lifecycle.stop(FLSe, runtime, FL_task_list, task_id,
                                run_id=request.runId, phase=request.phase)
    except ValueError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    fl_server_status[task_id] = runtime
    return {"Server_Status": FLSe}


@app.put("/FLSe/ScalableServerReady/{task_id}")
def scalable_server_ready(task_id: str):
    """Scalable FL 서버 Pod가 실행 준비 완료되었음을 알림 (코드/의존성 준비 완료)"""
    global fl_server_status, FLSe_dict
    
    if ensure_runtime_status(task_id):
        # Pod 준비 완료 상태로 변경 (server_main.py 실행 대기 중)
        fl_server_status[task_id]["status"] = "FL Server created"
        
        logging.info(f"Scalable FL server Pod {task_id} is ready (waiting for server_main.py execution)")
        
        return {
            "status": "success",
            "message": f"Scalable FL server Pod {task_id} setup completed and ready for FL execution",
            "server_status": fl_server_status[task_id]
        }
    else:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")

@app.put("/FLSe/ScalableServerRunning/{task_id}")
def scalable_server_running(task_id: str):
    """Scalable FL 서버가 실제로 실행 중임을 알림 (server_main.py 실행 시작)"""
    global fl_server_status, FLSe_dict
    
    if ensure_runtime_status(task_id):
        FLSe = get_or_create_FLSe(task_id)
        if FLSe.Campaign_ended_at or FLSe.Campaign_status == "stopping":
            FLSe.FLSeReady = False
            return {"status": "ignored", "message": "Campaign is already terminal"}
        # FL 서버 실행 중 상태로 변경
        fl_server_status[task_id]["status"] = "FL Server Running"
        
        # FLSe 상태도 업데이트
        FLSe = get_or_create_FLSe(task_id)
        FLSe.FLSeReady = True
        FLSe.FLServer_start = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        
        logging.info(f"Scalable FL server {task_id} is now running (server_main.py started)")
        
        return {
            "status": "success",
            "message": f"Scalable FL server {task_id} is running",
            "server_status": fl_server_status[task_id]
        }
    else:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")

@app.get("/FLSe/CheckServerReady/{task_id}")
def check_server_ready(task_id: str):
    """클라이언트가 서버 준비 상태를 확인하는 API"""
    global fl_server_status, FLSe_dict
    
    if not ensure_runtime_status(task_id):
        return {
            "ready": False,
            "status": "Not Found",
            "message": f"Task {task_id} not found"
        }
    
    server_info = fl_server_status[task_id]
    FLSe = get_or_create_FLSe(task_id)
    
    # 서버가 Ready 상태이고 FL 서버가 실행 중인지 확인
    is_ready = (
        server_info.get("status") == "FL Server Running" and
        FLSe.FLSeReady and
        server_info.get("external_ip") and
        server_info.get("port")
    )
    
    return {
        "ready": is_ready,
        "status": server_info.get("status", "Unknown"),
        "fl_ready": FLSe.FLSeReady,
        "connection_info": {
            "external_ip": server_info.get("external_ip"),
            "port": server_info.get("port"),
            "server_address": f"{server_info.get('external_ip')}:{server_info.get('port')}" if server_info.get("external_ip") and server_info.get("port") else None
        }
    }

@app.put("/FLSe/FLSeClosed/{task_id}")
def server_closed(task_id: str, FLSeReady: bool):
    global FLSe, FL_task_list, fl_server_status, FLSe_dict
    FLSe = get_or_create_FLSe(task_id)

    # Clear FL_task_list for matching task_id
    FL_task_list = [task for task in FL_task_list if task.FL_task_ID != task_id]

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
                              server_repo_addr: str = '',
                              runtime_contract: Literal['legacy-v1', 'federated-task-v2', 'federated-task-v3'] = 'legacy-v1'):
    """웹에서 스케일링 가능한 FL 서버 생성"""
    try:
        from utils.scalable_server_operator import create_scalable_fl_server
        
        background_tasks.add_task(
            create_scalable_fl_server,
            task_id=task_id,
            fl_server_status=fl_server_status,
            server_repo_addr=server_repo_addr,
            runtime_contract=runtime_contract,
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
        logging.info(f"SBA-FL target: {task_data.sba_fl_target}")
        
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
            namespace=TARGET_NAMESPACE,
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

@app.post("/web-control/create-scalable-server-from-saved/{task_id}")
def create_scalable_server_from_saved_web(task_id: str, background_tasks: BackgroundTasks):
    """저장된 task 데이터를 사용하여 스케일링 가능한 FL 서버 생성"""
    try:
        from utils.scalable_server_operator import create_scalable_fl_server
        
        # 저장된 task 데이터 확인
        if task_id not in fl_server_status:
            raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
        
        saved_status = fl_server_status[task_id]
        if "task_data" not in saved_status:
            raise HTTPException(status_code=400, detail=f"No saved task data found for task {task_id}")
        
        # 저장된 task 데이터로 TestStartingTaskData 재생성
        task_data_dict = saved_status["task_data"]
        
        # 딕셔너리를 TestStartingTaskData 객체로 변환
        class TaskData:
            def __init__(self, **kwargs):
                for key, value in kwargs.items():
                    setattr(self, key, value)
        
        task_data = TaskData(**task_data_dict)
        
        logging.info(f"Creating scalable FL server from saved data for task: {task_id}")
        logging.info(f"Saved FL Configuration: {task_data.data_type}, {task_data.strategy}")
        
        background_tasks.add_task(
            create_scalable_fl_server,
            task_id=task_id,
            fl_server_status=fl_server_status,
            server_repo_addr=task_data.server_repo_addr or '',
            initial_cpu="1",
            initial_memory="2Gi",
            namespace=TARGET_NAMESPACE,
            task_data=task_data  # 저장된 FL config 데이터 사용
        )
        
        return {
            "message": f"Scalable FL server creation started for task {task_id} using saved config",
            "config_applied": True,
            "yaml_saved": saved_status.get("yaml_saved", False)
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating scalable server from saved data: {e}")
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
        
        if resume_fl_server(task_id=task_id):
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
        namespace = TARGET_NAMESPACE
        
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
        
        runtime_status = ensure_runtime_status(task_id) or {}

        return {
            "task_id": task_id,
            "deployment": deployment_status,
            "pods": pod_status,
            "pvc": pvc_status,
            "fl_server_status": runtime_status
        }
        
    except Exception as e:
        logger.error(f"Error getting server status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/web-control/validation-data/{task_id}")
def list_validation_data(task_id: str):
    from utils.validation_transfer import transfer
    try:
        return transfer(task_id)
    except ValueError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error


@app.post("/web-control/validation-data/{task_id}")
async def upload_validation_data(task_id: str, request: Request):
    import re
    import tempfile
    from starlette.concurrency import run_in_threadpool
    from utils.validation_transfer import transfer
    from utils.validation_store import MAX_ARCHIVE
    digest = request.headers.get('x-fedops-sha256', '')
    if not re.fullmatch(r'[a-f0-9]{64}', digest):
        raise HTTPException(status_code=422, detail='A dataset checksum is required')
    if request.headers.get('content-type') != 'application/zip':
        raise HTTPException(status_code=415, detail='Expected application/zip')
    # Receive in bounded chunks, never buffer the whole archive in memory.
    with tempfile.NamedTemporaryFile() as target:
        total = 0
        async for chunk in request.stream():
            total += len(chunk)
            if total > MAX_ARCHIVE:
                raise HTTPException(status_code=413, detail='Validation archive too large')
            await run_in_threadpool(target.write, chunk)
        target.flush()
        try:
            return await run_in_threadpool(transfer, task_id, target.name, digest)
        except ValueError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error


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
        
        namespace = TARGET_NAMESPACE
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
        
        namespace = TARGET_NAMESPACE
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


@app.delete("/web-control/delete/{task_id}")
def delete_fl_stack_api(task_id: str, namespace: str = TARGET_NAMESPACE, delete_pv: bool = True) -> Dict[str, Any]:
    """
    task_id 기준으로 Kubernetes 리소스(Deployment, Service, PVC, PV)를 삭제하고,
    프로세스 내부 상태(fl_server_status, FL_task_list, FLSe_dict)까지 정리한다.
    """
    try:
        from utils.scalable_server_operator import delete_fl_stack

        # 1) K8s 리소스 삭제
        report = delete_fl_stack(task_id=task_id, namespace=namespace, delete_pv=delete_pv)

        # 2) 인메모리 상태 정리
        global fl_server_status, FL_task_list, FLSe_dict
        try:
            # 락이 있다면 사용(없으면 noop)
            lock = globals().get("_state_lock")
            ctx = lock if lock else contextlib.nullcontext()
            with ctx:
                if task_id in fl_server_status:
                    del fl_server_status[task_id]
                FL_task_list = [t for t in FL_task_list if t.FL_task_ID != task_id]
                if task_id in FLSe_dict:
                    del FLSe_dict[task_id]
            report["in_memory_state_cleared"] = True
        except Exception as e:
            report.setdefault("errors", []).append({"in_memory_state": str(e)})
            report["in_memory_state_cleared"] = False

        return report

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting stack for task {task_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

LOG_STREAM_ROOT = "/app/data/logs"
LOG_STREAM_INITIAL_LINES = 100
LOG_STREAM_POLL_SECONDS = 2
LOG_STREAM_HEARTBEAT_SECONDS = 15


def normalize_log_stream_path(file_path: str) -> str:
    normalized = posixpath.normpath(file_path or "")
    if not normalized.startswith(f"{LOG_STREAM_ROOT}/"):
        raise HTTPException(
            status_code=400,
            detail=f"Log file must be inside {LOG_STREAM_ROOT}",
        )
    return normalized


def format_sse_message(event_type: str, content: str) -> str:
    payload = json.dumps(
        {"type": event_type, "content": content},
        ensure_ascii=False,
    )
    return f"data: {payload}\n\n"


def get_task_log_pod(v1, namespace: str, task_id: str):
    pods = v1.list_namespaced_pod(
        namespace=namespace,
        label_selector=f"app=fl-server,task_id={task_id}",
    )
    running_pods = [
        pod for pod in pods.items
        if pod.status.phase == "Running"
        and pod.metadata.deletion_timestamp is None
    ]
    if not running_pods:
        raise RuntimeError(f"No running FL server Pod found for task {task_id}")

    running_pods.sort(
        key=lambda pod: pod.metadata.creation_timestamp,
        reverse=True,
    )
    pod = running_pods[0]
    container_names = [container.name for container in pod.spec.containers]
    container_name = next(
        (name for name in container_names if name.startswith("fl-server-")),
        container_names[0],
    )
    return pod.metadata.name, container_name


def exec_in_task_pod(v1, namespace: str, pod_name: str, container_name: str, command):
    return stream(
        v1.connect_get_namespaced_pod_exec,
        name=pod_name,
        namespace=namespace,
        container=container_name,
        command=command,
        stderr=False,
        stdin=False,
        stdout=True,
        tty=False,
    )


def get_pod_file_size(v1, namespace: str, pod_name: str, container_name: str, file_path: str) -> int:
    output = exec_in_task_pod(
        v1,
        namespace,
        pod_name,
        container_name,
        ["stat", "-c", "%s", "--", file_path],
    )
    value = output.strip()
    return int(value) if value.isdigit() else 0


@app.get("/web-control/stream-logs/{task_id}")
async def stream_logs(
    task_id: str,
    request: Request,
    file_path: str = "/app/data/logs/serverlog.txt",
):
    """Stream a Task Pod log file as Server-Sent Events."""
    safe_file_path = normalize_log_stream_path(file_path)
    namespace = TARGET_NAMESPACE

    try:
        config.load_incluster_config()
    except ConfigException:
        config.load_kube_config("config.txt")

    v1 = client.CoreV1Api()

    async def log_generator():
        try:
            pod_name, container_name = await asyncio.to_thread(
                get_task_log_pod,
                v1,
                namespace,
                task_id,
            )
            logging.info(
                "Starting log stream for task=%s pod=%s file=%s",
                task_id,
                pod_name,
                safe_file_path,
            )

            try:
                last_size = await asyncio.to_thread(
                    get_pod_file_size,
                    v1,
                    namespace,
                    pod_name,
                    container_name,
                    safe_file_path,
                )
                initial_logs = await asyncio.to_thread(
                    exec_in_task_pod,
                    v1,
                    namespace,
                    pod_name,
                    container_name,
                    ["tail", "-n", str(LOG_STREAM_INITIAL_LINES), "--", safe_file_path],
                )
            except Exception:
                last_size = 0
                initial_logs = f"Waiting for log file: {safe_file_path}"

            yield format_sse_message("initial", initial_logs)
            last_heartbeat = asyncio.get_running_loop().time()

            while not await request.is_disconnected():
                await asyncio.sleep(LOG_STREAM_POLL_SECONDS)
                try:
                    current_size = await asyncio.to_thread(
                        get_pod_file_size,
                        v1,
                        namespace,
                        pod_name,
                        container_name,
                        safe_file_path,
                    )
                except Exception:
                    current_size = 0

                if current_size < last_size:
                    reset_logs = await asyncio.to_thread(
                        exec_in_task_pod,
                        v1,
                        namespace,
                        pod_name,
                        container_name,
                        ["tail", "-n", str(LOG_STREAM_INITIAL_LINES), "--", safe_file_path],
                    )
                    last_size = current_size
                    yield format_sse_message("initial", reset_logs)
                elif current_size > last_size:
                    new_content = await asyncio.to_thread(
                        exec_in_task_pod,
                        v1,
                        namespace,
                        pod_name,
                        container_name,
                        ["tail", "-c", f"+{last_size + 1}", "--", safe_file_path],
                    )
                    last_size = current_size
                    if new_content:
                        yield format_sse_message("update", new_content)

                now = asyncio.get_running_loop().time()
                if now - last_heartbeat >= LOG_STREAM_HEARTBEAT_SECONDS:
                    yield ": keep-alive\n\n"
                    last_heartbeat = now
        except asyncio.CancelledError:
            logging.info("Log stream cancelled for task=%s", task_id)
        except Exception as error:
            logging.error("Log stream failed for task=%s: %s", task_id, error)
            yield format_sse_message("error", str(error))

    return StreamingResponse(
        log_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# ============= 통합 태스크 관리 API (기존 방식 + Scalable 서버) =============

@app.get("/FLSe/GetAvailableClients/{task_id}")
def get_available_clients(task_id: str):
    """특정 태스크에 등록된 온라인 클라이언트 목록 조회 (웹 UI용)"""
    global FL_task_list
    
    # 해당 태스크에 등록된 클라이언트들만 필터링
    available_clients = []
    for task in FL_task_list:
        if task.FL_task_ID == task_id and task.Device_online:
            client_info = {
                "device_mac": task.Device_mac,
                "device_hostname": task.Device_hostname,
                "device_online": task.Device_online,
                "device_training": task.Device_training,
                "cluster_id": task.clusterId,
                "last_request_time": task.last_request_time
            }
            available_clients.append(client_info)
    
    return {
        "task_id": task_id,
        "available_clients": available_clients,
        "total_count": len(available_clients)
    }

@app.post("/FLSe/StartFLWithSelectedClients/{task_id}")
def start_fl_with_selected_clients(task_id: str, request_data: dict, background_tasks: BackgroundTasks):
    """선택된 클라이언트들과 함께 연합학습 시작 (기존 방식 호환)"""
    global fl_server_status, FL_task_list
    
    selected_devices = request_data.get("selected_devices", [])
    server_type = request_data.get("server_type", "scalable")  # "scalable" or "job"
    fl_config = request_data.get("fl_config", {})
    
    if not selected_devices:
        raise HTTPException(status_code=400, detail="No devices selected")
    
    # 선택된 디바이스들이 모두 온라인 상태인지 확인
    online_devices = []
    for device_mac in selected_devices:
        matching_task = next((t for t in FL_task_list 
                            if t.FL_task_ID == task_id and t.Device_mac == device_mac 
                            and t.Device_online), None)
        if matching_task:
            online_devices.append(device_mac)
        else:
            logger.warning(f"Device {device_mac} is not online or not found")
    
    if not online_devices:
        raise HTTPException(status_code=400, detail="No selected devices are online")
    
    # 태스크 상태 초기화
    if task_id in fl_server_status:
        if "Error" not in fl_server_status[task_id]["status"]:
            return {"status": "already started", "selected_devices": online_devices}
    
    fl_server_status[task_id] = {"status": "Initializing", "selected_devices": online_devices}
    
    # FL 설정이 있으면 TestStartingTaskData 형식으로 변환
    if fl_config:
        task_data = TestStartingTaskData(
            task_id=task_id,
            devices=online_devices,
            server_repo_addr=fl_config.get("server_repo_addr", ""),
            yaml_config=fl_config.get("yaml_config"),
            data_type=fl_config.get("data_type"),
            model_type=fl_config.get("model_type"),
            learning_rate=fl_config.get("learning_rate"),
            num_epochs=fl_config.get("num_epochs"),
            batch_size=fl_config.get("batch_size"),
            num_rounds=fl_config.get("num_rounds"),
            client_per_round=str(len(online_devices)),  # 선택된 클라이언트 수
            strategy=fl_config.get("strategy"),
            strategy_params=fl_config.get("strategy_params"),
            xai_enabled=fl_config.get("xai_enabled"),
            llm_params=fl_config.get("llm_params"),
            dataset_params=fl_config.get("dataset_params"),
            campaign_config=fl_config.get("campaign_config"),
            runtime_contract=fl_config.get("runtime_contract", "legacy-v1"),
            runtime_release=fl_config.get("runtime_release"),
        )
        
        if server_type == "scalable":
            # Scalable 서버 사용
            from utils.scalable_server_operator import create_scalable_fl_server
            background_tasks.add_task(
                create_scalable_fl_server,
                task_id=task_id,
                fl_server_status=fl_server_status,
                server_repo_addr=task_data.server_repo_addr,
                initial_cpu="1",
                initial_memory="2Gi", 
                namespace=TARGET_NAMESPACE,
                task_data=task_data
            )
        else:
            # 기존 Job 방식 사용
            background_tasks.add_task(
                server_operator_test.create_fl_server,
                task_id,
                fl_server_status,
                task_data.server_repo_addr,
                task_data
            )
    else:
        # 기본 설정으로 시작 (기존 방식)
        task_data = StartingTaskData(
            task_id=task_id,
            devices=online_devices,
            server_repo_addr=""
        )
        
        background_tasks.add_task(
            server_operator.create_fl_server,
            task_id,
            fl_server_status,
            task_data.server_repo_addr
        )
    
    # 선택된 클라이언트들의 상태를 training으로 업데이트
    for device_mac in online_devices:
        for task in FL_task_list:
            if task.FL_task_ID == task_id and task.Device_mac == device_mac:
                task.Device_training = True
                break
    
    return {
        "status": "Task started",
        "task_id": task_id,
        "selected_devices": online_devices,
        "server_type": server_type,
        "total_selected": len(online_devices)
    }

@app.get("/FLSe/GetTaskSummary/{task_id}")
def get_task_summary(task_id: str):
    """태스크 전체 상태 요약 (웹 대시보드용)"""
    global FL_task_list, fl_server_status, FLSe_dict
    
    # 서버 상태
    server_status = fl_server_status.get(task_id, {"status": "Not Started"})
    
    # 클라이언트 상태 통계
    task_clients = [t for t in FL_task_list if t.FL_task_ID == task_id]
    client_stats = {
        "total": len(task_clients),
        "online": sum(1 for t in task_clients if t.Device_online),
        "training": sum(1 for t in task_clients if t.Device_training),
        "offline": sum(1 for t in task_clients if not t.Device_online)
    }
    
    # 클러스터별 통계
    cluster_stats = {}
    for client in task_clients:
        cluster_id = client.clusterId or "unassigned"
        if cluster_id not in cluster_stats:
            cluster_stats[cluster_id] = {"total": 0, "online": 0, "training": 0}
        cluster_stats[cluster_id]["total"] += 1
        if client.Device_online:
            cluster_stats[cluster_id]["online"] += 1
        if client.Device_training:
            cluster_stats[cluster_id]["training"] += 1
    
    # FL 서버 상태 (ServerStatus)
    fl_se = FLSe_dict.get(task_id)
    fl_server_info = {
        "ready": fl_se.FLSeReady if fl_se else False,
        "model_version": fl_se.GL_Model_V if fl_se else 0,
        "start_time": fl_se.FLServer_start if fl_se else None,
        "campaign_run_id": fl_se.Campaign_run_id if fl_se else None,
        "base_global_model_version": fl_se.Base_GL_Model_V if fl_se else None,
        "target_global_model_version": fl_se.Target_GL_Model_V if fl_se else None,
        "campaign_config": fl_se.Campaign_config if fl_se else None,
        "campaign_started_at": fl_se.Campaign_started_at if fl_se else None,
        "campaign_ended_at": fl_se.Campaign_ended_at if fl_se else None,
        "campaign_status": fl_se.Campaign_status if fl_se else None,
    }
    
    return {
        "task_id": task_id,
        "server_status": server_status,
        "client_stats": client_stats,
        "cluster_stats": cluster_stats,
        "fl_server_info": fl_server_info,
        "connection_info": {
            "external_ip": server_status.get("external_ip"),
            "port": server_status.get("port")
        }
    }

@app.post("/FLSe/BulkAssignCluster/{task_id}")
def bulk_assign_cluster(task_id: str, assignments: dict):
    """여러 클라이언트를 한번에 클러스터에 할당"""
    global FL_task_list, CLUSTER_MAP
    
    device_assignments = assignments.get("assignments", [])
    # assignments = [{"device_mac": "mac1", "cluster_id": 1}, ...]
    
    results = []
    for assignment in device_assignments:
        device_mac = assignment.get("device_mac")
        cluster_id = assignment.get("cluster_id")
        
        if not device_mac:
            continue
            
        # 영속 맵에 저장
        CLUSTER_MAP[(task_id, device_mac)] = cluster_id
        
        # 런타임 리스트에도 반영
        updated = False
        for task in FL_task_list:
            if task.FL_task_ID == task_id and task.Device_mac == device_mac:
                task.clusterId = cluster_id
                updated = True
                break
        
        if not updated:
            # 클라이언트가 아직 등록되지 않은 경우 최소 정보로 추가
            FL_task_list.append(
                FLTask(
                    FL_task_ID=task_id,
                    Device_mac=device_mac,
                    Device_hostname='',
                    Device_online=False,
                    Device_training=False,
                    clusterId=cluster_id
                )
            )
        
        results.append({
            "device_mac": device_mac,
            "cluster_id": cluster_id,
            "status": "assigned"
        })
    
    return {
        "task_id": task_id,
        "results": results,
        "total_assigned": len(results)
    }

async def monitor_task():
    while True:
        current_time = datetime.datetime.today()
        global FL_task_list
        FL_task_list = [task for task in FL_task_list if (current_time - datetime.datetime.strptime(task.last_request_time, '%Y-%m-%d %H:%M:%S')).total_seconds() <= 300]
        await asyncio.sleep(60)  # wait for 60 seconds


@app.on_event("startup")
async def startup_runtime_state():
    """Restore durable runtime allocation state and start presence cleanup."""
    try:
        recovered = reconcile_runtime_statuses(fl_server_status)
        logger.info(
            "Recovered %s scalable FL runtime allocation(s): %s",
            len(recovered),
            ", ".join(sorted(recovered)) or "none",
        )
    except Exception:
        logger.exception("Failed to reconcile scalable FL runtimes during startup")
    app.state.monitor_task = asyncio.create_task(monitor_task())


@app.on_event("shutdown")
async def shutdown_runtime_state():
    monitor = getattr(app.state, "monitor_task", None)
    if monitor:
        monitor.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await monitor


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=False)
