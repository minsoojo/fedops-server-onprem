from pydantic import BaseModel,Field

import uvicorn
from fastapi import FastAPI
from typing import Optional
import logging, os
import pymongo


logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)8.8s] %(message)s",
                    handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)

class TrainResult(BaseModel):
    fl_task_id: str = ''
    client_mac: str = ''
    client_name: str = ''
    round: int = 0
    train_loss: float = 0
    train_accuracy: float = 0
    val_loss: float = 0
    val_accuracy: float = 0
    train_time: float = 0
    gl_model_v: int = 0
    wandb_name: str = ''
    # 일단 옵션으로 값 지정
    cluster_id: Optional[int] = Field(default=None, alias="clusterId")
    class Config:
        allow_population_by_field_name = True  # alias/원래이름 모두 허용

class TestResult(BaseModel):
    fl_task_id: str = ''
    client_mac: str = ''
    client_name: str = ''
    round: int = 0
    test_loss: float = 0
    test_accuracy: float = 0
    gl_model_v: int = 0
    wandb_name: str = ''
    # 일단 옵션으로 값 지정
    cluster_id: Optional[int] = Field(default=None, alias="clusterId")
    class Config:
        allow_population_by_field_name = True

class ClientTimeResult(BaseModel):
    fl_task_id: str = ''
    client_mac: str = ''
    client_name: str = ''
    operation_time: float = 0
    gl_model_v: int = 0
    wandb_name: str = ''

class ClientBasicSystem(BaseModel):
    network_sent: float = 0
    network_recv: float = 0
    disk: float = 0
    runtime: float = 0
    memory_rssMB: float = 0
    memory_availableMB: float = 0
    cpu: float = 0
    cpu_threads: float = 0
    memory: float = 0
    memory_percent: float = 0
    timestamp: float = 0
    fl_task_id: str = ''
    client_mac: str = ''
    gl_model_v: int = 0
    wandb_name: str = ''


class GLModelEvaluation(BaseModel):
    fl_task_id: str = ''
    round: int = 0
    gl_loss: Optional[float] = None
    gl_accuracy: Optional[float] = None
    run_time_by_round: float = 0
    gl_model_v: int = 0
    campaign_run_id: Optional[str] = None
    evaluation_source: Optional[str] = None
    evaluation_status: Optional[str] = None
    evaluation_clients: Optional[int] = None
    evaluation_samples: Optional[int] = None
    accuracy_samples: Optional[int] = None
    evaluation_failures: Optional[int] = None
    additional_metrics: dict = Field(default_factory=dict)

class ServerTimeResult(BaseModel):
    fl_task_id: str = ''
    server_operation_time: float = 0
    gl_model_v: int = 0

# class ClientGpuSystem(BaseModel):


# create App
app = FastAPI()

# Define Class
train_result = TrainResult()
test_result = TestResult()
client_time_result = ClientTimeResult()
client_basic_system = ClientBasicSystem()
gl_model_evaluation = GLModelEvaluation()
server_time_result = ServerTimeResult()

# MogoDB
MONGODB_URI = os.environ["MONGODB_URI"]
MONGODB_DATABASE = os.environ["MONGODB_DATABASE"]

# logging.info(f'MONGODB_URI: {MONGODB_URI}')
# logging.info(f'MONGODB_DATABASE: {MONGODB_DATABASE}')


# Connect MongoDB
client = pymongo.MongoClient(MONGODB_URI)
db = client[MONGODB_DATABASE]

@app.put("/client_perf/train_result/{task_id}")
def train_result_put(task_id: str, Train: TrainResult):
    global train_result, db

    train_result.fl_task_id = task_id
    train_result.client_mac = Train.client_mac
    train_result.client_name = Train.client_name
    train_result.round = Train.round
    train_result.train_loss = Train.train_loss
    train_result.train_accuracy= Train.train_accuracy
    train_result.val_loss = Train.val_loss
    train_result.val_accuracy = Train.val_accuracy
    train_result.train_time = Train.train_time
    train_result.gl_model_v = Train.gl_model_v
    train_result.wandb_name = Train.wandb_name

    logging.info(f'train_result: {train_result}')

    collection = db["fl-client_train_result_log"]

    # input train_result data
    document = {
        "fl_task_id": task_id,
        "client_mac": train_result.client_mac,
        "client_name": train_result.client_name,
        "round": train_result.round,
        "train_loss": train_result.train_loss,
        "train_accuracy": train_result.train_accuracy,
        "val_loss": train_result.val_loss,
        "val_accuracy": train_result.val_accuracy,
        "train_time": train_result.train_time,
        "gl_model_v": train_result.gl_model_v,
        "wandb_name": train_result.wandb_name
    }
    # cluster_id가 있으면 값 추가
    if Train.cluster_id is not None :
        document["cluster_id"] = Train.cluster_id

    collection.insert_one(document)

    return {"train_result": train_result}


@app.put("/client_perf/test_result/{task_id}")
def test_result_put(task_id: str, Test: TestResult):
    global test_result, db

    test_result.fl_task_id = task_id
    test_result.client_mac = Test.client_mac
    test_result.client_name = Test.client_name
    test_result.round = Test.round
    test_result.test_loss = Test.test_loss
    test_result.test_accuracy = Test.test_accuracy
    test_result.gl_model_v = Test.gl_model_v
    test_result.wandb_name = Test.wandb_name

    logging.info(f'test_result: {test_result}')

    collection = db["fl-client_test_result_log"]

    # input test_result data
    document = {
        "fl_task_id": task_id,
        "client_mac": test_result.client_mac,
        "client_name": test_result.client_name,
        "round": test_result.round,
        "test_loss": test_result.test_loss,
        "test_accuracy": test_result.test_accuracy,
        "gl_model_v": test_result.gl_model_v,
        "wandb_name": test_result.wandb_name
    }
    # cluster_id가 있으면 값 추가
    if  Test.cluster_id is not None :
        document["cluster_id"] = Test.cluster_id
    collection.insert_one(document)

    return {"test_result": test_result}

@app.put("/client_perf/client_time_result/{task_id}")
def client_time_result_put(task_id: str, Time: ClientTimeResult):
    global client_time_result, db

    client_time_result.fl_task_id = task_id
    client_time_result.client_mac = Time.client_mac
    client_time_result.client_name = Time.client_name
    client_time_result.operation_time = Time.operation_time
    client_time_result.gl_model_v = Time.gl_model_v
    client_time_result.wandb_name = Time.wandb_name

    logging.info(f'client_time_result: {client_time_result}')

    collection = db["fl-client_time_result_log"]

    # input client_time_result data
    document = {
        "fl_task_id": task_id,
        "client_mac": client_time_result.client_mac,
        "client_name": client_time_result.client_name,
        "operation_time": client_time_result.operation_time,
        "gl_model_v": client_time_result.gl_model_v,
        "wandb_name": client_time_result.wandb_name
    }

    collection.insert_one(document)

    return {"client_time_result": client_time_result}

@app.put("/client_perf/client_system/{task_id}")
def client_basic_system_put(task_id: str, System: ClientBasicSystem):
    global client_basic_system, db

    client_basic_system.network_sent = System.network_sent
    client_basic_system.network_recv = System.network_recv
    client_basic_system.disk = System.disk
    client_basic_system.runtime = System.runtime
    client_basic_system.memory_rssMB = System.memory_rssMB
    client_basic_system.memory_availableMB = System.memory_availableMB
    client_basic_system.cpu = System.cpu
    client_basic_system.cpu_threads = System.cpu_threads
    client_basic_system.memory = System.memory
    client_basic_system.memory_percent = System.memory_percent
    client_basic_system.timestamp = System.timestamp
    client_basic_system.fl_task_id = task_id
    client_basic_system.client_mac = System.client_mac
    client_basic_system.gl_model_v = System.gl_model_v
    client_basic_system.wandb_name = System.wandb_name

    logging.info(f'client_basic_system: {client_basic_system}')

    collection = db["fl-client_basic_system_log"]

    # input client_basic_system data
    document = {
        "fl_task_id": task_id,
        "client_mac": client_basic_system.client_mac,
        "network_sent": client_basic_system.network_sent,
        "network_recv": client_basic_system.network_recv,
        "disk_utilization": client_basic_system.disk,
        "runtime": client_basic_system.runtime,
        "memory_rssMB": client_basic_system.memory_rssMB,
        "memory_availableMB": client_basic_system.memory_availableMB,
        "cpu_utilization": client_basic_system.cpu,
        "cpu_threads": client_basic_system.cpu_threads,
        "memory_utilization": client_basic_system.memory,
        "memory_percent": client_basic_system.memory_percent,
        "timestamp": client_basic_system.timestamp,
        "gl_model_v": client_basic_system.gl_model_v,
        "wandb_name": client_basic_system.wandb_name
    }

    collection.insert_one(document)


    return {"client_system": client_basic_system}


@app.put("/server_perf/gl_model_evaluation/{task_id}")
def gl_model_evaluation_put(task_id: str, Evaluation: GLModelEvaluation):
    global gl_model_evaluation, db

    gl_model_evaluation.fl_task_id = task_id
    gl_model_evaluation.round = Evaluation.round
    gl_model_evaluation.gl_loss = Evaluation.gl_loss
    gl_model_evaluation.gl_accuracy = Evaluation.gl_accuracy
    gl_model_evaluation.run_time_by_round = Evaluation.run_time_by_round
    gl_model_evaluation.gl_model_v = Evaluation.gl_model_v

    logging.info(f'gl_model_evaluation_result: {gl_model_evaluation}')

    collection = db["fl-gl_model_evaluation_log"]

    # input train_result data
    document = {
        "fl_task_id": task_id,
        "round": gl_model_evaluation.round,
        "gl_loss": gl_model_evaluation.gl_loss,
        "gl_accuracy": gl_model_evaluation.gl_accuracy,
        "run_time_by_round": gl_model_evaluation.run_time_by_round,
        "gl_model_v": gl_model_evaluation.gl_model_v,
    }

    # Preserve source and nullable metrics; absence must never become a fake zero.
    document.update(Evaluation.dict())
    document["fl_task_id"] = task_id
    if Evaluation.campaign_run_id:
        collection.update_one(
            {key: document[key] for key in ("fl_task_id", "campaign_run_id", "round", "gl_model_v")},
            {"$set": document}, upsert=True,
        )
    else:
        collection.insert_one(document)

    return {"gl_model_evaluation": Evaluation}


@app.put("/server_perf/server_time_result/{task_id}")
def server_time_result_put(task_id: str, ServerTime: ServerTimeResult):
    global server_time_result, db

    server_time_result.fl_task_id = task_id
    server_time_result.server_operation_time = ServerTime.server_operation_time
    server_time_result.gl_model_v = ServerTime.gl_model_v

    logging.info(f'server_time_result: {server_time_result}')

    collection = db["fl-server_time_result_log"]

    # input train_result data
    document = {
        "fl_task_id": task_id,
        "server_operation_time": server_time_result.server_operation_time,
        "gl_model_v": server_time_result.gl_model_v,
    }

    collection.insert_one(document)

    return {"gl_model_evaluation": gl_model_evaluation}

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8001, reload=True)
