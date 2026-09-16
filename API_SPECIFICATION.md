# FedOps API 명세서 (API Specification)

## 📋 목차
- [시스템 구조](#시스템-구조)
- [1. Frontend API (React)](#1-frontend-api-react)
- [2. Backend API (Node.js)](#2-backend-api-nodejs)
- [3. FedOps-Server API (Python FastAPI)](#3-fedops-server-api-python-fastapi)
- [4. 워크플로우](#4-워크플로우)

---

## 시스템 구조

```
Frontend (React) → Backend (Node.js) → FedOps-Server (Python FastAPI) → Kubernetes
     ↓                    ↓                        ↓                      ↓
serverControlAPI.js → serverControl/index.js → app.py APIs → scalable_server_operator.py
```

---

## 1. Frontend API (React)

### 📁 파일: `client/src/lib/api/serverControlAPI.js`

### 1.1 서버 라이프사이클 관리

#### `createScalableServer(taskId, serverRepoAddr)`
- **설명**: 기본 스케일링 가능한 FL 서버 생성
- **파라미터**:
  - `taskId` (string): 고유 태스크 ID
  - `serverRepoAddr` (string): FL 서버 저장소 주소 (선택사항)
- **반환**: Promise\<{success: boolean, message: string, taskId: string}>

#### `createScalableServerWithConfig(taskId, flConfig)`
- **설명**: FL 설정이 포함된 스케일링 가능한 서버 생성
- **파라미터**:
  - `taskId` (string): 고유 태스크 ID
  - `flConfig` (object): FL 설정 객체
    ```javascript
    {
      task_id: string,
      server_repo_addr: string,
      yaml_config: string,
      data_type: string,        // "Image", "LLM", etc.
      model_type: string,       // "CNN", "ResNet", etc.
      learning_rate: string,
      num_epochs: string,
      batch_size: string,
      num_rounds: string,
      client_per_round: string,
      strategy: string,         // "FedAvg", "FedProx", etc.
      xai_enabled: string,
      strategy_params: object,
      llm_params: object,
      dataset_params: object
    }
    ```
- **반환**: Promise\<{success: boolean, message: string, config_applied: boolean}>

#### `pauseServer(taskId)`
- **설명**: FL 서버 일시정지 (리소스 해제, 데이터 보존)
- **파라미터**: `taskId` (string)
- **반환**: Promise\<{success: boolean, message: string}>

#### `resumeServer(taskId)`
- **설명**: 일시정지된 FL 서버 재개
- **파라미터**: `taskId` (string)
- **반환**: Promise\<{success: boolean, message: string}>

### 1.2 리소스 스케일링

#### `scaleResources(taskId, cpu, memory)`
- **설명**: FL 서버 리소스 동적 스케일링
- **파라미터**:
  - `taskId` (string)
  - `cpu` (string): CPU 리소스 (예: "2", "500m")
  - `memory` (string): 메모리 리소스 (예: "4Gi", "2048Mi")
- **반환**: Promise\<{success: boolean, message: string}>

### 1.3 서버 상태 및 모니터링

#### `getServerStatus(taskId)`
- **설명**: 서버 상태 및 배포 정보 조회
- **파라미터**: `taskId` (string)
- **반환**: Promise\<ServerStatusResponse>
```javascript
{
  task_id: string,
  deployment: {
    replicas: number,
    ready_replicas: number,
    available_replicas: number
  },
  pods: [{
    name: string,
    phase: string,
    ready: boolean
  }],
  pvc: {
    name: string,
    phase: string,
    capacity: string
  },
  fl_server_status: object
}
```

#### `getLogs(taskId, lines)`
- **설명**: 서버 로그 조회
- **파라미터**:
  - `taskId` (string)
  - `lines` (number): 조회할 로그 라인 수 (기본값: 100)
- **반환**: Promise\<{success: boolean, data: string}>

### 1.4 FL 서버 제어

#### `startFLServer(taskId)`
- **설명**: FL 서버 프로세스 시작
- **파라미터**: `taskId` (string)
- **반환**: Promise\<{success: boolean, message: string}>

#### `stopFLServer(taskId)`
- **설명**: FL 서버 프로세스 종료
- **파라미터**: `taskId` (string)
- **반환**: Promise\<{success: boolean, message: string}>

#### `getFLServerStatus(taskId)`
- **설명**: FL 서버 프로세스 상태 확인
- **파라미터**: `taskId` (string)
- **반환**: Promise\<{success: boolean, data: {processes: string, ports: string}}>

### 1.5 기존 방식 호환 태스크 관리

#### `getAvailableClients(taskId)`
- **설명**: 온라인 상태인 사용 가능한 클라이언트 목록 조회
- **파라미터**: `taskId` (string)
- **반환**: Promise\<AvailableClientsResponse>
```javascript
{
  task_id: string,
  available_clients: [{
    device_mac: string,
    device_hostname: string,
    device_online: boolean,
    device_training: boolean,
    cluster_id: number|null,
    last_request_time: string
  }],
  total_count: number
}
```

#### `startFLWithSelectedClients(taskId, selectedDevices, serverType, flConfig)`
- **설명**: 선택된 클라이언트들과 연합학습 시작
- **파라미터**:
  - `taskId` (string)
  - `selectedDevices` (string[]): 선택된 디바이스 MAC 주소 배열
  - `serverType` (string): "scalable" 또는 "job" (기본값: "scalable")
  - `flConfig` (object): FL 설정 (선택사항)
- **반환**: Promise\<StartFLResponse>
```javascript
{
  status: string,
  task_id: string,
  selected_devices: string[],
  server_type: string,
  total_selected: number
}
```

#### `getTaskSummary(taskId)`
- **설명**: 태스크 전체 상태 요약 (웹 대시보드용)
- **파라미터**: `taskId` (string)
- **반환**: Promise\<TaskSummaryResponse>
```javascript
{
  task_id: string,
  server_status: {
    status: string,
    port: number,
    external_ip: string
  },
  client_stats: {
    total: number,
    online: number,
    training: number,
    offline: number
  },
  cluster_stats: {
    [cluster_id]: {
      total: number,
      online: number,
      training: number
    }
  },
  fl_server_info: {
    ready: boolean,
    model_version: number,
    start_time: string
  },
  connection_info: {
    external_ip: string,
    port: number
  }
}
```

#### `assignCluster(taskId, deviceMac, clusterId)`
- **설명**: 클라이언트를 특정 클러스터에 할당
- **파라미터**:
  - `taskId` (string)
  - `deviceMac` (string): 클라이언트 MAC 주소
  - `clusterId` (number): 클러스터 ID
- **반환**: Promise\<{ok: boolean, cluster_id: number}>

#### `bulkAssignCluster(taskId, assignments)`
- **설명**: 여러 클라이언트를 한번에 클러스터에 할당
- **파라미터**:
  - `taskId` (string)
  - `assignments` (object[]): 할당 정보 배열
    ```javascript
    [{device_mac: string, cluster_id: number}]
    ```
- **반환**: Promise\<BulkAssignResponse>

### 1.6 통합 워크플로우

#### `createFLTaskWorkflow(taskId, flConfig, serverType)`
- **설명**: 전체 FL 워크플로우 시작 (서버 생성 → 클라이언트 대기)
- **파라미터**:
  - `taskId` (string)
  - `flConfig` (object): FL 설정
  - `serverType` (string): "scalable" (기본값)
- **반환**: Promise\<{success: boolean, next_step: string}>

#### `waitAndStartFL(taskId, minimumClients, timeout)`
- **설명**: 최소 클라이언트 수가 연결될 때까지 대기 후 자동 시작
- **파라미터**:
  - `taskId` (string)
  - `minimumClients` (number): 최소 클라이언트 수 (기본값: 1)
  - `timeout` (number): 타임아웃 ms (기본값: 300000)
- **반환**: Promise\<StartFLResponse>

---

## 2. Backend API (Node.js)

### 📁 파일: `server/src/api/serverControl/index.js`

**Base URL**: `/fedops/api/server-control`

### 2.1 서버 라이프사이클 관리

#### `POST /create-scalable/:taskId`
- **설명**: 기본 스케일링 가능한 서버 생성
- **Request Body**:
```javascript
{
  serverRepoAddr: string (optional)
}
```
- **Response**:
```javascript
{
  success: boolean,
  message: string,
  taskId: string
}
```

#### `POST /create-scalable-with-config/:taskId`
- **설명**: FL 설정이 포함된 스케일링 가능한 서버 생성
- **Request Body**: FL 설정 객체 (Frontend API 참조)
- **Response**:
```javascript
{
  success: boolean,
  message: string,
  config_applied: boolean,
  yaml_saved: boolean,
  taskId: string
}
```

#### `POST /pause/:taskId`
- **설명**: 서버 일시정지
- **Response**:
```javascript
{
  success: boolean,
  message: string,
  taskId: string
}
```

#### `POST /resume/:taskId`
- **설명**: 서버 재개
- **Response**:
```javascript
{
  success: boolean,
  message: string,
  taskId: string
}
```

### 2.2 리소스 스케일링

#### `POST /scale/:taskId`
- **설명**: 리소스 스케일링
- **Request Body**:
```javascript
{
  cpu: string,
  memory: string
}
```
- **Response**:
```javascript
{
  success: boolean,
  message: string,
  cpu: string,
  memory: string
}
```

### 2.3 서버 상태 및 모니터링

#### `GET /status/:taskId`
- **설명**: 서버 상태 조회
- **Response**: ServerStatusResponse (Frontend API 참조)

#### `GET /logs/:taskId?lines={number}`
- **설명**: 서버 로그 조회
- **Query Parameters**: `lines` (number, optional)
- **Response**:
```javascript
{
  success: boolean,
  data: string,
  taskId: string
}
```

### 2.4 명령 실행

#### `POST /execute/:taskId`
- **설명**: 컨테이너 내부 명령 실행
- **Request Body**:
```javascript
{
  command: string
}
```
- **Response**:
```javascript
{
  success: boolean,
  data: {
    output: string,
    error: string,
    exit_code: number
  },
  taskId: string
}
```

### 2.5 FL 서버 제어

#### `POST /start-fl-server/:taskId`
- **설명**: FL 서버 시작
- **Response**:
```javascript
{
  success: boolean,
  message: string,
  data: object,
  taskId: string
}
```

#### `POST /stop-fl-server/:taskId`
- **설명**: FL 서버 중지
- **Response**:
```javascript
{
  success: boolean,
  message: string,
  data: object,
  taskId: string
}
```

#### `GET /fl-server-status/:taskId`
- **설명**: FL 서버 상태 확인
- **Response**:
```javascript
{
  success: boolean,
  message: string,
  data: {
    processes: object,
    ports: object
  },
  taskId: string
}
```

### 2.6 기존 방식 호환 클라이언트 관리

#### `GET /clients/:taskId`
- **설명**: 사용 가능한 클라이언트 목록 조회
- **Response**:
```javascript
{
  success: boolean,
  data: AvailableClientsResponse,
  taskId: string
}
```

#### `POST /start-fl-with-clients/:taskId`
- **설명**: 선택된 클라이언트들과 FL 시작
- **Request Body**:
```javascript
{
  selected_devices: string[],
  server_type: string,
  fl_config: object
}
```
- **Response**:
```javascript
{
  success: boolean,
  data: StartFLResponse,
  taskId: string
}
```

#### `GET /task-summary/:taskId`
- **설명**: 태스크 요약 정보 조회
- **Response**:
```javascript
{
  success: boolean,
  data: TaskSummaryResponse,
  taskId: string
}
```

#### `PUT /assign-cluster/:taskId`
- **설명**: 클러스터 할당
- **Request Body**:
```javascript
{
  client_mac: string,
  cluster_id: number
}
```

#### `POST /bulk-assign-cluster/:taskId`
- **설명**: 클러스터 일괄 할당
- **Request Body**:
```javascript
{
  assignments: [{
    device_mac: string,
    cluster_id: number
  }]
}
```

---

## 3. FedOps-Server API (Python FastAPI)

### 📁 파일: `server_manager/app.py`

**Base URL**: `http://localhost:8000`

### 3.1 기존 FL 태스크 관리 (Core APIs)

#### `PUT /FLSe/RegisterFLTask`
- **설명**: FL 클라이언트 등록
- **Request Body**:
```python
{
  FL_task_ID: str,
  Device_mac: str,
  Device_hostname: str,
  Device_online: bool,
  Device_training: bool,
  cluster_id: int (optional)
}
```
- **Response**: List[FLTask]

#### `GET /FLSe/GetFLTask/{task_id}`
- **설명**: 특정 태스크의 모든 등록된 클라이언트 조회
- **Path Parameters**: `task_id` (str)
- **Response**: List[FLTask] 또는 {"error": "message"}

#### `GET /FLSe/info/{task_id}/{device_mac}`
- **설명**: 특정 클라이언트의 상태 정보 조회
- **Path Parameters**: 
  - `task_id` (str)
  - `device_mac` (str)
- **Response**:
```python
{
  "Server_Status": {
    "S3_bucket": str,
    "Last_GL_Model": str,
    "FLServer_start": str,
    "FLSeReady": bool,
    "GL_Model_V": int,
    "Task_status": FLTask
  }
}
```

#### `PUT /FLSe/cluster/{task_id}`
- **설명**: 클라이언트를 클러스터에 할당
- **Path Parameters**: `task_id` (str)
- **Request Body**:
```python
{
  client_mac: str,
  cluster_id: int
}
```
- **Response**: {"ok": bool, "cluster_id": int}

### 3.2 FL 서버 시작 APIs

#### `POST /FLSe/startTask`
- **설명**: 기본 FL 서버 시작 (Job 방식)
- **Request Body**:
```python
{
  task_id: str,
  devices: List[str],
  server_repo_addr: str
}
```
- **Response**: {"status": str}

#### `POST /FLSe/teststartTask`
- **설명**: 테스트 FL 서버 시작 (설정 포함)
- **Request Body**: TestStartingTaskData
- **Response**: {"status": str, "yaml_saved": bool}

#### `POST /FLSe/scalableStartTask`
- **설명**: 스케일링 가능한 FL 서버 시작
- **Request Body**: TestStartingTaskData
- **Response**: 
```python
{
  "status": str,
  "yaml_saved": bool,
  "deployment_type": "scalable"
}
```

### 3.3 FL 서버 상태 관리

#### `GET /FLSe/status/{task_id}`
- **설명**: FL 서버 상태 조회
- **Path Parameters**: `task_id` (str)
- **Response**: {"task_id": str, "status": dict}

#### `GET /FLSe/getPort/{task_id}`
- **설명**: Task ID를 기준으로 현재 FL 서버 연결 endpoint 조회
- **Path Parameters**: `task_id` (str)
- **연결 계약**: 클라이언트는 포트나 IP를 자체 저장·지정하지 않고 Task ID만
  전달합니다. Server Manager는 해당 Task의 Kubernetes Service와 Istio
  VirtualService 실제 경로를 다시 확인한 뒤 현재 할당 endpoint를 반환합니다.
- **Response**: 
```python
{
  "task_id": str,
  "port": int,
  "external_ip": str (optional)
}
```

#### `GET /FLSe/getConnectionInfo/{task_id}`
- **설명**: FL 서버 연결 정보 조회 (포트, 외부 IP 포함)
- **Path Parameters**: `task_id` (str)
- **연결 계약**: `getPort`와 동일하게 Task ID를 현재 Kubernetes 라우팅으로
  해석한 결과이며, 응답 포트는 클라이언트 설정값이 아닙니다.
- **Response**:
```python
{
  "task_id": str,
  "status": str,
  "port": int,
  "external_ip": str,
  "server_type": str,  # "scalable" or "job"
  "deployment": str (optional)
}
```

#### `PUT /FLSe/FLSeUpdate/{task_id}`
- **설명**: FL 서버 상태 업데이트 (FL 서버에서 호출)
- **Path Parameters**: `task_id` (str)
- **Request Body**: ServerStatus
- **Response**: {"Server_Status": ServerStatus}

#### `PUT /FLSe/FLRoundFin/{task_id}`
- **설명**: FL 라운드 완료 알림
- **Path Parameters**: `task_id` (str)
- **Request Body**: {"FLSeReady": bool}
- **Response**: {"Server_Status": ServerStatus}

#### `PUT /FLSe/FLSeClosed/{task_id}`
- **설명**: FL 서버 종료 알림
- **Path Parameters**: `task_id` (str)
- **Request Body**: {"FLSeReady": bool}
- **Response**: {"Server_Status": ServerStatus}

### 3.4 통합 태스크 관리 APIs (새로 추가)

#### `GET /FLSe/GetAvailableClients/{task_id}`
- **설명**: 온라인 상태인 사용 가능한 클라이언트 목록 조회
- **Path Parameters**: `task_id` (str)
- **Response**: AvailableClientsResponse

#### `POST /FLSe/StartFLWithSelectedClients/{task_id}`
- **설명**: 선택된 클라이언트들과 연합학습 시작
- **Path Parameters**: `task_id` (str)
- **Request Body**:
```python
{
  selected_devices: List[str],
  server_type: str,  # "scalable" or "job"
  fl_config: dict
}
```
- **Response**: StartFLResponse

#### `GET /FLSe/GetTaskSummary/{task_id}`
- **설명**: 태스크 전체 상태 요약
- **Path Parameters**: `task_id` (str)
- **Response**: TaskSummaryResponse

#### `POST /FLSe/BulkAssignCluster/{task_id}`
- **설명**: 여러 클라이언트를 한번에 클러스터에 할당
- **Path Parameters**: `task_id` (str)
- **Request Body**:
```python
{
  assignments: List[{
    device_mac: str,
    cluster_id: int
  }]
}
```
- **Response**: BulkAssignResponse

### 3.5 웹 컨트롤 APIs

#### `POST /web-control/create-scalable-server/{task_id}`
- **설명**: 웹에서 스케일링 가능한 FL 서버 생성
- **Path Parameters**: `task_id` (str)
- **Query Parameters**: `server_repo_addr` (str, optional)
- **Response**: {"message": str}

#### `POST /web-control/create-scalable-server-with-config/{task_id}`
- **설명**: FL config를 포함한 스케일링 가능한 FL 서버 생성
- **Path Parameters**: `task_id` (str)
- **Request Body**: TestStartingTaskData
- **Response**:
```python
{
  "message": str,
  "config_applied": bool,
  "yaml_saved": bool
}
```

#### `POST /web-control/scale-resources/{task_id}`
- **설명**: 웹에서 FL 서버 리소스 스케일링
- **Path Parameters**: `task_id` (str)
- **Request Body**:
```python
{
  cpu: str,
  memory: str
}
```
- **Response**: {"message": str, "cpu": str, "memory": str}

#### `POST /web-control/pause/{task_id}`
- **설명**: 웹에서 FL 서버 일시정지
- **Path Parameters**: `task_id` (str)
- **Response**: {"message": str}

#### `POST /web-control/resume/{task_id}`
- **설명**: 웹에서 FL 서버 재개
- **Path Parameters**: `task_id` (str)
- **Response**: {"message": str}

#### `GET /web-control/status/{task_id}`
- **설명**: 웹에서 FL 서버 상태 확인
- **Path Parameters**: `task_id` (str)
- **Response**: ServerStatusResponse (상세)

#### `POST /web-control/execute-command/{task_id}`
- **설명**: 웹에서 컨테이너 내부 명령 실행
- **Path Parameters**: `task_id` (str)
- **Request Body**:
```python
{
  command: str
}
```
- **Response**:
```python
{
  "success": bool,
  "output": str,
  "error": str,
  "exit_code": int
}
```

---

## 4. 워크플로우

### 4.1 기본 FL 서버 생성 및 시작 워크플로우

```
1. Frontend: createScalableServerWithConfig()
   ↓
2. Backend: POST /create-scalable-with-config/:taskId
   ↓
3. FedOps-Server: POST /web-control/create-scalable-server-with-config/:taskId
   ↓
4. Kubernetes: 스케일링 가능한 FL 서버 Pod 생성 + 외부 IP 할당
   ↓
5. Frontend: startFLServer()
   ↓
6. Backend: POST /start-fl-server/:taskId
   ↓
7. FedOps-Server: POST /web-control/execute-command/:taskId (start_fl_server.sh 실행)
   ↓
8. FL Server Process 시작 (포트 8080에서 대기)
```

### 4.2 기존 방식 호환 클라이언트 선택 워크플로우

```
1. FL 클라이언트들: PUT /FLSe/RegisterFLTask (자동 등록)
   ↓
2. Frontend: getAvailableClients() (온라인 클라이언트 목록 조회)
   ↓
3. 웹 UI에서 참여할 클라이언트 선택
   ↓
4. 필요시 클러스터 할당: assignCluster() 또는 bulkAssignCluster()
   ↓
5. Frontend: startFLWithSelectedClients() (선택된 클라이언트와 FL 시작)
   ↓
6. FedOps-Server: 선택된 클라이언트 상태를 training으로 업데이트
   ↓
7. FL 서버 시작 + 외부 IP:포트로 클라이언트들이 연결
   ↓
8. 연합학습 수행
```

### 4.3 모니터링 워크플로우

```
1. Frontend: getTaskSummary() (주기적 호출)
   ↓
2. 서버 상태, 클라이언트 통계, 클러스터 통계 조회
   ↓
3. Frontend: getFLServerStatus() (FL 서버 프로세스 상태)
   ↓
4. Frontend: getLogs() (필요시 로그 확인)
   ↓
5. 실시간 대시보드 업데이트
```

### 4.4 에러 처리 패턴

모든 API는 다음과 같은 일관된 에러 응답 형식을 사용합니다:

```javascript
// 성공 응답
{
  success: true,
  data: any,
  message?: string
}

// 에러 응답
{
  success: false,
  error: string,
  status?: number
}
```

---

## 📋 데이터 모델

### FLTask
```python
{
  FL_task_ID: str,
  Device_mac: str,
  Device_hostname: str,
  Device_online: bool,
  Device_training: bool,
  last_request_time: str,
  clusterId: int | None
}
```

### ServerStatus
```python
{
  S3_bucket: str,
  Last_GL_Model: str,
  FLServer_start: str,
  FLSeReady: bool,
  GL_Model_V: int,
  Task_status: FLTask | None
}
```

### TestStartingTaskData
```python
{
  task_id: str,
  devices: List[str],
  server_repo_addr: str,
  yaml_config: str | None,
  data_type: str | None,
  model_type: str | None,
  learning_rate: str | None,
  num_epochs: str | None,
  batch_size: str | None,
  num_rounds: str | None,
  client_per_round: str | None,
  strategy: str | None,
  strategy_params: dict | None,
  xai_enabled: str | None,
  llm_params: dict | None,
  dataset_params: dict | None
}
```

---

## 🔗 환경 변수

### Backend (Node.js)
- `FL_SERVER_MANAGER_URL`: FedOps-Server URL (기본값: http://192.168.10.4:8000)

### FedOps-Server (Python)
- Kubernetes 설정 파일 경로
- S3 관련 시크릿 (ACCESS_KEY_ID, ACCESS_SECRET_KEY, BUCKET_NAME)

---

이 API 명세서는 FedOps 시스템의 전체 아키텍처와 각 컴포넌트 간의 통신 방법을 상세히 설명합니다. 각 API는 일관된 인터페이스를 제공하며, 기존 방식과의 호환성을 유지하면서 새로운 스케일링 기능을 지원합니다.
