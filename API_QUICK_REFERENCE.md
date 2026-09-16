# FedOps API 빠른 참조 (Quick Reference)

## 🚀 주요 워크플로우

### 1. 기본 FL 서버 생성 및 시작
```javascript
// 1. 서버 생성
await serverControlAPI.createScalableServerWithConfig(taskId, flConfig);

// 2. FL 서버 시작
await serverControlAPI.startFLServer(taskId);

// 3. 연결 정보 확인
const info = await serverControlAPI.getConnectionInfo(taskId);
// FL 클라이언트는 info.server_address로 연결
```

### 2. 클라이언트 선택 방식 (기존 호환)
```javascript
// 1. 사용 가능한 클라이언트 조회
const clients = await serverControlAPI.getAvailableClients(taskId);

// 2. 클러스터 할당 (선택사항)
await serverControlAPI.assignCluster(taskId, deviceMac, clusterId);

// 3. 선택된 클라이언트와 FL 시작
const result = await serverControlAPI.startFLWithSelectedClients(
  taskId, 
  selectedDeviceMacs, 
  'scalable', 
  flConfig
);
```

### 3. 상태 모니터링
```javascript
// 전체 상태 요약
const summary = await serverControlAPI.getTaskSummary(taskId);

// 서버 상태
const status = await serverControlAPI.getServerStatus(taskId);

// 로그 확인
const logs = await serverControlAPI.getLogs(taskId, 100);
```

---

## 📋 핵심 API 엔드포인트

### Frontend → Backend
| 메서드 | 엔드포인트 | 설명 |
|--------|-----------|------|
| `createScalableServerWithConfig(taskId, config)` | `/create-scalable-with-config/:taskId` | FL 서버 생성 |
| `startFLServer(taskId)` | `/start-fl-server/:taskId` | FL 서버 시작 |
| `getAvailableClients(taskId)` | `/clients/:taskId` | 클라이언트 목록 |
| `startFLWithSelectedClients(taskId, devices, type, config)` | `/start-fl-with-clients/:taskId` | 선택 클라이언트 FL 시작 |
| `getTaskSummary(taskId)` | `/task-summary/:taskId` | 태스크 상태 요약 |
| `scaleResources(taskId, cpu, memory)` | `/scale/:taskId` | 리소스 스케일링 |

### Backend → FedOps-Server
| HTTP 메서드 | 엔드포인트 | 설명 |
|-------------|-----------|------|
| POST | `/web-control/create-scalable-server-with-config/:taskId` | FL 서버 생성 |
| POST | `/web-control/execute-command/:taskId` | 명령 실행 |
| GET | `/FLSe/GetAvailableClients/:taskId` | 클라이언트 목록 |
| POST | `/FLSe/StartFLWithSelectedClients/:taskId` | 선택 클라이언트 FL 시작 |
| GET | `/FLSe/GetTaskSummary/:taskId` | 태스크 상태 요약 |
| POST | `/web-control/scale-resources/:taskId` | 리소스 스케일링 |

### FL 클라이언트 → FedOps-Server (직접)
| HTTP 메서드 | 엔드포인트 | 설명 |
|-------------|-----------|------|
| PUT | `/FLSe/RegisterFLTask` | 클라이언트 등록 |
| GET | `/FLSe/info/:taskId/:deviceMac` | 클라이언트 상태 조회 |
| PUT | `/FLSe/FLSeUpdate/:taskId` | FL 서버 상태 업데이트 |

---

## 🔧 설정 객체

### FL Config
```javascript
const flConfig = {
  task_id: "my-fl-task",
  server_repo_addr: "https://github.com/gachon-CCLab/FedOps-Training-Server.git",
  data_type: "Image",           // "Image", "LLM", "Tabular"
  model_type: "CNN",            // "CNN", "ResNet", "BERT", etc.
  strategy: "FedAvg",           // "FedAvg", "FedProx", "FedNova"
  num_rounds: "10",
  num_epochs: "5",
  batch_size: "32",
  learning_rate: "0.001",
  client_per_round: "5",
  xai_enabled: "false",
  yaml_config: `
    # YAML 설정 내용
    model:
      type: CNN
      layers: [64, 128, 256]
    training:
      epochs: 5
      batch_size: 32
  `,
  strategy_params: {
    mu: 0.1,                    // FedProx용
    tau: 0.5                    // FedNova용
  },
  llm_params: {
    max_length: 512,
    model_name: "bert-base-uncased"
  },
  dataset_params: {
    dataset_name: "CIFAR10",
    num_classes: 10
  }
};
```

---

## 📊 응답 형식

### 성공 응답
```javascript
{
  success: true,
  data: any,
  message?: string,
  taskId?: string
}
```

### 에러 응답
```javascript
{
  success: false,
  error: string,
  status?: number
}
```

### 태스크 상태 응답
```javascript
{
  task_id: "my-fl-task",
  server_status: {
    status: "Ready",
    port: 40001,
    external_ip: "1.2.3.4"
  },
  client_stats: {
    total: 10,
    online: 8,
    training: 5,
    offline: 2
  },
  cluster_stats: {
    "1": {total: 3, online: 3, training: 2},
    "2": {total: 2, online: 2, training: 1}
  },
  connection_info: {
    external_ip: "1.2.3.4",
    port: 40001
  }
}
```

---

## 🐛 디버깅 팁

### 1. 서버 생성 실패
```javascript
// 상태 확인
const status = await serverControlAPI.getServerStatus(taskId);
console.log(status);

// 로그 확인
const logs = await serverControlAPI.getLogs(taskId, 50);
console.log(logs);
```

### 2. 클라이언트 연결 실패
```javascript
// 클라이언트 등록 상태 확인
const clients = await serverControlAPI.getAvailableClients(taskId);
console.log(clients);

// FL 서버 프로세스 상태 확인
const flStatus = await serverControlAPI.getFLServerStatus(taskId);
console.log(flStatus);
```

### 3. 연결 정보 확인
```javascript
// 연결 정보 조회
const info = await serverControlAPI.getConnectionInfo(taskId);
console.log(`FL Server: ${info.external_ip}:${info.port}`);
```

---

## 🔄 상태 전이

```
서버 생성: Initializing → Ready
FL 시작: Ready → FL Server Creating → FL Server Running
클라이언트: offline → online → training
라운드: FLSeReady=false → FLSeReady=true (라운드 완료)
```

---

## 🌐 네트워크 구조

```
클라이언트 (FL) ↔ [외부IP:포트] ↔ LoadBalancer ↔ FL Server Pod (8080)
       ↕                                                    ↕
   웹 클라이언트 ↔ Node.js ↔ Python FastAPI ↔ Kubernetes API
```

---

이 빠른 참조는 FedOps API의 핵심 사용법과 주요 워크플로우를 간결하게 정리한 것입니다. 상세한 정보는 `API_SPECIFICATION.md`를 참조하세요.