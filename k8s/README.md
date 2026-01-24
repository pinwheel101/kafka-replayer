# Kafka Replayer Kubernetes 배포 가이드

## 사전 요구사항

- Kubernetes 클러스터 (1.19+)
- kubectl CLI
- Docker 이미지 레지스트리 접근 권한
- 외부 HDFS 클러스터 접근 가능
- 클러스터 내부 Kafka (또는 외부 Kafka 접근 가능)

## 배포 단계

### 1. Docker 이미지 빌드 및 푸시

```bash
cd kotlin-replayer

# 이미지 빌드
docker build -t your-registry/kafka-replayer:1.0.0 .

# 레지스트리에 푸시
docker push your-registry/kafka-replayer:1.0.0
```

### 2. 환경 설정 수정

`k8s/deployment.yaml` 파일에서 환경변수 수정:

```yaml
env:
- name: HDFS_URI
  value: "hdfs://your-hdfs-namenode:8020"  # 실제 HDFS 주소
- name: HDFS_INPUT_PATH
  value: "/replay/prepared/2021-01-02"     # 실제 데이터 경로
- name: KAFKA_BOOTSTRAP_SERVERS
  value: "kafka-service:9092"              # 실제 Kafka 서비스
- name: KAFKA_TOPIC
  value: "events-replay"                   # 목적지 토픽
- name: REPLAY_MODE
  value: "TIME_SYNCED"                     # TIME_SYNCED or SEQUENTIAL
- name: REPLAY_SPEED
  value: "1.0"                             # 재생 속도
```

`k8s/kustomization.yaml`에서 이미지 경로 수정:

```yaml
images:
  - name: kafka-replayer
    newName: your-registry/kafka-replayer
    newTag: "1.0.0"
```

### 3. StorageClass 확인 (선택)

`k8s/pvc.yaml`에서 클러스터 환경에 맞는 StorageClass 지정:

```yaml
# AWS EKS
storageClassName: gp2

# GCP GKE
storageClassName: pd-ssd

# Azure AKS
storageClassName: managed-premium
```

### 4. Kubernetes 리소스 배포

#### 방법 1: kubectl apply

```bash
cd k8s

# 순서대로 배포
kubectl apply -f configmap.yaml
kubectl apply -f pvc.yaml
kubectl apply -f service.yaml
kubectl apply -f deployment.yaml
```

#### 방법 2: kustomize

```bash
cd k8s
kubectl apply -k .
```

### 5. 배포 확인

```bash
# Pod 상태 확인
kubectl get pods -l app=kafka-replayer

# 로그 확인
kubectl logs -f deployment/kafka-replayer

# 메트릭 확인
kubectl port-forward svc/kafka-replayer-service 9090:9090
curl http://localhost:9090/metrics
```

## 운영

### 재생 속도 변경

```bash
# Deployment 환경변수 수정
kubectl set env deployment/kafka-replayer REPLAY_SPEED=2.0

# 또는 직접 수정
kubectl edit deployment kafka-replayer
```

### 체크포인트에서 재시작

```bash
# 체크포인트가 PVC에 저장되어 있으므로
# Pod 재시작 시 자동으로 재개됩니다
kubectl rollout restart deployment/kafka-replayer
```

### 체크포인트 초기화

```bash
# 체크포인트 삭제 (처음부터 재시작)
kubectl exec deployment/kafka-replayer -- rm -f /app/checkpoints/kafka-replayer-checkpoint.json

# Pod 재시작
kubectl rollout restart deployment/kafka-replayer
```

### 스케일 조정

⚠️ **주의**: 단일 Pod로 실행하도록 설계되었습니다.

```bash
# 중지 (replicas=0)
kubectl scale deployment kafka-replayer --replicas=0

# 재시작 (replicas=1)
kubectl scale deployment kafka-replayer --replicas=1
```

### 로그 수집

```bash
# 실시간 로그
kubectl logs -f deployment/kafka-replayer

# 최근 100줄
kubectl logs --tail=100 deployment/kafka-replayer

# 특정 시간대 로그
kubectl logs --since=1h deployment/kafka-replayer
```

### 메트릭 모니터링

Prometheus가 설치되어 있다면 Service의 annotation으로 자동 스크래핑:

```yaml
annotations:
  prometheus.io/scrape: "true"
  prometheus.io/port: "9090"
  prometheus.io/path: "/metrics"
```

주요 메트릭:
- `replayer_events_sent_total`: 전송된 이벤트 수
- `replayer_events_failed_total`: 실패한 이벤트 수
- `replayer_current_chunk`: 현재 처리 중인 청크
- `replayer_timing_drift_ms`: 타이밍 오차

## 트러블슈팅

### Pod가 시작되지 않음

```bash
# Pod 상태 확인
kubectl describe pod -l app=kafka-replayer

# 일반적인 원인:
# 1. 이미지 pull 실패 -> imagePullSecrets 확인
# 2. PVC 바인딩 실패 -> StorageClass 확인
# 3. 리소스 부족 -> kubectl describe nodes
```

### HDFS 연결 실패

```bash
# Pod에서 HDFS 접근 테스트
kubectl exec -it deployment/kafka-replayer -- \
  sh -c "curl -f hdfs://external-hdfs-namenode:8020"

# DNS 확인
kubectl exec -it deployment/kafka-replayer -- nslookup external-hdfs-namenode
```

### Kafka 연결 실패

```bash
# Kafka 서비스 확인
kubectl get svc kafka-service

# 네트워크 정책 확인
kubectl get networkpolicies
```

### OOM (Out of Memory)

```bash
# Heap dump 확인
kubectl exec deployment/kafka-replayer -- ls -lh /app/checkpoints/*.hprof

# 메모리 제한 증가
kubectl set resources deployment kafka-replayer \
  --limits=memory=32Gi
```

## 삭제

```bash
# 모든 리소스 삭제
kubectl delete -k k8s/

# 또는 개별 삭제
kubectl delete deployment kafka-replayer
kubectl delete svc kafka-replayer-service
kubectl delete pvc kafka-replayer-checkpoint-pvc
kubectl delete configmap kafka-replayer-config
```

⚠️ **주의**: PVC 삭제 시 체크포인트 데이터도 함께 삭제됩니다.

## 고급 설정

### 멀티 리플레이어 실행 (청크 분할)

여러 날짜를 동시에 리플레이하려면:

```bash
# 2021-01-02용 리플레이어
kubectl apply -f k8s/
kubectl set env deployment/kafka-replayer HDFS_INPUT_PATH=/replay/prepared/2021-01-02

# 2021-01-03용 리플레이어 (별도 네임스페이스)
kubectl create ns replayer-20210103
kubectl apply -f k8s/ -n replayer-20210103
kubectl set env deployment/kafka-replayer -n replayer-20210103 \
  HDFS_INPUT_PATH=/replay/prepared/2021-01-03
```

### S3 사용 (HDFS 대신)

ConfigMap에서 S3 설정 추가 필요 (향후 지원 예정)
