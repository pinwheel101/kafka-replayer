# Spark Data Preparation

Hive 테이블 데이터를 Kafka로 리플레이하는 두 가지 방식을 제공합니다.

## 방식 1: 직접 Kafka 전송 (권장 🌟)

HDFS 중간 저장 없이 Hive → Kafka 직접 전송

### 빌드

```bash
sbt assembly
```

### 실행

#### 시간 간격 제어 모드

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 20 \
  --executor-memory 8g \
  --executor-cores 4 \
  --class com.example.replayer.DirectKafkaReplayer \
  target/scala-2.12/spark-data-prep-assembly-1.0.0.jar \
  --source-table mydb.events \
  --target-date 2021-01-02 \
  --kafka-bootstrap kafka-service:9092 \
  --topic events-replay \
  --speed 1.0 \
  --batch-size 10000
```

#### 최대 속도 모드

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class com.example.replayer.DirectKafkaReplayer \
  target/scala-2.12/spark-data-prep-assembly-1.0.0.jar \
  --source-table mydb.events \
  --target-date 2021-01-02 \
  --kafka-bootstrap kafka-service:9092 \
  --topic events-replay \
  --max-speed
```

### 옵션

| 옵션 | 필수 | 설명 | 기본값 |
|------|------|------|--------|
| `--source-table` | O | Hive 테이블명 (예: mydb.events) | - |
| `--target-date` | O | 대상 날짜 (YYYY-MM-DD) | - |
| `--kafka-bootstrap` | O | Kafka 브로커 주소 | - |
| `--topic` | O | Kafka 토픽명 | - |
| `--speed` | X | 재생 속도 배수 (1.0 = 실시간) | 1.0 |
| `--batch-size` | X | 배치 크기 (시간 간격 제어 단위) | 10000 |
| `--max-speed` | X | 최대 속도 모드 (타이밍 무시) | false |

### 장점

- ✅ 단순함: 단일 Spark 작업
- ✅ 빠름: 중간 저장 없음
- ✅ 비용 절감: HDFS 스토리지 불필요

### 한계

- ⚠️ 타이밍 정밀도: 초 단위 (±수백ms)
- ⚠️ 재사용 불가: 재실행 시 Spark부터

---

## 방식 2: 2-Phase (중간 저장)

정밀한 타이밍 제어가 필요한 경우

### Phase 1: Parquet 준비

```bash
sbt assembly

spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class com.example.replayer.PrepareChunks \
  target/scala-2.12/spark-data-prep-assembly-1.0.0.jar \
  --source-table mydb.events \
  --target-date 2021-01-02 \
  --output-path hdfs:///replay/prepared/2021-01-02
```

### Phase 2: Kotlin 리플레이

```bash
cd ../kotlin-replayer

java -jar build/libs/kafka-replayer-1.0.0-all.jar \
  --mode time-synced \
  --hdfs-uri hdfs://namenode:8020 \
  --input-path /replay/prepared/2021-01-02 \
  --kafka-bootstrap kafka:9092 \
  --topic events-replay \
  --speed 1.0
```

### 장점

- ✅ 정밀 타이밍: ±1-5ms
- ✅ 재사용 가능: 여러 번 리플레이
- ✅ 검증 가능: 중간 데이터 확인

### 한계

- ⚠️ 복잡함: 두 단계 실행
- ⚠️ 느림: 중간 저장 I/O
- ⚠️ 비용: HDFS 스토리지 2배

---

## 선택 가이드

### Direct 방식 사용 (권장)
- [x] 시간 간격이 대략 유사하면 충분
- [x] 일회성 리플레이
- [x] 빠른 처리 우선

### 2-Phase 방식 사용
- [ ] 밀리초 단위 정밀 타이밍 필수
- [ ] 같은 데이터 반복 리플레이
- [ ] 중간 검증 필요

---

## Kubernetes 환경에서 실행

### Spark Operator 사용

```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: kafka-replayer
  namespace: default
spec:
  type: Scala
  mode: cluster
  image: your-registry/spark:3.5.0
  imagePullPolicy: IfNotPresent
  mainClass: com.example.replayer.DirectKafkaReplayer
  mainApplicationFile: "local:///opt/spark/jars/spark-data-prep-assembly-1.0.0.jar"
  arguments:
    - "--source-table"
    - "mydb.events"
    - "--target-date"
    - "2021-01-02"
    - "--kafka-bootstrap"
    - "kafka-service:9092"
    - "--topic"
    - "events-replay"
    - "--speed"
    - "1.0"
  sparkVersion: "3.5.0"
  restartPolicy:
    type: Never
  driver:
    cores: 2
    memory: "4g"
    serviceAccount: spark
  executor:
    cores: 4
    instances: 20
    memory: "8g"
```

### 실행

```bash
kubectl apply -f spark-replayer-job.yaml
```

---

## 성능 튜닝

### 배치 크기 조정

- 작은 배치 (1,000): 정밀한 타이밍, 느린 처리
- 큰 배치 (100,000): 빠른 처리, 낮은 정밀도

```bash
--batch-size 50000  # 5만 건 단위
```

### Executor 수 조정

```bash
--num-executors 50 \
--executor-memory 16g \
--executor-cores 8
```

### Kafka Producer 튜닝

```bash
--conf spark.kafka.producer.batch.size=65536 \
--conf spark.kafka.producer.linger.ms=5 \
--conf spark.kafka.producer.compression.type=snappy
```
