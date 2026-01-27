# Kafka Direct Replayer

Hive 테이블의 이벤트 데이터를 시간 간격을 유지하며 Kafka로 리플레이하는 Spark 애플리케이션입니다.

## 아키텍처

```
┌─────────────────────────────────────────────────────────────┐
│                  Spark Direct Replay                         │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  Hive Table (ORC)                                           │
│       ↓                                                      │
│  Spark SQL (읽기 + 정렬)                                     │
│       ↓                                                      │
│  Batch Control (시간 간격 제어)                              │
│       ↓                                                      │
│  Kafka Producer (비동기 전송)                                │
│       ↓                                                      │
│  Kafka Topic                                                 │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### 특징

- ✅ **단순함**: 단일 Spark 작업으로 완료
- ✅ **빠름**: 중간 저장소(HDFS) 불필요, I/O 50% 감소
- ✅ **비용 절감**: 스토리지 비용 50% 절감
- ✅ **시간 간격 제어**: 대략적인 원본 시간 간격 유지 (초 단위)
- ✅ **배속 조절**: 1x, 2x, 10x 등 자유롭게 조절 가능
- ✅ **대용량 처리**: 30억 건 이상 데이터 처리 가능

## 프로젝트 구조

```
kafka-replayer/
├── spark-data-prep/              # Spark 애플리케이션
│   ├── src/main/scala/
│   │   └── com/example/replayer/
│   │       └── DirectKafkaReplayer.scala
│   ├── build.sbt
│   └── README.md
│
├── scripts/                      # 실행 스크립트
│   ├── run-replay.sh
│   └── replay.env.example
│
└── ARCHITECTURE_COMPARISON.md    # 아키텍처 비교 문서
```

## 요구사항

- Apache Spark 3.x
- Scala 2.12
- sbt 1.9+
- Hive Metastore 접근 권한
- Kafka 클러스터 접근

## 빠른 시작

### 1. 빌드

```bash
cd spark-data-prep
sbt assembly
```

빌드 결과: `target/scala-2.12/spark-data-prep-assembly-1.0.0.jar`

### 2. 환경 설정

```bash
cd scripts
cp replay.env.example replay.env
vi replay.env  # 환경에 맞게 수정
```

### 3. 실행

#### 방법 1: 환경변수 사용

```bash
source replay.env
./run-replay.sh
```

#### 방법 2: 직접 실행

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 20 \
  --executor-memory 8g \
  --executor-cores 4 \
  --class com.example.replayer.DirectKafkaReplayer \
  spark-data-prep/target/scala-2.12/spark-data-prep-assembly-1.0.0.jar \
  --source-table mydb.events \
  --target-date 2021-01-02 \
  --kafka-bootstrap kafka-service:9092 \
  --topic events-replay \
  --speed 1.0
```

## 사용법

### 리플레이 모드

#### 1. 시간 간격 제어 모드 (기본)

원본 이벤트 간의 시간 간격을 대략적으로 유지하며 리플레이합니다.

```bash
spark-submit \
  --class com.example.replayer.DirectKafkaReplayer \
  target/scala-2.12/spark-data-prep-assembly-1.0.0.jar \
  --source-table mydb.events \
  --target-date 2021-01-02 \
  --kafka-bootstrap kafka:9092 \
  --topic events-replay \
  --speed 1.0              # 1.0 = 실시간, 2.0 = 2배속, 10.0 = 10배속
  --batch-size 10000       # 배치 크기 (시간 제어 단위)
```

**타이밍 정밀도**: 초 단위 (±수백ms)

#### 2. 최대 속도 모드

시간 간격을 무시하고 최대 속도로 전송합니다.

```bash
spark-submit \
  --class com.example.replayer.DirectKafkaReplayer \
  target/scala-2.12/spark-data-prep-assembly-1.0.0.jar \
  --source-table mydb.events \
  --target-date 2021-01-02 \
  --kafka-bootstrap kafka:9092 \
  --topic events-replay \
  --max-speed
```

## Serialization Formats

Kafka Replayer는 다양한 직렬화 포맷을 지원하며 Apicurio Schema Registry와 통합됩니다.

### Binary (기본 - 하위 호환성)

기존 방식으로 `payload` 컬럼을 단순 binary로 변환합니다.

```bash
spark-submit \
  --class com.example.replayer.DirectKafkaReplayer \
  target/scala-2.12/spark-data-prep-assembly-1.0.0.jar \
  --source-table mydb.events \
  --target-date 2021-01-02 \
  --kafka-bootstrap kafka:9092 \
  --topic events-replay \
  --serialization-format binary \
  --max-speed
```

**요구사항**: Hive 테이블에 `payload` 컬럼 필수

### Avro (새로운 기능)

전체 row를 Avro 포맷으로 직렬화하고 Schema Registry에 스키마를 등록합니다.

```bash
spark-submit \
  --class com.example.replayer.DirectKafkaReplayer \
  target/scala-2.12/spark-data-prep-assembly-1.0.0.jar \
  --source-table mydb.events \
  --target-date 2021-01-02 \
  --kafka-bootstrap kafka:9092 \
  --topic events-replay \
  --serialization-format avro \
  --schema-registry-url http://apicurio:8080/apis/registry/v2 \
  --max-speed
```

**특징**:
- 전체 row를 하나의 Avro 메시지로 직렬화
- Schema Registry에 자동으로 스키마 등록
- 타입 안전성과 스키마 진화 지원

### 고급 옵션

```bash
# 커스텀 스키마 이름 지정
--schema-name my.custom.schema

# 특정 컬럼을 key로 사용
--key-column event_id

# value에서 제외할 컬럼 지정
--exclude-columns dt,partition,internal_field
```

### 타입 매핑 (Spark → Avro)

| Spark 타입 | Avro 타입 |
|-----------|-----------|
| StringType | string |
| LongType | long |
| IntegerType | int |
| DoubleType | double |
| BooleanType | boolean |
| TimestampType | long (milliseconds) |
| Other types | string (fallback) |

### 스키마 네이밍 규칙

기본 스키마 이름: `<table_name>.value`

예시: `mydb.events` → 스키마 이름: `events.value`

### 커맨드 라인 옵션

#### 기본 옵션

| 옵션 | 필수 | 설명 | 기본값 |
|------|------|------|--------|
| `--source-table` | O | Hive 테이블명 (예: mydb.events) | - |
| `--target-date` | O | 대상 날짜 (YYYY-MM-DD) | - |
| `--kafka-bootstrap` | O | Kafka 브로커 주소 | - |
| `--topic` | O | Kafka 토픽명 | - |
| `--speed` | X | 재생 속도 배수 | 1.0 |
| `--batch-size` | X | 배치 크기 (이벤트 수) | 10000 |
| `--max-speed` | X | 최대 속도 모드 플래그 | false |

#### Serialization 옵션

| 옵션 | 필수 | 설명 | 기본값 |
|------|------|------|--------|
| `--serialization-format` | X | 직렬화 포맷: binary, avro | binary |
| `--schema-registry-url` | Avro 사용시 O | Apicurio Schema Registry URL | - |
| `--schema-name` | X | 스키마 이름 (기본: <table>.value) | - |
| `--key-column` | X | Kafka key로 사용할 컬럼 | event_key |
| `--exclude-columns` | X | value에서 제외할 컬럼 (쉼표 구분) | dt |

## 환경변수 설정

`scripts/replay.env` 파일을 통해 설정:

```bash
# 데이터 소스
export SOURCE_TABLE="mydb.events"
export TARGET_DATE="2021-01-02"

# Kafka
export KAFKA_BOOTSTRAP="kafka-service:9092"
export KAFKA_TOPIC="events-replay"

# 리플레이 설정
export SPEED="2.0"           # 2배속
export BATCH_SIZE="10000"
export MAX_SPEED="false"

# Spark 리소스
export NUM_EXECUTORS="20"
export EXECUTOR_MEMORY="8g"
export EXECUTOR_CORES="4"
```

## 대용량 처리 (30억 건)

### 리소스 요구사항

| 구성 요소 | 최소 사양 | 권장 사양 |
|----------|----------|----------|
| Executors | 10 × 4GB | 20 × 8GB |
| Driver | 2GB | 4GB |
| 총 메모리 | ~40GB | ~160GB |

### 예상 처리 시간 (30억 건 기준)

| 모드 | 처리 시간 |
|------|----------|
| 1x 속도 (실시간) | ~25시간 |
| 2x 속도 | ~12.5시간 |
| 10x 속도 | ~3시간 |
| 최대 속도 | ~1.5시간 |

*실제 시간은 클러스터 성능과 네트워크 대역폭에 따라 달라집니다.*

## Kubernetes 환경 실행

### Spark Operator 사용

```bash
kubectl apply -f k8s/spark-replayer-job.yaml
```

`k8s/spark-replayer-job.yaml` 예시:

```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: kafka-replayer-20210102
  namespace: default
spec:
  type: Scala
  mode: cluster
  image: your-registry/spark:3.5.0
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
    - "2.0"
  sparkVersion: "3.5.0"
  driver:
    cores: 2
    memory: "4g"
  executor:
    cores: 4
    instances: 20
    memory: "8g"
```

자세한 내용은 [spark-data-prep/README.md](spark-data-prep/README.md)를 참조하세요.

## 테스트

### 로컬 테스트 (Docker Compose)

Docker를 사용하여 빠르게 로컬에서 테스트할 수 있습니다.

```bash
# 테스트 환경 시작
cd docker
./test-local.sh start

# 서비스 상태 확인
./test-local.sh status

# 테스트 완료 후 정리
./test-local.sh stop
```

**포함된 서비스:**
- Kafka (localhost:9092)
- Apicurio Schema Registry (http://localhost:8080)
- Kafka UI (http://localhost:8090)
- PostgreSQL with test data (localhost:5432)

### 통합 테스트 (Testcontainers)

자동화된 통합 테스트 실행:

```bash
cd spark-data-prep

# 모든 테스트 실행
sbt test

# 특정 테스트만 실행
sbt "testOnly *IntegrationTest"
```

**요구사항:** Docker Desktop 실행 중

상세한 테스트 가이드는 [TESTING.md](TESTING.md)를 참조하세요.

## 모니터링

### Spark UI

```bash
# Local mode
http://localhost:4040

# YARN
yarn application -list
# Spark UI 주소 확인 후 접속
```

### 로그 확인

```bash
# YARN 로그
yarn logs -applicationId application_xxx

# Kubernetes 로그
kubectl logs spark-replayer-driver
```

## 성능 튜닝

### 1. 배치 크기 조정

```bash
--batch-size 50000  # 더 큰 배치 = 빠른 처리, 낮은 타이밍 정밀도
--batch-size 1000   # 작은 배치 = 느린 처리, 높은 타이밍 정밀도
```

### 2. Executor 수 증가

```bash
--num-executors 50 \
--executor-memory 16g \
--executor-cores 8
```

### 3. Kafka Producer 튜닝

```bash
--conf spark.kafka.producer.batch.size=65536 \
--conf spark.kafka.producer.linger.ms=5 \
--conf spark.kafka.producer.compression.type=snappy \
--conf spark.kafka.producer.acks=1  # 성능 우선 시
```

### 4. Spark 설정 최적화

```bash
--conf spark.sql.adaptive.enabled=true \
--conf spark.sql.adaptive.coalescePartitions.enabled=true \
--conf spark.sql.shuffle.partitions=200
```

## 트러블슈팅

### OOM (Out of Memory)

```bash
# Executor 메모리 증가
--executor-memory 16g

# Batch size 감소
--batch-size 5000
```

### Kafka 연결 실패

```bash
# 네트워크 확인
telnet kafka-service 9092

# DNS 확인
nslookup kafka-service
```

### Hive 테이블 읽기 실패

```bash
# Metastore 연결 확인
spark-shell --conf spark.sql.catalogImplementation=hive

# 테이블 존재 확인
spark.sql("SHOW TABLES").show()
spark.sql("SELECT COUNT(*) FROM mydb.events WHERE dt='2021-01-02'").show()
```

## 비교: 기존 2-Phase vs Direct

자세한 아키텍처 비교는 [ARCHITECTURE_COMPARISON.md](ARCHITECTURE_COMPARISON.md)를 참조하세요.

| 항목 | Direct (현재) | 2-Phase (이전) |
|------|---------------|----------------|
| 복잡도 | 단순 | 복잡 |
| 처리 시간 | 빠름 | 느림 |
| 스토리지 | 50% 절감 | 2배 필요 |
| 타이밍 정밀도 | 초 단위 | ms 단위 |
| 재사용성 | 불가 | 가능 |

## 라이선스

MIT License

## 기여

이슈 및 Pull Request를 환영합니다!
