# Kafka Time-Synced Replayer

HDFS/Hive에 저장된 이벤트 데이터를 원본 시간 간격을 유지하며 Kafka로 리플레이하는 시스템입니다.

## 아키텍처

```
┌─────────────────────────────────────────────────────────────────┐
│                    하이브리드 아키텍처                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Phase 1: Spark (데이터 준비)                                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  Hive Table ──▶ Spark SQL ──▶ 시간별 Parquet 청크       │   │
│  │  (ORC)          (정렬)        hdfs:///replay/prepared/   │   │
│  └─────────────────────────────────────────────────────────┘   │
│                              │                                   │
│                              ▼                                   │
│  Phase 2: Kotlin (정밀 리플레이)                                │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  Parquet 청크 ──▶ HashedWheelTimer ──▶ Kafka            │   │
│  │  (시간순 정렬됨)   (ms 정밀도)                           │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## 프로젝트 구조

```
kafka-replayer/
├── spark-data-prep/           # Phase 1: Spark 데이터 준비
│   ├── prepare_chunks.py      # PySpark 버전
│   ├── build.sbt              # Scala 빌드 설정
│   └── src/main/scala/
│       └── PrepareChunks.scala
│
├── kotlin-replayer/           # Phase 2: Kotlin 리플레이어
│   ├── build.gradle.kts
│   └── src/main/kotlin/
│       ├── Main.kt
│       ├── config/
│       ├── reader/
│       ├── scheduler/
│       ├── producer/
│       └── model/
│
└── scripts/                   # 실행 스크립트
    ├── run-spark-prep.sh
    └── run-replayer.sh
```

## 요구사항

### Spark 데이터 준비
- Apache Spark 3.x
- Hive Metastore 접근 권한
- HDFS 쓰기 권한

### Kotlin 리플레이어
- JDK 17+
- Gradle 8.x
- HDFS 읽기 권한
- Kafka 클러스터 접근

## 빠른 시작

### 1. Spark 데이터 준비

```bash
# PySpark 버전
cd spark-data-prep
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 20 \
  --executor-memory 8g \
  --executor-cores 4 \
  prepare_chunks.py \
  --source-table mydb.events \
  --target-date 2021-01-02 \
  --output-path hdfs:///replay/prepared/2021-01-02

# 또는 Scala 버전
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

### 2. Kotlin 리플레이어 빌드

```bash
cd kotlin-replayer
./gradlew shadowJar
```

### 3. 리플레이 실행

```bash
# 시간 동기화 모드 (원본 시간 간격 유지)
java -jar build/libs/kafka-replayer-1.0.0-all.jar \
  --mode time-synced \
  --hdfs-uri hdfs://namenode:8020 \
  --input-path /replay/prepared/2021-01-02 \
  --kafka-bootstrap kafka:9092 \
  --topic events-replay \
  --speed 1.0

# 순차 모드 (최대 속도)
java -jar build/libs/kafka-replayer-1.0.0-all.jar \
  --mode sequential \
  --hdfs-uri hdfs://namenode:8020 \
  --input-path /replay/prepared/2021-01-02 \
  --kafka-bootstrap kafka:9092 \
  --topic events-replay
```

## 리플레이 모드

### 1. 시간 동기화 모드 (time-synced)

원본 이벤트의 시간 간격을 유지하며 리플레이합니다.

- `--speed 1.0`: 원본 속도 (24시간 데이터 = 24시간 소요)
- `--speed 2.0`: 2배속 (24시간 데이터 = 12시간 소요)
- `--speed 0.5`: 0.5배속 (24시간 데이터 = 48시간 소요)

**타이밍 정밀도**: ~1-5ms (HashedWheelTimer 사용)

### 2. 순차 모드 (sequential)

시간 간격을 무시하고 최대 속도로 순서대로 적재합니다.

- `--delay-ms 0`: 지연 없이 최대 속도
- `--delay-ms 10`: 이벤트 간 10ms 지연

## 설정 파일

`kotlin-replayer/src/main/resources/application.yaml`:

```yaml
hdfs:
  uri: "hdfs://namenode:8020"
  user: "hadoop"

kafka:
  bootstrap-servers: "kafka1:9092,kafka2:9092,kafka3:9092"
  topic: "events-replay"
  acks: "all"
  batch-size: 65536
  linger-ms: 5
  buffer-memory: 134217728

replay:
  mode: "time-synced"  # time-synced | sequential
  speed-factor: 1.0
  chunk-hours: 1
  
metrics:
  enabled: true
  port: 9090
```

## 대용량 처리 (30억 건)

### 리소스 요구사항

| 구성 요소 | 최소 사양 | 권장 사양 |
|----------|----------|----------|
| Spark 데이터 준비 | 10 executors × 4GB | 20 executors × 8GB |
| Kotlin 리플레이어 | 16GB RAM, 4 cores | 32GB RAM, 8 cores |

### 청크 처리

30억 건을 1시간 단위 청크로 분할하여 처리:
- 청크당 약 1.25억 건
- 메모리 효율적 처리
- 청크 간 연속성 보장

## 모니터링

### Prometheus 메트릭 (포트 9090)

- `replayer_events_sent_total`: 전송된 이벤트 수
- `replayer_events_failed_total`: 실패한 이벤트 수
- `replayer_current_chunk`: 현재 처리 중인 청크
- `replayer_timing_drift_ms`: 타이밍 오차 (ms)

### 로그

```bash
# 상세 로그 활성화
java -jar kafka-replayer-1.0.0-all.jar \
  -Dlog.level=DEBUG \
  ...
```

## 장애 복구

### 체크포인트

리플레이어는 청크 단위로 체크포인트를 저장합니다:

```bash
# 체크포인트에서 재시작
java -jar kafka-replayer-1.0.0-all.jar \
  --resume \
  --checkpoint-path /tmp/replayer-checkpoint.json \
  ...
```

## 테스트

### 단위 테스트

```bash
cd kotlin-replayer
./gradlew test
```

### 통합 테스트 (Docker)

```bash
cd kotlin-replayer
docker-compose -f docker-compose.test.yaml up -d
./gradlew integrationTest
```

## 라이선스

MIT License
