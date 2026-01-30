# Trace Table Integration Guide

## 개요

`sql/schema.sql`에 정의된 반도체 제조 trace 테이블을 Kafka Replayer에 통합했습니다.

## 테이블 구조

### trace 테이블
반도체 공정 추적 데이터를 저장하는 메인 테이블입니다.

**주요 컬럼:**
- `ts`: 이벤트 발생 시각 (Timestamp)
- `lot_id`: Lot 식별자 (Kafka 키로 사용)
- `eqp_id`: 장비 ID
- `module_name`: 모듈명
- `slot_id`: 슬롯 ID
- `product_id`: 제품 ID
- `step_name`: 공정 단계명
- `param_name[]`: 파라미터 이름 배열
- `param_value[]`: 파라미터 값 배열
- `dt`: 날짜 파티션 (YYYY-MM-DD)
- `fab`: FAB 파티션

## DirectKafkaReplayer 변경 사항

### 1. 자동 컬럼 감지
키 컬럼과 타임스탬프 컬럼을 자동으로 감지합니다.

**키 컬럼 우선순위:**
1. `lot_id` (trace 테이블용)
2. `event_key` (기존 events 테이블용)
3. `eqp_id`
4. 첫 번째 컬럼

**타임스탬프 컬럼 우선순위:**
1. `ts` (trace 테이블용)
2. `event_time` (기존 events 테이블용)
3. `create_dtts`
4. `start_dtts`
5. 첫 번째 timestamp/long 타입 컬럼

### 2. 파티션 컬럼 기본값 변경
```scala
excludeColumns: Seq[String] = Seq("dt", "fab")  // 기존: Seq("dt")
```

### 3. Timestamp 타입 자동 변환
- `TimestampType` → milliseconds (Long) 자동 변환
- `LongType` → 그대로 사용

## 사용 예시

### 1. Trace 테이블 Binary 직렬화
```bash
spark-submit \
  --class com.example.replayer.DirectKafkaReplayer \
  spark-data-prep-assembly-1.0.0.jar \
  --source-table "mydb.trace" \
  --target-date "2024-01-01" \
  --kafka-bootstrap "localhost:9092" \
  --topic "trace-replay" \
  --speed 1.0
```

**자동 감지:**
- 키 컬럼: `lot_id`
- 타임스탬프: `ts`
- 제외 컬럼: `dt`, `fab`

### 2. Trace 테이블 Avro 직렬화
```bash
spark-submit \
  --class com.example.replayer.DirectKafkaReplayer \
  spark-data-prep-assembly-1.0.0.jar \
  --source-table "mydb.trace" \
  --target-date "2024-01-01" \
  --kafka-bootstrap "localhost:9092" \
  --topic "trace-replay-avro" \
  --serialization-format avro \
  --schema-registry-url "http://localhost:8080" \
  --schema-name "trace.value"
```

### 3. 커스텀 키 컬럼 지정
```bash
spark-submit \
  --class com.example.replayer.DirectKafkaReplayer \
  spark-data-prep-assembly-1.0.0.jar \
  --source-table "mydb.trace" \
  --target-date "2024-01-01" \
  --kafka-bootstrap "localhost:9092" \
  --topic "trace-replay" \
  --key-column "eqp_id" \
  --timestamp-column "create_dtts"
```

### 4. 최대 속도 모드 (타이밍 무시)
```bash
spark-submit \
  --class com.example.replayer.DirectKafkaReplayer \
  spark-data-prep-assembly-1.0.0.jar \
  --source-table "mydb.trace" \
  --target-date "2024-01-01" \
  --kafka-bootstrap "localhost:9092" \
  --topic "trace-replay" \
  --max-speed
```

## 테스트 데이터

### PostgreSQL 테스트 데이터 생성
```bash
cd docker
docker-compose up -d postgres
docker exec -i kafka-replayer-postgres psql -U test -d testdb < init-trace-data.sql
```

### 테스트 데이터 확인
```sql
-- 총 이벤트 수
SELECT COUNT(*) FROM trace;

-- Lot별 이벤트 수
SELECT lot_id, COUNT(*) as event_count
FROM trace
GROUP BY lot_id
ORDER BY lot_id;

-- 날짜별 통계
SELECT dt, fab, COUNT(*) as cnt
FROM trace
GROUP BY dt, fab
ORDER BY dt, fab;

-- 파라미터 배열 확인
SELECT
    lot_id,
    eqp_id,
    ts,
    array_length(param_name, 1) as param_count,
    param_name,
    param_value
FROM trace
LIMIT 5;
```

## Array 타입 처리

Spark는 PostgreSQL의 array 타입을 자동으로 `ArrayType`으로 변환합니다.

**직렬화 시:**
- Binary 직렬화: Array가 JSON 문자열로 변환됨
- Avro 직렬화: Avro array 타입으로 직렬화됨

**Avro 스키마 예시:**
```json
{
  "type": "record",
  "name": "TraceEvent",
  "fields": [
    {"name": "lot_id", "type": "string"},
    {"name": "eqp_id", "type": "string"},
    {"name": "param_name", "type": {"type": "array", "items": "string"}},
    {"name": "param_value", "type": {"type": "array", "items": "string"}}
  ]
}
```

## 주의 사항

1. **Array 필드**: trace 테이블의 array 필드는 Spark의 `ArrayType`으로 처리됩니다.
2. **타임스탬프 정밀도**: PostgreSQL의 timestamp는 microsecond 정밀도를 가질 수 있지만, Kafka replay는 millisecond 단위로 변환됩니다.
3. **메모리 사용량**: Array 필드가 많으면 메모리 사용량이 증가할 수 있습니다. `--batch-size` 조정이 필요할 수 있습니다.
4. **파티션 필터링**: `dt`와 `fab` 컬럼은 기본적으로 Kafka 메시지에서 제외됩니다. 포함하려면 `--exclude-columns` 옵션으로 지정하세요.

## 커맨드 라인 옵션

```
--source-table <table>          Hive/Spark SQL 테이블명 (필수)
--target-date <YYYY-MM-DD>      대상 날짜 (필수)
--kafka-bootstrap <servers>     Kafka bootstrap servers (필수)
--topic <topic>                 Kafka 토픽명 (필수)
--speed <factor>                재생 속도 배율 (기본값: 1.0)
--batch-size <size>             배치 크기 (기본값: 10000)
--max-speed                     최대 속도 모드
--serialization-format <fmt>    직렬화 포맷: binary, avro (기본값: binary)
--schema-registry-url <url>     Apicurio Schema Registry URL
--schema-name <name>            스키마 이름 (기본값: <table>.value)
--key-column <column>           Kafka 키 컬럼 (자동 감지)
--timestamp-column <column>     타임스탬프 컬럼 (자동 감지)
--exclude-columns <col1,col2>   제외할 컬럼들 (기본값: dt,fab)
```

## 트러블슈팅

### 1. 키 컬럼을 찾을 수 없음
```
Error: Key column 'xxx' not found
```
**해결:** `--key-column` 옵션으로 명시적 지정

### 2. 타임스탬프 컬럼을 찾을 수 없음
```
Error: No timestamp column found
```
**해결:** `--timestamp-column` 옵션으로 명시적 지정

### 3. Array 필드 직렬화 오류
Avro 직렬화 시 스키마가 array 타입을 지원하는지 확인하세요.

## 성능 튜닝

### 대용량 데이터 처리
```bash
spark-submit \
  --class com.example.replayer.DirectKafkaReplayer \
  --driver-memory 4g \
  --executor-memory 8g \
  --conf spark.sql.shuffle.partitions=200 \
  spark-data-prep-assembly-1.0.0.jar \
  --source-table "mydb.trace" \
  --target-date "2024-01-01" \
  --kafka-bootstrap "localhost:9092" \
  --topic "trace-replay" \
  --batch-size 5000 \
  --max-speed
```

### 파티션 수 조정
```bash
--conf spark.sql.shuffle.partitions=<N>
```
- 기본값: 200
- 데이터 크기에 따라 조정 (각 파티션당 128MB 권장)
