# Kafka Replayer - Testing Guide

로컬에서 Kafka Replayer를 빠르게 테스트하는 방법

## 🚀 빠른 시작

### 사전 요구사항

- Docker Desktop 설치 및 실행 중
- sbt 설치 (테스트용)
- (선택) Spark 로컬 설치

### 1. Docker Compose로 테스트 환경 시작

```bash
cd docker
./test-local.sh start
```

이 명령어는 다음을 실행합니다:
- Kafka (포트 9092)
- Apicurio Schema Registry (포트 8080)
- Kafka UI (포트 8090) - 모니터링용
- PostgreSQL (포트 5432) - 테스트 데이터용

### 2. 서비스 상태 확인

```bash
./test-local.sh status
```

### 3. 웹 UI 접속

**Kafka UI**: http://localhost:8090
- 토픽, 메시지, 컨슈머 그룹 모니터링

**Apicurio Registry**: http://localhost:8080
- 등록된 스키마 확인

### 4. 테스트 데이터 확인

PostgreSQL에 자동으로 200개의 테스트 이벤트가 생성됩니다:

```bash
docker exec -it kafka-replayer-postgres psql -U test -d testdb

# 데이터 확인
SELECT dt, COUNT(*) FROM events GROUP BY dt;
SELECT * FROM events LIMIT 5;
```

## 🧪 테스트 방법

### 방법 1: Testcontainers (추천)

자동화된 통합 테스트를 실행합니다.

```bash
cd spark-data-prep
sbt test
```

**특징:**
- ✅ Docker만 있으면 실행 가능
- ✅ 자동으로 Kafka, Schema Registry 시작
- ✅ 테스트 후 자동 정리
- ✅ CI/CD 통합 가능

**테스트 내용:**
- Serialization Strategy 생성 검증
- Binary 직렬화 테스트
- Kafka 메시지 쓰기/읽기
- Schema 이름 자동 생성
- 에러 처리 검증

### 방법 2: Docker Compose + 수동 테스트

실제 환경과 유사한 구성에서 수동 테스트

#### 2-1. 환경 시작

```bash
cd docker
./test-local.sh start
```

#### 2-2. 테스트 토픽 생성 (자동 생성됨)

토픽이 자동으로 생성되지만, 수동으로 만들 수도 있습니다:

```bash
docker exec kafka-replayer-kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create --topic my-test-topic \
  --partitions 3 --replication-factor 1
```

#### 2-3. Binary 모드 테스트

PostgreSQL 데이터를 CSV로 추출:

```bash
# 현재 날짜 확인
TODAY=$(docker exec kafka-replayer-postgres psql -U test -d testdb -t -c "SELECT DISTINCT dt FROM events ORDER BY dt DESC LIMIT 1;" | xargs)

# 데이터 추출
docker exec kafka-replayer-postgres psql -U test -d testdb -c \
  "COPY (SELECT event_key, event_time, user_id, event_type, payload, dt FROM events WHERE dt='$TODAY') TO STDOUT WITH CSV HEADER" \
  > /tmp/test-events.csv

echo "Test data exported to /tmp/test-events.csv"
```

**제한사항**: Binary 모드는 `payload` 컬럼이 필요하므로, Spark + Hive 환경이 필요합니다.

#### 2-4. Avro 모드 테스트 (Spark 필요)

Spark가 로컬에 설치되어 있다면:

```bash
spark-submit \
  --master local[2] \
  --class com.example.replayer.DirectKafkaReplayer \
  ../spark-data-prep/target/scala-2.12/spark-data-prep-assembly-1.0.0.jar \
  --source-table events \
  --target-date $TODAY \
  --kafka-bootstrap localhost:9092 \
  --topic test-avro \
  --serialization-format avro \
  --schema-registry-url http://localhost:8080/apis/registry/v2 \
  --max-speed
```

**참고**: PostgreSQL은 Hive가 아니므로, 실제로는 Spark에서 PostgreSQL JDBC로 읽어야 합니다.

#### 2-5. 결과 확인

**Kafka 메시지 확인:**

```bash
# Binary 토픽
docker exec kafka-replayer-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic test-binary \
  --from-beginning \
  --max-messages 5

# Avro 토픽 (바이너리로 보임)
docker exec kafka-replayer-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic test-avro \
  --from-beginning \
  --max-messages 5
```

**Schema Registry 확인:**

```bash
# 등록된 스키마 목록
curl http://localhost:8080/apis/registry/v2/search/artifacts | jq

# 특정 스키마 조회
curl http://localhost:8080/apis/registry/v2/groups/default/artifacts/events.value | jq
```

**Kafka UI로 확인:**

브라우저에서 http://localhost:8090 접속
- Topics 탭에서 메시지 확인
- Schema Registry 탭에서 스키마 확인

## 🔧 유용한 명령어

### 서비스 관리

```bash
# 서비스 시작
./test-local.sh start

# 서비스 중지
./test-local.sh stop

# 서비스 재시작
./test-local.sh restart

# 로그 확인 (follow 모드)
./test-local.sh logs

# 서비스 상태 확인
./test-local.sh status

# 모든 데이터 삭제 및 정리
./test-local.sh clean
```

### Kafka 명령어

```bash
# 토픽 목록
docker exec kafka-replayer-kafka kafka-topics --bootstrap-server localhost:9092 --list

# 토픽 상세 정보
docker exec kafka-replayer-kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic test-avro

# 컨슈머 그룹 목록
docker exec kafka-replayer-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --list

# 메시지 개수 확인
docker exec kafka-replayer-kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 --topic test-avro --time -1
```

### PostgreSQL 명령어

```bash
# psql 접속
docker exec -it kafka-replayer-postgres psql -U test -d testdb

# 데이터 조회
docker exec kafka-replayer-postgres psql -U test -d testdb -c "SELECT * FROM events LIMIT 10;"

# 통계
docker exec kafka-replayer-postgres psql -U test -d testdb -c \
  "SELECT dt, event_type, COUNT(*) FROM events GROUP BY dt, event_type ORDER BY dt, event_type;"
```

### Schema Registry 명령어

```bash
# 모든 스키마 목록
curl http://localhost:8080/apis/registry/v2/search/artifacts | jq '.artifacts[].id'

# 스키마 상세 조회
curl http://localhost:8080/apis/registry/v2/groups/default/artifacts/events.value | jq

# 스키마 버전 목록
curl http://localhost:8080/apis/registry/v2/groups/default/artifacts/events.value/versions | jq
```

## 🐛 트러블슈팅

### Docker 컨테이너가 시작되지 않음

```bash
# Docker Desktop이 실행 중인지 확인
docker ps

# 포트 충돌 확인 (9092, 8080, 5432)
lsof -i :9092
lsof -i :8080
lsof -i :5432

# 이전 컨테이너 정리
cd docker
docker-compose down -v
```

### Kafka 연결 실패

```bash
# Kafka 상태 확인
docker exec kafka-replayer-kafka kafka-broker-api-versions --bootstrap-server localhost:9092

# 로그 확인
docker logs kafka-replayer-kafka --tail 50
```

### Schema Registry 연결 실패

```bash
# Health check
curl http://localhost:8080/health

# 로그 확인
docker logs kafka-replayer-apicurio --tail 50
```

### 테스트 데이터가 없음

```bash
# PostgreSQL 재초기화
cd docker
docker-compose down -v
docker-compose up -d postgres

# 데이터 확인
docker exec kafka-replayer-postgres psql -U test -d testdb -c "SELECT COUNT(*) FROM events;"
```

### Testcontainers 테스트 실패

```bash
# Docker가 실행 중인지 확인
docker ps

# Testcontainers 로그 레벨 증가
export TESTCONTAINERS_RYUK_DISABLED=true
sbt "testOnly *IntegrationTest -- -oF"

# Docker 리소스 정리
docker system prune -f
```

## 📊 성능 테스트

### 소규모 테스트 (100 이벤트)

기본 테스트 데이터로 충분합니다.

```bash
# 현재 테스트 데이터
docker exec kafka-replayer-postgres psql -U test -d testdb -c \
  "SELECT COUNT(*) FROM events;"
```

### 대규모 테스트 (10,000+ 이벤트)

추가 테스트 데이터 생성:

```bash
docker exec kafka-replayer-postgres psql -U test -d testdb -c "
INSERT INTO events (event_key, event_time, user_id, event_type, payload, dt)
SELECT
    'event_' || generate_series,
    extract(epoch from (NOW() + (generate_series || ' seconds')::interval)) * 1000,
    'user_' || (random() * 100)::int,
    CASE (random() * 3)::int
        WHEN 0 THEN 'click'
        WHEN 1 THEN 'view'
        WHEN 2 THEN 'purchase'
        ELSE 'other'
    END,
    '{\"data\": \"sample_' || generate_series || '\", \"value\": ' || (random() * 100)::int || '}',
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD')
FROM generate_series(1, 10000);
"

# 확인
docker exec kafka-replayer-postgres psql -U test -d testdb -c \
  "SELECT COUNT(*) FROM events;"
```

## 🎯 다음 단계

로컬 테스트 완료 후:

1. **실제 Hive 환경 테스트**
   - 프로덕션과 유사한 Hive 클러스터에서 테스트
   - 대용량 데이터 (GB ~ TB) 처리 검증

2. **성능 벤치마크**
   - Binary vs Avro 처리 시간 비교
   - 메모리 사용량 측정
   - 네트워크 대역폭 분석

3. **Protobuf/JSON Schema 구현**
   - NEXT_STEPS.md 참조

4. **CI/CD 통합**
   - GitHub Actions에 Testcontainers 테스트 추가
   - 자동 빌드 및 배포

## 📚 참고 자료

- [Testcontainers Documentation](https://www.testcontainers.org/)
- [Kafka Testing Best Practices](https://kafka.apache.org/documentation/#testing)
- [Apicurio Registry](https://www.apicur.io/registry/)
- Docker Compose 파일: `docker/docker-compose.yml`
- 통합 테스트: `spark-data-prep/src/test/scala/com/example/replayer/`
