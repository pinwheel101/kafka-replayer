# Kafka Replayer - Next Steps & TODO List

마지막 업데이트: 2026-01-26

## ✅ Phase 1 완료 (Avro 직렬화)

- [x] Serialization Strategy 패턴 구현
- [x] Binary 직렬화 (하위 호환)
- [x] Avro 직렬화 구현
- [x] Apicurio Schema Registry 통합
- [x] CLI 옵션 추가
- [x] 자동 스키마 생성 및 등록
- [x] 빌드 및 컴파일 검증
- [x] 기본 문서화 (README.md)

## 📋 Phase 2: Protobuf 직렬화 (예정)

### 설계 결정
- **Static 코드 생성 방식 사용** (사용자 요구사항)
- .proto 파일을 프로젝트에 포함하여 컴파일 타임에 클래스 생성
- 동적 메시지 생성보다 높은 성능

### 구현 작업

#### 2.1 빌드 설정
- [ ] `build.sbt`에 Protobuf 의존성 추가
  ```scala
  "com.google.protobuf" % "protobuf-java" % "3.25.1"
  "com.google.protobuf" % "protobuf-java-util" % "3.25.1"
  ```
- [ ] sbt-protoc 플러그인 추가 (`project/plugins.sbt`)
  ```scala
  addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.6")
  ```
- [ ] Protobuf 컴파일 설정 추가
  ```scala
  PB.targets in Compile := Seq(
    scalapb.gen() -> (sourceManaged in Compile).value / "scalapb"
  )
  ```

#### 2.2 .proto 파일 템플릿
- [ ] `src/main/protobuf/` 디렉토리 생성
- [ ] 샘플 .proto 파일 작성
  ```protobuf
  syntax = "proto3";
  package com.example.replayer;

  // 런타임에 Hive 스키마에서 자동 생성하는 로직 필요
  message DynamicRecord {
    map<string, string> fields = 1;
  }
  ```
- [ ] 또는 Hive 스키마에서 .proto 파일을 자동 생성하는 유틸리티 구현

#### 2.3 ProtobufSerializationStrategy 구현
- [ ] `ProtobufSerializationStrategy.scala` 파일 생성
- [ ] Hive 스키마 → .proto 파일 생성기 구현
- [ ] 생성된 Protobuf 클래스로 Row 직렬화
- [ ] Apicurio Schema Registry에 .proto 스키마 등록
- [ ] 타입 매핑 구현 (Spark → Protobuf)

#### 2.4 CLI 및 문서화
- [ ] `--serialization-format protobuf` 지원 추가
- [ ] SerializationFactory에서 Protobuf 전략 생성
- [ ] README.md에 Protobuf 사용법 추가
- [ ] .proto 파일 관리 가이드 문서 작성

**예상 소요 시간**: 2-3일

**주요 도전 과제**:
- 런타임에 동적으로 .proto 파일 생성할지, 사전 정의할지 결정
- Nested 타입 (Struct, Array, Map) 처리 방법

---

## 📋 Phase 3: JSON Schema 직렬화 (예정)

### 구현 작업

#### 3.1 빌드 설정
- [ ] JSON Schema 의존성 추가
  ```scala
  "com.github.victools" % "jsonschema-generator" % "4.35.0"
  ```

#### 3.2 JsonSchemaSerializationStrategy 구현
- [ ] `JsonSchemaSerializationStrategy.scala` 파일 생성
- [ ] Hive 스키마 → JSON Schema 변환기 구현
- [ ] Spark `to_json()` 함수로 Row를 JSON으로 직렬화
- [ ] Apicurio Schema Registry에 JSON Schema 등록
- [ ] 타입 매핑 구현 (Spark → JSON Schema)

#### 3.3 CLI 및 문서화
- [ ] `--serialization-format json` 지원 추가
- [ ] README.md에 JSON Schema 사용법 추가

**예상 소요 시간**: 1-2일

---

## 🧪 테스트 (우선순위 높음)

### 단위 테스트
- [ ] SerializationFactory 테스트 작성
  - [ ] 각 포맷에 대한 전략 생성 검증
  - [ ] 잘못된 포맷 입력 시 예외 처리
  - [ ] Schema name 자동 생성 테스트
- [ ] AvroSerializationStrategy 테스트
  - [ ] Spark 스키마 → Avro 스키마 변환 검증
  - [ ] 다양한 타입 매핑 테스트
  - [ ] Nullable 필드 처리 검증
- [ ] BinarySerializationStrategy 테스트
  - [ ] payload 컬럼 없을 때 예외 처리

### 통합 테스트
- [ ] 테스트 환경 구축
  - [ ] Embedded Kafka 설정
  - [ ] Testcontainers로 Apicurio Schema Registry 실행
  - [ ] In-memory Hive 테이블 생성
- [ ] End-to-End 테스트
  - [ ] Binary 모드: Hive → Kafka 전체 플로우
  - [ ] Avro 모드: 스키마 등록 → 직렬화 → Kafka 전송
  - [ ] 타이밍 제어 모드 검증
  - [ ] 최대 속도 모드 검증

### 성능 테스트
- [ ] 대용량 데이터 (10GB+) 처리 테스트
- [ ] 각 직렬화 포맷별 성능 벤치마크
  - Binary vs Avro vs Protobuf vs JSON
  - 처리 시간, 메모리 사용량, 네트워크 대역폭
- [ ] Schema Registry 부하 테스트

**예상 소요 시간**: 3-5일

---

## 📚 문서화

### 사용자 가이드
- [ ] Quickstart 가이드 작성
  - [ ] 로컬 환경에서 5분 안에 실행하는 방법
  - [ ] Docker Compose로 전체 스택 (Kafka + Apicurio) 실행
- [ ] 트러블슈팅 가이드
  - [ ] 일반적인 오류와 해결 방법
  - [ ] Schema Registry 연결 문제
  - [ ] 메모리 부족 문제
  - [ ] 타입 변환 오류

### 예제 및 튜토리얼
- [ ] 샘플 Hive 테이블 DDL 제공
- [ ] 각 직렬화 포맷별 사용 예제
- [ ] Kubernetes 환경 배포 가이드
  - [ ] Helm Chart 작성
  - [ ] Spark Operator 설정 예제
- [ ] 성능 튜닝 가이드
  - [ ] 배치 크기 최적화
  - [ ] Executor 설정
  - [ ] Kafka Producer 튜닝

### 개발자 문서
- [ ] 아키텍처 문서 (ARCHITECTURE.md)
  - [ ] Serialization Strategy 패턴 설명
  - [ ] 클래스 다이어그램
  - [ ] 데이터 플로우 다이어그램
- [ ] 새로운 직렬화 포맷 추가 가이드
- [ ] 기여 가이드 (CONTRIBUTING.md)

**예상 소요 시간**: 2-3일

---

## 🔧 개선 사항

### 기능 개선
- [ ] **스키마 캐싱**: Schema Registry 호출 최소화
  - [ ] 로컬 파일 시스템에 스키마 캐시
  - [ ] TTL 기반 캐시 무효화
- [ ] **스키마 버전 관리**
  - [ ] 기존 스키마와 호환성 체크
  - [ ] 스키마 진화 (evolution) 전략 구현
- [ ] **배치 재시도 로직**
  - [ ] Kafka 전송 실패 시 재시도
  - [ ] Dead Letter Queue 지원
- [ ] **모니터링 및 메트릭**
  - [ ] Prometheus 메트릭 노출
  - [ ] 처리량, 레이턴시, 에러율 추적
- [ ] **다양한 Key Serializer 지원**
  - [ ] 현재는 String만 지원
  - [ ] Avro, Protobuf Key 직렬화 옵션 추가

### 성능 최적화
- [ ] Zero-copy serialization 검토
- [ ] Vectorized 처리 (Spark Arrow) 활용
- [ ] Schema Registry 연결 풀링
- [ ] 병렬 스키마 등록 (여러 토픽 처리 시)

### 코드 품질
- [ ] Scala 스타일 가이드 적용
- [ ] ScalaFmt 설정 추가
- [ ] Scalastyle 린터 설정
- [ ] 코드 커버리지 측정 (scoverage)
- [ ] CI/CD 파이프라인 구축
  - [ ] GitHub Actions 설정
  - [ ] 자동 빌드 및 테스트
  - [ ] Docker 이미지 자동 생성

**예상 소요 시간**: 3-5일

---

## 🐛 알려진 이슈 및 제한사항

### 현재 제한사항
1. **타입 지원 제한**
   - Complex types (Struct, Array, Map)은 String으로 fallback
   - Decimal, Date 타입 직접 지원 안 됨

2. **Binary 모드의 하드코딩**
   - `payload` 컬럼명이 하드코딩되어 있음
   - 향후 커스터마이징 옵션 추가 필요

3. **타이밍 제어 정밀도**
   - 초 단위 정밀도 (±수백ms)
   - 밀리초 단위 정밀도가 필요한 경우 2-Phase 방식 사용 필요

4. **메모리 제약**
   - `replayWithTiming`에서 `toSeq` 사용으로 메모리 부담
   - 대용량 배치 처리 시 OOM 가능성

### 개선 필요 항목
- [ ] Complex type 지원 (Struct → nested Avro record)
- [ ] Decimal type 정확한 매핑
- [ ] 타이밍 제어 방식 개선 (streaming 기반)
- [ ] 메모리 효율적인 배치 처리 (`toLocalIterator` 활용)

---

## 🎯 우선순위별 로드맵

### High Priority (즉시)
1. **통합 테스트 작성** - 현재 기능 검증
2. **Quickstart 가이드** - 사용자 온보딩
3. **알려진 이슈 수정** - Stability 향상

### Medium Priority (1-2주)
1. **Protobuf 직렬화 구현** - 사용자 요구사항
2. **JSON Schema 직렬화 구현** - 완전한 직렬화 옵션
3. **성능 벤치마크** - 프로덕션 준비

### Low Priority (1개월+)
1. **고급 기능** - 스키마 캐싱, 재시도, 모니터링
2. **코드 품질** - CI/CD, 린터, 커버리지
3. **문서 완성도** - 상세 가이드, 튜토리얼

---

## 📝 작업 시작 전 체크리스트

새로운 작업을 시작하기 전에:

- [ ] 이 문서 읽고 전체 컨텍스트 파악
- [ ] 구현 계획 파일 확인: `/Users/milkyway/.claude/plans/abstract-puzzling-flamingo.md`
- [ ] 최신 코드 pull 및 의존성 확인
- [ ] 작업할 항목의 우선순위 확인
- [ ] 예상 소요 시간 확인
- [ ] 필요한 테스트 환경 준비

## 🔗 관련 파일 및 리소스

### 주요 파일
- 구현 계획: `/Users/milkyway/.claude/plans/abstract-puzzling-flamingo.md`
- 빌드 설정: `/Users/milkyway/Code/kafka-replayer/spark-data-prep/build.sbt`
- 메인 클래스: `/Users/milkyway/Code/kafka-replayer/spark-data-prep/src/main/scala/com/example/replayer/DirectKafkaReplayer.scala`
- Serialization 패키지: `/Users/milkyway/Code/kafka-replayer/spark-data-prep/src/main/scala/com/example/replayer/serialization/`

### 외부 참고 자료
- [Apicurio Registry Documentation](https://www.apicur.io/registry/docs/)
- [Spark Avro Documentation](https://spark.apache.org/docs/latest/sql-data-sources-avro.html)
- [Protocol Buffers Guide](https://protobuf.dev/)
- [Confluent Schema Registry Comparison](https://docs.confluent.io/platform/current/schema-registry/index.html)

---

## 📧 작업 이어하기

다음 번에 작업을 이어할 때는:

1. 이 파일 (`NEXT_STEPS.md`) 확인
2. 우선순위 높은 항목부터 시작
3. 완료된 항목은 `[x]`로 체크
4. 새로운 이슈 발견 시 "알려진 이슈" 섹션에 추가
5. 작업 완료 후 문서 업데이트 날짜 수정

**질문이나 막히는 부분이 있으면 이 문서를 참조하거나 구현 계획 파일을 확인하세요!**
