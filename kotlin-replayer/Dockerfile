# Stage 1: Build
FROM gradle:8.5-jdk17 AS builder

WORKDIR /build

# Gradle 캐시 최적화를 위해 의존성 먼저 다운로드
COPY build.gradle.kts settings.gradle.kts ./
COPY gradle ./gradle
RUN gradle dependencies --no-daemon

# 소스 코드 복사 및 빌드
COPY src ./src
RUN gradle shadowJar --no-daemon

# Stage 2: Runtime
FROM eclipse-temurin:17-jre-jammy

# 메타데이터
LABEL maintainer="kafka-replayer"
LABEL version="1.0.0"
LABEL description="Kafka Time-Synced Replayer"

# 작업 디렉토리 설정
WORKDIR /app

# 비root 사용자 생성
RUN groupadd -r replayer && useradd -r -g replayer replayer

# 빌드된 JAR 복사
COPY --from=builder /build/build/libs/kafka-replayer-all-*.jar /app/kafka-replayer.jar

# 체크포인트 디렉토리 생성 (PVC 마운트 포인트)
RUN mkdir -p /app/checkpoints && \
    chown -R replayer:replayer /app

# 사용자 전환
USER replayer

# 포트 노출 (메트릭 포트)
EXPOSE 9090

# 헬스체크
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
  CMD curl -f http://localhost:9090/metrics || exit 1

# 기본 JVM 옵션 (환경변수로 오버라이드 가능)
ENV JAVA_OPTS="-Xms2g -Xmx16g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"

# 실행
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /app/kafka-replayer.jar \"$@\"", "--"]
