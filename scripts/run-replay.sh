#!/bin/bash
#
# Kafka Direct Replayer 실행 스크립트
#
# 사용법:
#   ./run-replay.sh [OPTIONS]
#

set -e

# 기본값 설정
SOURCE_TABLE="${SOURCE_TABLE:-mydb.events}"
TARGET_DATE="${TARGET_DATE:-2021-01-02}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka-service:9092}"
KAFKA_TOPIC="${KAFKA_TOPIC:-events-replay}"
SPEED="${SPEED:-1.0}"
BATCH_SIZE="${BATCH_SIZE:-10000}"
MAX_SPEED="${MAX_SPEED:-false}"

# Spark 설정
SPARK_MASTER="${SPARK_MASTER:-yarn}"
DEPLOY_MODE="${DEPLOY_MODE:-cluster}"
NUM_EXECUTORS="${NUM_EXECUTORS:-20}"
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-8g}"
EXECUTOR_CORES="${EXECUTOR_CORES:-4}"
DRIVER_MEMORY="${DRIVER_MEMORY:-4g}"

# JAR 파일 경로
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
JAR_FILE="${PROJECT_ROOT}/spark-data-prep/target/scala-2.12/spark-data-prep-assembly-1.0.0.jar"

# JAR 파일 존재 확인
if [ ! -f "$JAR_FILE" ]; then
    echo "Error: JAR file not found at $JAR_FILE"
    echo "Please build the project first:"
    echo "  cd spark-data-prep && sbt assembly"
    exit 1
fi

# 인수 빌드
ARGS=(
    "--source-table" "$SOURCE_TABLE"
    "--target-date" "$TARGET_DATE"
    "--kafka-bootstrap" "$KAFKA_BOOTSTRAP"
    "--topic" "$KAFKA_TOPIC"
)

if [ "$MAX_SPEED" = "true" ]; then
    ARGS+=("--max-speed")
else
    ARGS+=("--speed" "$SPEED")
    ARGS+=("--batch-size" "$BATCH_SIZE")
fi

echo "================================"
echo "Kafka Direct Replayer"
echo "================================"
echo "Source Table:    $SOURCE_TABLE"
echo "Target Date:     $TARGET_DATE"
echo "Kafka Bootstrap: $KAFKA_BOOTSTRAP"
echo "Kafka Topic:     $KAFKA_TOPIC"
if [ "$MAX_SPEED" = "true" ]; then
    echo "Mode:            Max Speed"
else
    echo "Speed Factor:    ${SPEED}x"
    echo "Batch Size:      $BATCH_SIZE"
fi
echo "================================"
echo

# Spark Submit
spark-submit \
    --master "$SPARK_MASTER" \
    --deploy-mode "$DEPLOY_MODE" \
    --num-executors "$NUM_EXECUTORS" \
    --executor-memory "$EXECUTOR_MEMORY" \
    --executor-cores "$EXECUTOR_CORES" \
    --driver-memory "$DRIVER_MEMORY" \
    --conf "spark.sql.adaptive.enabled=true" \
    --conf "spark.sql.adaptive.coalescePartitions.enabled=true" \
    --class com.example.replayer.DirectKafkaReplayer \
    "$JAR_FILE" \
    "${ARGS[@]}"

echo
echo "Replay completed successfully!"
