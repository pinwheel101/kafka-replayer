#!/bin/bash
#
# Kafka Replayer 실행 스크립트
#
# 사용법:
#   ./run-replayer.sh replay --input-path /replay/prepared/2021-01-02 --topic events-replay
#   ./run-replayer.sh stats --input-path /replay/prepared/2021-01-02
#   ./run-replayer.sh validate --input-path /replay/prepared/2021-01-02 --topic events-replay
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
JAR_FILE="$PROJECT_DIR/kotlin-replayer/build/libs/kafka-replayer-1.0.0-all.jar"

# JVM 설정
JAVA_OPTS="${JAVA_OPTS:--Xms4g -Xmx16g}"
JAVA_OPTS="$JAVA_OPTS -XX:+UseG1GC"
JAVA_OPTS="$JAVA_OPTS -XX:MaxGCPauseMillis=50"
JAVA_OPTS="$JAVA_OPTS -XX:+ParallelRefProcEnabled"
JAVA_OPTS="$JAVA_OPTS -Djava.net.preferIPv4Stack=true"

# Hadoop 설정 (필요시)
if [[ -n "$HADOOP_HOME" ]]; then
    CLASSPATH="$HADOOP_HOME/etc/hadoop:$CLASSPATH"
fi

# 색상 출력
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Kafka Time-Synced Replayer${NC}"
echo -e "${GREEN}========================================${NC}"

# JAR 파일 확인
if [[ ! -f "$JAR_FILE" ]]; then
    echo -e "${YELLOW}JAR file not found. Building...${NC}"
    cd "$PROJECT_DIR/kotlin-replayer"
    ./gradlew shadowJar
    cd -
fi

if [[ ! -f "$JAR_FILE" ]]; then
    echo -e "${RED}Error: JAR file not found at $JAR_FILE${NC}"
    echo "Please build the project first: cd kotlin-replayer && ./gradlew shadowJar"
    exit 1
fi

echo ""
echo -e "${YELLOW}JVM Options: $JAVA_OPTS${NC}"
echo ""

# 실행
java $JAVA_OPTS -jar "$JAR_FILE" "$@"
