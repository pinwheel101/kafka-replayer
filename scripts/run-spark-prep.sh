#!/bin/bash
#
# Spark 데이터 준비 스크립트
#
# 사용법:
#   ./run-spark-prep.sh --source-table mydb.events --target-date 2021-01-02 --output-path hdfs:///replay/prepared/2021-01-02
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
SPARK_APP="$PROJECT_DIR/spark-data-prep/prepare_chunks.py"

# 기본값
SPARK_MASTER="${SPARK_MASTER:-yarn}"
DEPLOY_MODE="${DEPLOY_MODE:-cluster}"
NUM_EXECUTORS="${NUM_EXECUTORS:-20}"
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-8g}"
EXECUTOR_CORES="${EXECUTOR_CORES:-4}"
DRIVER_MEMORY="${DRIVER_MEMORY:-4g}"

# 색상 출력
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Kafka Replayer - Spark Data Preparation${NC}"
echo -e "${GREEN}========================================${NC}"

# 인자 파싱
SOURCE_TABLE=""
TARGET_DATE=""
OUTPUT_PATH=""
EVENT_TIME_COLUMN="event_time"
EVENT_KEY_COLUMN="event_key"
PAYLOAD_COLUMN="payload"
PARTITION_COLUMN="dt"

while [[ $# -gt 0 ]]; do
    case $1 in
        --source-table|-s)
            SOURCE_TABLE="$2"
            shift 2
            ;;
        --target-date|-d)
            TARGET_DATE="$2"
            shift 2
            ;;
        --output-path|-o)
            OUTPUT_PATH="$2"
            shift 2
            ;;
        --event-time-column)
            EVENT_TIME_COLUMN="$2"
            shift 2
            ;;
        --event-key-column)
            EVENT_KEY_COLUMN="$2"
            shift 2
            ;;
        --payload-column)
            PAYLOAD_COLUMN="$2"
            shift 2
            ;;
        --partition-column)
            PARTITION_COLUMN="$2"
            shift 2
            ;;
        --num-executors)
            NUM_EXECUTORS="$2"
            shift 2
            ;;
        --executor-memory)
            EXECUTOR_MEMORY="$2"
            shift 2
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Required options:"
            echo "  -s, --source-table    Source Hive table (e.g., mydb.events)"
            echo "  -d, --target-date     Target date (e.g., 2021-01-02)"
            echo "  -o, --output-path     Output HDFS path"
            echo ""
            echo "Optional options:"
            echo "  --event-time-column   Event time column name (default: event_time)"
            echo "  --event-key-column    Event key column name (default: event_key)"
            echo "  --payload-column      Payload column name (default: payload)"
            echo "  --partition-column    Partition column name (default: dt)"
            echo "  --num-executors       Number of Spark executors (default: 20)"
            echo "  --executor-memory     Executor memory (default: 8g)"
            echo ""
            echo "Environment variables:"
            echo "  SPARK_MASTER          Spark master (default: yarn)"
            echo "  DEPLOY_MODE           Deploy mode (default: cluster)"
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

# 필수 인자 확인
if [[ -z "$SOURCE_TABLE" || -z "$TARGET_DATE" || -z "$OUTPUT_PATH" ]]; then
    echo -e "${RED}Error: Missing required arguments${NC}"
    echo "Run '$0 --help' for usage information"
    exit 1
fi

# 설정 출력
echo ""
echo -e "${YELLOW}Configuration:${NC}"
echo "  Source Table:    $SOURCE_TABLE"
echo "  Target Date:     $TARGET_DATE"
echo "  Output Path:     $OUTPUT_PATH"
echo "  Spark Master:    $SPARK_MASTER"
echo "  Deploy Mode:     $DEPLOY_MODE"
echo "  Num Executors:   $NUM_EXECUTORS"
echo "  Executor Memory: $EXECUTOR_MEMORY"
echo "  Executor Cores:  $EXECUTOR_CORES"
echo ""

# Spark submit 실행
echo -e "${GREEN}Starting Spark job...${NC}"
echo ""

spark-submit \
    --master "$SPARK_MASTER" \
    --deploy-mode "$DEPLOY_MODE" \
    --num-executors "$NUM_EXECUTORS" \
    --executor-memory "$EXECUTOR_MEMORY" \
    --executor-cores "$EXECUTOR_CORES" \
    --driver-memory "$DRIVER_MEMORY" \
    --conf "spark.sql.parquet.compression.codec=snappy" \
    --conf "spark.sql.shuffle.partitions=200" \
    --conf "spark.sql.adaptive.enabled=true" \
    --conf "spark.dynamicAllocation.enabled=false" \
    "$SPARK_APP" \
    --source-table "$SOURCE_TABLE" \
    --target-date "$TARGET_DATE" \
    --output-path "$OUTPUT_PATH" \
    --event-time-column "$EVENT_TIME_COLUMN" \
    --event-key-column "$EVENT_KEY_COLUMN" \
    --payload-column "$PAYLOAD_COLUMN" \
    --partition-column "$PARTITION_COLUMN"

echo ""
echo -e "${GREEN}Spark job completed!${NC}"
echo -e "${GREEN}Output: $OUTPUT_PATH${NC}"
