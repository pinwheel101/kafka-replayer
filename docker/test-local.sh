#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
JAR_PATH="$PROJECT_ROOT/spark-data-prep/target/scala-2.13/spark-data-prep-assembly-1.0.0.jar"

echo -e "${GREEN}=== Kafka Replayer Local Test ===${NC}\n"

# Function to print colored messages
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if JAR exists
check_jar() {
    if [ ! -f "$JAR_PATH" ]; then
        log_error "JAR file not found at $JAR_PATH"
        log_info "Please build the project first: cd spark-data-prep && sbt assembly"
        exit 1
    fi
    log_info "JAR file found: $JAR_PATH"
}

# Start Docker Compose
start_services() {
    log_info "Starting Docker Compose services..."
    cd "$SCRIPT_DIR"
    docker-compose up -d

    log_info "Waiting for services to be ready..."
    sleep 10

    # Wait for Kafka
    log_info "Waiting for Kafka..."
    for i in {1..30}; do
        if docker exec kafka-replayer-kafka kafka-broker-api-versions --bootstrap-server localhost:9092 &>/dev/null; then
            log_info "Kafka is ready!"
            break
        fi
        if [ $i -eq 30 ]; then
            log_error "Kafka failed to start"
            exit 1
        fi
        sleep 2
    done

    # Wait for Apicurio
    log_info "Waiting for Apicurio Schema Registry..."
    for i in {1..30}; do
        if curl -f http://localhost:8080/health &>/dev/null; then
            log_info "Apicurio Schema Registry is ready!"
            break
        fi
        if [ $i -eq 30 ]; then
            log_error "Apicurio failed to start"
            exit 1
        fi
        sleep 2
    done

    # Wait for PostgreSQL
    log_info "Waiting for PostgreSQL..."
    for i in {1..20}; do
        if docker exec kafka-replayer-postgres pg_isready -U test &>/dev/null; then
            log_info "PostgreSQL is ready!"
            break
        fi
        if [ $i -eq 20 ]; then
            log_error "PostgreSQL failed to start"
            exit 1
        fi
        sleep 2
    done
}

# Show service status
show_status() {
    echo ""
    log_info "=== Service Status ==="
    docker-compose ps

    echo ""
    log_info "=== Service URLs ==="
    echo "  Kafka:            localhost:9092"
    echo "  Apicurio:         http://localhost:8080"
    echo "  Kafka UI:         http://localhost:8090"
    echo "  PostgreSQL:       localhost:5432 (user: test, password: test, db: testdb)"
}

# Create test topics
create_topics() {
    log_info "Creating test topics..."
    docker exec kafka-replayer-kafka kafka-topics --bootstrap-server localhost:9092 --create --topic test-binary --partitions 3 --replication-factor 1 --if-not-exists
    docker exec kafka-replayer-kafka kafka-topics --bootstrap-server localhost:9092 --create --topic test-avro --partitions 3 --replication-factor 1 --if-not-exists

    log_info "Topics created:"
    docker exec kafka-replayer-kafka kafka-topics --bootstrap-server localhost:9092 --list
}

# Check test data
check_test_data() {
    log_info "Checking test data in PostgreSQL..."
    docker exec kafka-replayer-postgres psql -U test -d testdb -c "SELECT dt, COUNT(*) as count FROM events GROUP BY dt ORDER BY dt;"
}

# Run test (Binary format)
run_test_binary() {
    echo ""
    log_info "=== Running Binary Format Test ==="

    # Note: This requires Spark installed locally
    if ! command -v spark-submit &> /dev/null; then
        log_warn "spark-submit not found. Skipping actual Spark test."
        log_info "To run with Spark, install Spark locally or use Docker Spark image"
        return
    fi

    log_info "Running Binary serialization test..."
    # This is a placeholder - actual implementation needs Spark with Hive support
    log_warn "Binary test requires Hive support - using manual verification instead"
}

# Verify Kafka messages
verify_kafka() {
    echo ""
    log_info "=== Verifying Kafka Topics ==="

    log_info "Messages in test-binary topic:"
    docker exec kafka-replayer-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic test-binary --from-beginning --max-messages 5 --timeout-ms 5000 || true

    log_info "Messages in test-avro topic:"
    docker exec kafka-replayer-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic test-avro --from-beginning --max-messages 5 --timeout-ms 5000 || true
}

# Verify Schema Registry
verify_schema_registry() {
    echo ""
    log_info "=== Checking Schema Registry ==="

    SCHEMAS=$(curl -s http://localhost:8080/apis/registry/v2/search/artifacts | grep -o '"id":"[^"]*"' | cut -d'"' -f4 || echo "")

    if [ -z "$SCHEMAS" ]; then
        log_info "No schemas registered yet"
    else
        log_info "Registered schemas:"
        echo "$SCHEMAS"
    fi
}

# Show helper commands
show_helper_commands() {
    echo ""
    log_info "=== Useful Commands ==="
    echo ""
    echo "# Check test data"
    echo "docker exec -it kafka-replayer-postgres psql -U test -d testdb"
    echo ""
    echo "# Consume from Kafka topic"
    echo "docker exec kafka-replayer-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic test-avro --from-beginning"
    echo ""
    echo "# Check schemas in registry"
    echo "curl http://localhost:8080/apis/registry/v2/search/artifacts | jq"
    echo ""
    echo "# View Kafka UI"
    echo "open http://localhost:8090"
    echo ""
    echo "# Stop services"
    echo "cd $SCRIPT_DIR && docker-compose down"
    echo ""
}

# Manual test instruction
show_manual_test() {
    echo ""
    log_info "=== Manual Test Instructions ==="
    echo ""
    echo "Since this test environment uses PostgreSQL (not Hive), you need to:"
    echo ""
    echo "1. Query the test data to get current date:"
    TODAY=$(docker exec kafka-replayer-postgres psql -U test -d testdb -t -c "SELECT DISTINCT dt FROM events ORDER BY dt DESC LIMIT 1;" | xargs)
    echo "   Current test date: $TODAY"
    echo ""
    echo "2. Create a simple CSV file from PostgreSQL data:"
    echo "   docker exec kafka-replayer-postgres psql -U test -d testdb -c \"COPY (SELECT event_key, event_time, user_id, event_type, payload, dt FROM events WHERE dt='$TODAY') TO STDOUT WITH CSV HEADER\" > /tmp/test-events.csv"
    echo ""
    echo "3. For full Spark testing, you would need:"
    echo "   - Spark with Hive support"
    echo "   - Or use the Testcontainers integration test (see tests/)"
    echo ""
    log_warn "Alternatively, use the integration tests with Testcontainers (recommended)"
    echo "   cd spark-data-prep && sbt test"
    echo ""
}

# Main execution
main() {
    case "${1:-start}" in
        start)
            check_jar
            start_services
            show_status
            create_topics
            check_test_data
            verify_schema_registry
            show_manual_test
            show_helper_commands
            ;;
        stop)
            log_info "Stopping services..."
            cd "$SCRIPT_DIR"
            docker-compose down
            log_info "Services stopped"
            ;;
        restart)
            log_info "Restarting services..."
            cd "$SCRIPT_DIR"
            docker-compose restart
            sleep 5
            show_status
            ;;
        status)
            show_status
            check_test_data
            verify_schema_registry
            ;;
        logs)
            cd "$SCRIPT_DIR"
            docker-compose logs -f
            ;;
        clean)
            log_warn "Cleaning up all data..."
            cd "$SCRIPT_DIR"
            docker-compose down -v
            log_info "All data cleaned"
            ;;
        *)
            echo "Usage: $0 {start|stop|restart|status|logs|clean}"
            echo ""
            echo "Commands:"
            echo "  start   - Start all services and show test instructions"
            echo "  stop    - Stop all services"
            echo "  restart - Restart all services"
            echo "  status  - Show service status"
            echo "  logs    - Show service logs (follow mode)"
            echo "  clean   - Stop services and remove all data"
            exit 1
            ;;
    esac
}

main "$@"
