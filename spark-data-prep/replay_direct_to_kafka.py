#!/usr/bin/env python3
"""
Spark에서 직접 Kafka로 리플레이 (시간 간격 제어)

HDFS 중간 저장 없이 Hive → Kafka 직접 전송
대략적인 시간 간격 유지 (초 단위 정밀도)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, unix_timestamp, from_json, to_json, struct
import time
import argparse


def create_spark_session():
    return (SparkSession.builder
            .appName("Kafka Direct Replayer")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .enableHiveSupport()
            .getOrCreate())


def replay_with_timing(
    spark,
    source_table: str,
    target_date: str,
    kafka_bootstrap: str,
    kafka_topic: str,
    speed_factor: float = 1.0,
    batch_size: int = 10000
):
    """
    시간 간격을 제어하며 Kafka로 리플레이

    Args:
        source_table: Hive 테이블명 (예: mydb.events)
        target_date: 대상 날짜 (예: 2021-01-02)
        kafka_bootstrap: Kafka 브로커 주소
        kafka_topic: 목적지 토픽
        speed_factor: 재생 속도 (1.0=실시간, 2.0=2배속)
        batch_size: 배치당 이벤트 수 (시간 간격 제어 단위)
    """

    print(f"[*] Reading from {source_table} for date {target_date}")
    print(f"[*] Speed factor: {speed_factor}x")
    print(f"[*] Batch size: {batch_size} events")

    # 1. Hive에서 데이터 읽기 및 정렬
    df = (spark.table(source_table)
          .filter(f"dt = '{target_date}'")
          .select(
              col("event_key").alias("key"),
              col("event_time").alias("event_time_ms"),
              col("payload").alias("value")
          )
          .orderBy("event_time_ms"))

    total_count = df.count()
    print(f"[*] Total events: {total_count:,}")

    # 2. Pandas Iterator로 배치 처리
    # (대용량 데이터는 메모리 효율적으로 처리)
    pdf_iter = df.toLocalIterator()

    batch = []
    batch_num = 0
    prev_time_ms = None
    start_wall_time = time.time()
    base_event_time = None

    total_sent = 0

    for row in pdf_iter:
        event_time_ms = row.event_time_ms

        # 첫 이벤트 시간 기록
        if base_event_time is None:
            base_event_time = event_time_ms
            prev_time_ms = event_time_ms

        batch.append(row)

        # 배치가 찼거나 마지막 이벤트
        if len(batch) >= batch_size:
            batch_num += 1

            # 배치 전송
            send_batch_to_kafka(spark, batch, kafka_bootstrap, kafka_topic)
            total_sent += len(batch)

            # 시간 간격 계산 및 대기
            if prev_time_ms is not None:
                # 원본 시간 간격 (ms)
                original_interval = event_time_ms - prev_time_ms

                # 배속 적용
                adjusted_interval = original_interval / speed_factor / 1000.0  # 초 단위

                # 최소/최대 대기 시간 제한
                adjusted_interval = max(0, min(adjusted_interval, 60))  # 최대 60초

                if adjusted_interval > 0:
                    print(f"[{batch_num}] Sent {len(batch):,} events, "
                          f"waiting {adjusted_interval:.2f}s "
                          f"(original: {original_interval/1000:.2f}s)")
                    time.sleep(adjusted_interval)

            prev_time_ms = event_time_ms
            batch = []

            # 진행 상황 출력
            progress = (total_sent / total_count) * 100
            elapsed = time.time() - start_wall_time
            print(f"[*] Progress: {progress:.1f}% ({total_sent:,}/{total_count:,}), "
                  f"Elapsed: {elapsed:.1f}s")

    # 마지막 배치 전송
    if batch:
        send_batch_to_kafka(spark, batch, kafka_bootstrap, kafka_topic)
        total_sent += len(batch)
        print(f"[FINAL] Sent last batch of {len(batch):,} events")

    total_elapsed = time.time() - start_wall_time
    print(f"\n[✓] Replay completed!")
    print(f"    Total events: {total_sent:,}")
    print(f"    Total time: {total_elapsed:.1f}s")
    print(f"    Avg throughput: {total_sent/total_elapsed:.0f} events/sec")


def send_batch_to_kafka(spark, batch, kafka_bootstrap, kafka_topic):
    """
    배치를 Kafka로 전송
    """
    # Pandas DataFrame으로 변환
    import pandas as pd
    pdf = pd.DataFrame([row.asDict() for row in batch])

    # Spark DataFrame으로 변환
    df = spark.createDataFrame(pdf)

    # Kafka 전송
    (df.selectExpr(
        "CAST(key AS STRING) as key",
        "CAST(value AS BINARY) as value"
    )
    .write
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap)
    .option("topic", kafka_topic)
    .save())


def replay_max_speed(
    spark,
    source_table: str,
    target_date: str,
    kafka_bootstrap: str,
    kafka_topic: str
):
    """
    최대 속도로 Kafka에 전송 (시간 간격 무시)
    """
    print(f"[*] Max speed replay mode")
    print(f"[*] Reading from {source_table} for date {target_date}")

    # 간단한 방법: DataFrame을 직접 Kafka에 쓰기
    (spark.table(source_table)
     .filter(f"dt = '{target_date}'")
     .select(
         col("event_key").alias("key"),
         col("payload").alias("value")
     )
     .orderBy("event_time")  # 순서 유지
     .selectExpr(
         "CAST(key AS STRING) as key",
         "CAST(value AS BINARY) as value"
     )
     .write
     .format("kafka")
     .option("kafka.bootstrap.servers", kafka_bootstrap)
     .option("topic", kafka_topic)
     .save())

    print("[✓] Max speed replay completed!")


def main():
    parser = argparse.ArgumentParser(description="Spark Direct Kafka Replayer")
    parser.add_argument("--source-table", required=True, help="Hive source table (e.g., mydb.events)")
    parser.add_argument("--target-date", required=True, help="Target date (YYYY-MM-DD)")
    parser.add_argument("--kafka-bootstrap", required=True, help="Kafka bootstrap servers")
    parser.add_argument("--topic", required=True, help="Kafka topic")
    parser.add_argument("--speed", type=float, default=1.0, help="Replay speed factor (default: 1.0)")
    parser.add_argument("--batch-size", type=int, default=10000, help="Batch size (default: 10000)")
    parser.add_argument("--max-speed", action="store_true", help="Max speed mode (ignore timing)")

    args = parser.parse_args()

    spark = create_spark_session()

    try:
        if args.max_speed:
            replay_max_speed(
                spark,
                args.source_table,
                args.target_date,
                args.kafka_bootstrap,
                args.topic
            )
        else:
            replay_with_timing(
                spark,
                args.source_table,
                args.target_date,
                args.kafka_bootstrap,
                args.topic,
                args.speed,
                args.batch_size
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
