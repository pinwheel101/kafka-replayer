#!/usr/bin/env python3
"""
Spark Data Preparation for Kafka Replayer

Hive 테이블(ORC)에서 데이터를 읽어 시간순 정렬된 Parquet 청크로 변환합니다.
"""

import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, hour, from_unixtime, count, min as spark_min, max as spark_max
)


def create_spark_session(app_name: str) -> SparkSession:
    """Spark 세션 생성 (Hive 지원 포함)"""
    return SparkSession.builder \
        .appName(app_name) \
        .enableHiveSupport() \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()


def validate_table_exists(spark: SparkSession, table_name: str) -> bool:
    """테이블 존재 여부 확인"""
    try:
        spark.sql(f"DESCRIBE {table_name}")
        return True
    except Exception as e:
        print(f"Error: Table '{table_name}' not found - {e}")
        return False


def get_table_schema(spark: SparkSession, table_name: str) -> dict:
    """테이블 스키마 정보 조회"""
    df = spark.sql(f"DESCRIBE {table_name}")
    columns = {row['col_name']: row['data_type'] for row in df.collect()}
    return columns


def prepare_chunks(
    spark: SparkSession,
    source_table: str,
    target_date: str,
    output_path: str,
    event_time_column: str = "event_time",
    event_key_column: str = "event_key",
    payload_column: str = "payload",
    partition_column: str = "dt",
    chunk_hours: int = 1
):
    """
    데이터 준비 메인 로직
    
    Args:
        spark: SparkSession
        source_table: 소스 Hive 테이블 (예: mydb.events)
        target_date: 대상 날짜 (예: 2021-01-02)
        output_path: 출력 HDFS 경로
        event_time_column: 이벤트 시간 컬럼명 (epoch ms)
        event_key_column: 이벤트 키 컬럼명
        payload_column: 페이로드 컬럼명
        partition_column: 파티션 컬럼명
        chunk_hours: 청크 단위 (시간)
    """
    
    print(f"=" * 60)
    print(f"Kafka Replayer Data Preparation")
    print(f"=" * 60)
    print(f"Source Table: {source_table}")
    print(f"Target Date: {target_date}")
    print(f"Output Path: {output_path}")
    print(f"Event Time Column: {event_time_column}")
    print(f"Event Key Column: {event_key_column}")
    print(f"Chunk Hours: {chunk_hours}")
    print(f"=" * 60)
    
    # 1. 테이블 검증
    if not validate_table_exists(spark, source_table):
        raise ValueError(f"Table {source_table} does not exist")
    
    schema = get_table_schema(spark, source_table)
    print(f"Table schema: {schema}")
    
    # 2. 데이터 읽기
    print(f"\nReading data for date: {target_date}")
    
    # 필요한 컬럼만 선택
    select_columns = [event_key_column, event_time_column]
    if payload_column in schema:
        select_columns.append(payload_column)
    
    df = spark.sql(f"""
        SELECT 
            {', '.join(select_columns)}
        FROM {source_table}
        WHERE {partition_column} = '{target_date}'
    """)
    
    # 3. 시간대(hour) 컬럼 추가
    # event_time이 epoch milliseconds라고 가정
    df = df.withColumn(
        "event_hour",
        hour(from_unixtime(col(event_time_column) / 1000))
    )
    
    # 4. 통계 출력
    stats = df.agg(
        count("*").alias("total_count"),
        spark_min(event_time_column).alias("min_event_time"),
        spark_max(event_time_column).alias("max_event_time")
    ).collect()[0]
    
    print(f"\nData Statistics:")
    print(f"  Total Records: {stats['total_count']:,}")
    print(f"  Min Event Time: {stats['min_event_time']}")
    print(f"  Max Event Time: {stats['max_event_time']}")
    
    if stats['total_count'] == 0:
        print("Warning: No data found for the specified date!")
        return
    
    # 시간대별 분포
    print(f"\nHourly Distribution:")
    hourly_dist = df.groupBy("event_hour").count().orderBy("event_hour").collect()
    for row in hourly_dist:
        print(f"  Hour {row['event_hour']:02d}: {row['count']:,} events")
    
    # 5. 시간대별 파티션으로 정렬 및 저장
    print(f"\nWriting to {output_path}...")
    
    df.repartition("event_hour") \
      .sortWithinPartitions(col(event_time_column).asc()) \
      .write \
      .partitionBy("event_hour") \
      .mode("overwrite") \
      .option("compression", "snappy") \
      .parquet(output_path)
    
    print(f"\nData preparation completed successfully!")
    print(f"Output: {output_path}")
    
    # 6. 출력 검증
    print(f"\nVerifying output...")
    output_df = spark.read.parquet(output_path)
    output_count = output_df.count()
    print(f"Output record count: {output_count:,}")
    
    if output_count != stats['total_count']:
        print(f"Warning: Record count mismatch! Input: {stats['total_count']}, Output: {output_count}")


def main():
    parser = argparse.ArgumentParser(
        description="Prepare Hive/ORC data for Kafka Replayer"
    )
    parser.add_argument(
        "--source-table", "-s",
        required=True,
        help="Source Hive table (e.g., mydb.events)"
    )
    parser.add_argument(
        "--target-date", "-d",
        required=True,
        help="Target date (e.g., 2021-01-02)"
    )
    parser.add_argument(
        "--output-path", "-o",
        required=True,
        help="Output HDFS path (e.g., hdfs:///replay/prepared/2021-01-02)"
    )
    parser.add_argument(
        "--event-time-column",
        default="event_time",
        help="Event time column name (default: event_time)"
    )
    parser.add_argument(
        "--event-key-column",
        default="event_key",
        help="Event key column name (default: event_key)"
    )
    parser.add_argument(
        "--payload-column",
        default="payload",
        help="Payload column name (default: payload)"
    )
    parser.add_argument(
        "--partition-column",
        default="dt",
        help="Partition column name (default: dt)"
    )
    parser.add_argument(
        "--chunk-hours",
        type=int,
        default=1,
        help="Chunk size in hours (default: 1)"
    )
    
    args = parser.parse_args()
    
    # Spark 세션 생성
    spark = create_spark_session("Kafka-Replayer-Data-Prep")
    
    try:
        prepare_chunks(
            spark=spark,
            source_table=args.source_table,
            target_date=args.target_date,
            output_path=args.output_path,
            event_time_column=args.event_time_column,
            event_key_column=args.event_key_column,
            payload_column=args.payload_column,
            partition_column=args.partition_column,
            chunk_hours=args.chunk_hours
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
