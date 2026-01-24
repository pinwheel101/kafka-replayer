package com.example.replayer

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import scopt.OParser

/**
 * Kafka Replayer 데이터 준비 (Spark Scala 버전)
 * 
 * Hive 테이블(ORC)에서 데이터를 읽어 시간순 정렬된 Parquet 청크로 변환합니다.
 */
object PrepareChunks {

  case class Config(
    sourceTable: String = "",
    targetDate: String = "",
    outputPath: String = "",
    eventTimeColumn: String = "event_time",
    eventKeyColumn: String = "event_key",
    payloadColumn: String = "payload",
    partitionColumn: String = "dt",
    chunkHours: Int = 1
  )

  def main(args: Array[String]): Unit = {
    val builder = OParser.builder[Config]
    
    val parser = {
      import builder._
      OParser.sequence(
        programName("spark-data-prep"),
        head("Kafka Replayer Data Preparation", "1.0.0"),
        
        opt[String]('s', "source-table")
          .required()
          .action((x, c) => c.copy(sourceTable = x))
          .text("Source Hive table (e.g., mydb.events)"),
        
        opt[String]('d', "target-date")
          .required()
          .action((x, c) => c.copy(targetDate = x))
          .text("Target date (e.g., 2021-01-02)"),
        
        opt[String]('o', "output-path")
          .required()
          .action((x, c) => c.copy(outputPath = x))
          .text("Output HDFS path"),
        
        opt[String]("event-time-column")
          .action((x, c) => c.copy(eventTimeColumn = x))
          .text("Event time column name (default: event_time)"),
        
        opt[String]("event-key-column")
          .action((x, c) => c.copy(eventKeyColumn = x))
          .text("Event key column name (default: event_key)"),
        
        opt[String]("payload-column")
          .action((x, c) => c.copy(payloadColumn = x))
          .text("Payload column name (default: payload)"),
        
        opt[String]("partition-column")
          .action((x, c) => c.copy(partitionColumn = x))
          .text("Partition column name (default: dt)"),
        
        opt[Int]("chunk-hours")
          .action((x, c) => c.copy(chunkHours = x))
          .text("Chunk size in hours (default: 1)")
      )
    }

    OParser.parse(parser, args, Config()) match {
      case Some(config) => run(config)
      case None => sys.exit(1)
    }
  }

  def run(config: Config): Unit = {
    val spark = createSparkSession()
    
    try {
      prepareChunks(spark, config)
    } finally {
      spark.stop()
    }
  }

  def createSparkSession(): SparkSession = {
    SparkSession.builder()
      .appName("Kafka-Replayer-Data-Prep")
      .enableHiveSupport()
      .config("spark.sql.parquet.compression.codec", "snappy")
      .config("spark.sql.shuffle.partitions", "200")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .getOrCreate()
  }

  def prepareChunks(spark: SparkSession, config: Config): Unit = {
    import spark.implicits._
    
    println("=" * 60)
    println("Kafka Replayer Data Preparation")
    println("=" * 60)
    println(s"Source Table: ${config.sourceTable}")
    println(s"Target Date: ${config.targetDate}")
    println(s"Output Path: ${config.outputPath}")
    println(s"Event Time Column: ${config.eventTimeColumn}")
    println(s"Event Key Column: ${config.eventKeyColumn}")
    println(s"Chunk Hours: ${config.chunkHours}")
    println("=" * 60)

    // 1. 테이블 검증
    validateTableExists(spark, config.sourceTable)

    // 2. 데이터 읽기
    println(s"\nReading data for date: ${config.targetDate}")
    
    val df = spark.sql(
      s"""
         |SELECT 
         |  ${config.eventKeyColumn},
         |  ${config.eventTimeColumn},
         |  ${config.payloadColumn}
         |FROM ${config.sourceTable}
         |WHERE ${config.partitionColumn} = '${config.targetDate}'
       """.stripMargin
    ).withColumn(
      "event_hour",
      hour(from_unixtime(col(config.eventTimeColumn) / 1000))
    )

    // 3. 통계 출력
    val stats = df.agg(
      count("*").as("total_count"),
      min(config.eventTimeColumn).as("min_event_time"),
      max(config.eventTimeColumn).as("max_event_time")
    ).collect()(0)

    val totalCount = stats.getAs[Long]("total_count")
    println(s"\nData Statistics:")
    println(s"  Total Records: ${"%,d".format(totalCount)}")
    println(s"  Min Event Time: ${stats.getAs[Long]("min_event_time")}")
    println(s"  Max Event Time: ${stats.getAs[Long]("max_event_time")}")

    if (totalCount == 0) {
      println("Warning: No data found for the specified date!")
      return
    }

    // 시간대별 분포
    println("\nHourly Distribution:")
    df.groupBy("event_hour")
      .count()
      .orderBy("event_hour")
      .collect()
      .foreach { row =>
        val hour = row.getAs[Int]("event_hour")
        val count = row.getAs[Long]("count")
        println(f"  Hour $hour%02d: ${"%,d".format(count)} events")
      }

    // 4. 시간대별 파티션으로 정렬 및 저장
    println(s"\nWriting to ${config.outputPath}...")

    df.repartition($"event_hour")
      .sortWithinPartitions(col(config.eventTimeColumn).asc)
      .write
      .partitionBy("event_hour")
      .mode("overwrite")
      .option("compression", "snappy")
      .parquet(config.outputPath)

    println("\nData preparation completed successfully!")
    println(s"Output: ${config.outputPath}")

    // 5. 출력 검증
    println("\nVerifying output...")
    val outputDf = spark.read.parquet(config.outputPath)
    val outputCount = outputDf.count()
    println(s"Output record count: ${"%,d".format(outputCount)}")

    if (outputCount != totalCount) {
      println(s"Warning: Record count mismatch! Input: $totalCount, Output: $outputCount")
    }
  }

  def validateTableExists(spark: SparkSession, tableName: String): Unit = {
    try {
      spark.sql(s"DESCRIBE $tableName")
    } catch {
      case e: Exception =>
        throw new IllegalArgumentException(s"Table '$tableName' does not exist: ${e.getMessage}")
    }
  }
}
