package com.example.replayer

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import scala.collection.mutable.ArrayBuffer

/**
 * Spark에서 직접 Kafka로 리플레이 (시간 간격 제어)
 *
 * HDFS 중간 저장 없이 Hive → Kafka 직접 전송
 * 대략적인 시간 간격 유지 (초 단위 정밀도)
 */
object DirectKafkaReplayer {

  case class Config(
    sourceTable: String = "",
    targetDate: String = "",
    kafkaBootstrap: String = "",
    kafkaTopic: String = "",
    speedFactor: Double = 1.0,
    batchSize: Int = 10000,
    maxSpeed: Boolean = false
  )

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Config]("DirectKafkaReplayer") {
      opt[String]("source-table")
        .required()
        .action((x, c) => c.copy(sourceTable = x))
        .text("Hive source table (e.g., mydb.events)")

      opt[String]("target-date")
        .required()
        .action((x, c) => c.copy(targetDate = x))
        .text("Target date (YYYY-MM-DD)")

      opt[String]("kafka-bootstrap")
        .required()
        .action((x, c) => c.copy(kafkaBootstrap = x))
        .text("Kafka bootstrap servers")

      opt[String]("topic")
        .required()
        .action((x, c) => c.copy(kafkaTopic = x))
        .text("Kafka topic")

      opt[Double]("speed")
        .optional()
        .action((x, c) => c.copy(speedFactor = x))
        .text("Replay speed factor (default: 1.0)")

      opt[Int]("batch-size")
        .optional()
        .action((x, c) => c.copy(batchSize = x))
        .text("Batch size for timing control (default: 10000)")

      opt[Unit]("max-speed")
        .optional()
        .action((_, c) => c.copy(maxSpeed = true))
        .text("Max speed mode (ignore timing)")
    }

    parser.parse(args, Config()) match {
      case Some(config) =>
        val spark = createSparkSession()
        try {
          if (config.maxSpeed) {
            replayMaxSpeed(spark, config)
          } else {
            replayWithTiming(spark, config)
          }
        } finally {
          spark.stop()
        }
      case None =>
        System.exit(1)
    }
  }

  def createSparkSession(): SparkSession = {
    SparkSession.builder()
      .appName("Kafka Direct Replayer")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .enableHiveSupport()
      .getOrCreate()
  }

  /**
   * 시간 간격을 제어하며 Kafka로 리플레이
   */
  def replayWithTiming(spark: SparkSession, config: Config): Unit = {
    import spark.implicits._

    println(s"[*] Reading from ${config.sourceTable} for date ${config.targetDate}")
    println(s"[*] Speed factor: ${config.speedFactor}x")
    println(s"[*] Batch size: ${config.batchSize} events")

    // 1. Hive에서 데이터 읽기 및 정렬
    val df = spark.table(config.sourceTable)
      .filter($"dt" === config.targetDate)
      .select(
        $"event_key".as("key"),
        $"event_time".as("event_time_ms"),
        $"payload".as("value")
      )
      .orderBy($"event_time_ms")

    val totalCount = df.count()
    println(s"[*] Total events: ${totalCount}")

    // 2. 데이터를 collect하여 배치 처리
    // 주의: 메모리에 올라갈 수 있는 크기여야 함
    // 대용량의 경우 toLocalIterator() 사용
    val startWallTime = System.currentTimeMillis()
    var baseEventTime: Option[Long] = None
    var prevTime: Option[Long] = None
    var totalSent = 0
    var batchNum = 0

    // 파티션별로 처리
    val numPartitions = Math.ceil(totalCount.toDouble / config.batchSize).toInt
    val partitionedDF = df.repartition(numPartitions)

    partitionedDF.foreachPartition { partition =>
      val rows = partition.toArray
      if (rows.nonEmpty) {
        val batchDF = spark.createDataFrame(
          spark.sparkContext.parallelize(rows),
          df.schema
        )

        // Kafka 전송
        sendBatchToKafka(batchDF, config.kafkaBootstrap, config.kafkaTopic)

        // 시간 간격 계산
        val firstEventTime = rows.head.getLong(rows.head.fieldIndex("event_time_ms"))
        val lastEventTime = rows.last.getLong(rows.last.fieldIndex("event_time_ms"))

        synchronized {
          if (baseEventTime.isEmpty) {
            baseEventTime = Some(firstEventTime)
            prevTime = Some(firstEventTime)
          }

          prevTime.foreach { prev =>
            val originalInterval = lastEventTime - prev
            val adjustedInterval = (originalInterval / config.speedFactor / 1000.0).toLong
            val sleepTime = Math.max(0, Math.min(adjustedInterval, 60))

            if (sleepTime > 0) {
              batchNum += 1
              println(s"[${batchNum}] Sent ${rows.length} events, " +
                s"waiting ${sleepTime}s (original: ${originalInterval / 1000.0}s)")
              Thread.sleep(sleepTime * 1000)
            }
          }

          prevTime = Some(lastEventTime)
          totalSent += rows.length

          val progress = (totalSent.toDouble / totalCount * 100)
          val elapsed = (System.currentTimeMillis() - startWallTime) / 1000.0
          println(f"[*] Progress: $progress%.1f%% ($totalSent%,d/$totalCount%,d), Elapsed: $elapsed%.1fs")
        }
      }
    }

    val totalElapsed = (System.currentTimeMillis() - startWallTime) / 1000.0
    println(f"\n[✓] Replay completed!")
    println(f"    Total events: $totalSent%,d")
    println(f"    Total time: $totalElapsed%.1fs")
    println(f"    Avg throughput: ${totalSent / totalElapsed}%.0f events/sec")
  }

  /**
   * 최대 속도로 Kafka에 전송 (시간 간격 무시)
   */
  def replayMaxSpeed(spark: SparkSession, config: Config): Unit = {
    import spark.implicits._

    println(s"[*] Max speed replay mode")
    println(s"[*] Reading from ${config.sourceTable} for date ${config.targetDate}")

    val startTime = System.currentTimeMillis()

    // 간단한 방법: DataFrame을 직접 Kafka에 쓰기
    spark.table(config.sourceTable)
      .filter($"dt" === config.targetDate)
      .select(
        $"event_key".cast("string").as("key"),
        $"payload".cast("binary").as("value")
      )
      .orderBy($"event_time")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", config.kafkaBootstrap)
      .option("topic", config.kafkaTopic)
      .save()

    val elapsed = (System.currentTimeMillis() - startTime) / 1000.0
    println(f"[✓] Max speed replay completed in $elapsed%.1fs")
  }

  /**
   * 배치를 Kafka로 전송
   */
  def sendBatchToKafka(df: DataFrame, kafkaBootstrap: String, topic: String): Unit = {
    import df.sparkSession.implicits._

    df.select(
      $"key".cast("string"),
      $"value".cast("binary")
    )
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrap)
      .option("topic", topic)
      .save()
  }
}
