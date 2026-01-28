package com.example.replayer

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.example.replayer.serialization._
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
    maxSpeed: Boolean = false,
    // Serialization configuration
    serializationFormat: String = "binary",
    schemaRegistryUrl: Option[String] = None,
    schemaName: Option[String] = None,
    keyColumn: Option[String] = Some("event_key"),
    excludeColumns: Seq[String] = Seq("dt")
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

      opt[String]("serialization-format")
        .optional()
        .action((x, c) => c.copy(serializationFormat = x))
        .text("Serialization format: binary (default), avro")
        .validate(x =>
          if (Seq("binary", "avro").contains(x.toLowerCase)) success
          else failure("Format must be: binary or avro"))

      opt[String]("schema-registry-url")
        .optional()
        .action((x, c) => c.copy(schemaRegistryUrl = Some(x)))
        .text("Apicurio Schema Registry URL (required for avro)")

      opt[String]("schema-name")
        .optional()
        .action((x, c) => c.copy(schemaName = Some(x)))
        .text("Schema name/subject (default: <table>.value)")

      opt[String]("key-column")
        .optional()
        .action((x, c) => c.copy(keyColumn = Some(x)))
        .text("Column for Kafka key (default: event_key)")

      opt[Seq[String]]("exclude-columns")
        .optional()
        .action((x, c) => c.copy(excludeColumns = x))
        .text("Columns to exclude from value (default: dt)")
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
   * Prepare DataFrame for Kafka write with configurable serialization
   * @param includeEventTime If true, includes event_time column for timing control
   */
  def prepareDataFrame(
    spark: SparkSession,
    config: Config,
    strategy: SerializationStrategy,
    includeEventTime: Boolean = false
  ): DataFrame = {
    import spark.implicits._

    // 1. Read from Hive
    val rawDf = spark.table(config.sourceTable)
      .filter($"dt" === config.targetDate)

    // 2. Validate key column exists
    val keyCol = config.keyColumn.getOrElse("event_key")
    require(rawDf.columns.contains(keyCol),
      s"Key column '$keyCol' not found. Available: ${rawDf.columns.mkString(", ")}")

    // 3. Select value columns (exclude key and excluded columns)
    val valueColumns = rawDf.columns
      .filterNot(c => config.excludeColumns.contains(c))
      .filterNot(c => c == keyCol)
      .filterNot(c => includeEventTime && c == "event_time") // Exclude event_time from value if needed separately

    // 4. Initialize serialization (fetch schema from registry)
    val schemaName = SerializationFactory.deriveSchemaName(config)
    strategy.initialize(schemaName)

    // 5. Serialize value columns
    val dfForValue = rawDf.select(valueColumns.map(col).toIndexedSeq: _*)
    val serializedDf = strategy.prepareForKafka(dfForValue, dfForValue.schema, schemaName)

    // 6. Add key column and optionally event_time
    val dfWithKey = if (includeEventTime && rawDf.columns.contains("event_time")) {
      rawDf.select(
        col(keyCol).cast("string").as("key"),
        col("event_time").as("event_time_ms")
      ).withColumn("row_id", monotonically_increasing_id())
    } else {
      rawDf.select(col(keyCol).cast("string").as("key"))
        .withColumn("row_id", monotonically_increasing_id())
    }

    // 7. Combine key + value (using row_id to ensure correct join)
    val serializedDfWithId = serializedDf.withColumn("row_id", monotonically_increasing_id())

    val result = dfWithKey.join(serializedDfWithId, "row_id")

    if (includeEventTime && rawDf.columns.contains("event_time")) {
      result.select($"key", $"value", $"event_time_ms")
    } else {
      result.select($"key", $"value")
    }
  }

  /**
   * 시간 간격을 제어하며 Kafka로 리플레이
   */
  def replayWithTiming(spark: SparkSession, config: Config): Unit = {
    import spark.implicits._

    println(s"[*] Reading from ${config.sourceTable} for date ${config.targetDate}")
    println(s"[*] Serialization format: ${config.serializationFormat}")
    println(s"[*] Speed factor: ${config.speedFactor}x")
    println(s"[*] Batch size: ${config.batchSize} events")

    val strategy = SerializationFactory.createStrategy(config)

    try {
      // 1. Prepare DataFrame with serialization (include event_time for timing control)
      val df = prepareDataFrame(spark, config, strategy, includeEventTime = true)
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

      partitionedDF.foreachPartition { (partition: Iterator[org.apache.spark.sql.Row]) =>
        val rows = partition.toSeq
        if (rows.nonEmpty) {
          val batchDF = spark.createDataFrame(
            spark.sparkContext.parallelize(rows),
            df.schema
          )

          // Kafka 전송 (key, value 컬럼만 전송)
          sendBatchToKafka(batchDF.select($"key", $"value"), config.kafkaBootstrap, config.kafkaTopic)

          // 시간 간격 계산
          val firstEventTime = rows.head.getLong(rows.head.fieldIndex("event_time_ms"))
          val lastEventTime = rows.last.getLong(rows.last.fieldIndex("event_time_ms"))

          this.synchronized {
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
    } finally {
      strategy.cleanup()
    }
  }

  /**
   * 최대 속도로 Kafka에 전송 (시간 간격 무시)
   */
  def replayMaxSpeed(spark: SparkSession, config: Config): Unit = {
    import spark.implicits._

    println(s"[*] Max speed replay mode")
    println(s"[*] Serialization format: ${config.serializationFormat}")
    println(s"[*] Reading from ${config.sourceTable} for date ${config.targetDate}")

    val startTime = System.currentTimeMillis()
    val strategy = SerializationFactory.createStrategy(config)

    try {
      val preparedDf = prepareDataFrame(spark, config, strategy)

      preparedDf
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", config.kafkaBootstrap)
        .option("topic", config.kafkaTopic)
        .save()

      val elapsed = (System.currentTimeMillis() - startTime) / 1000.0
      println(f"[✓] Max speed replay completed in $elapsed%.1fs")
    } finally {
      strategy.cleanup()
    }
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
