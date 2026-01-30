package com.example.replayer.pipeline.sink

import com.example.replayer.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

object TimeSyncedSink extends KafkaSink {

  override def write(spark: SparkSession, df: DataFrame, config: Config): Unit = {
    import spark.implicits._

    println(s"[*] Speed factor: ${config.speedFactor}x")
    println(s"[*] Batch size: ${config.batchSize} events")

    // Ensure data is sorted by event time
    val sortedDf = df.orderBy($"event_time_ms")

    val totalCount = sortedDf.count()
    println(s"[*] Total events: ${totalCount}")

    val startWallTime = System.currentTimeMillis()
    var baseEventTime: Option[Long] = None
    var prevTime: Option[Long] = None
    var totalSent = 0
    var batchNum = 0

    // Repartition for batch processing
    val numPartitions = Math.ceil(totalCount.toDouble / config.batchSize).toInt
    val partitionedDF = sortedDf.repartition(numPartitions)

    partitionedDF.foreachPartition { (partition: Iterator[org.apache.spark.sql.Row]) =>
      val rows = partition.toSeq
      if (rows.nonEmpty) {
        val batchDF = spark.createDataFrame(
          spark.sparkContext.parallelize(rows),
          sortedDf.schema
        )

        // Send batch to Kafka
        sendBatchToKafka(batchDF.select($"key", $"value"), config.kafkaBootstrap, config.kafkaTopic)

        // Calculate timing
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
    println(s"\n[✓] Replay completed!")
    println(s"    Total events: $totalSent%,d")
    println(s"    Total time: $totalElapsed%.1fs")
    println(f"    Avg throughput: ${totalSent / totalElapsed}%.0f events/sec")
  }

  /**
   * Helper to send batch to Kafka
   */
  private def sendBatchToKafka(df: DataFrame, kafkaBootstrap: String, topic: String): Unit = {
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
