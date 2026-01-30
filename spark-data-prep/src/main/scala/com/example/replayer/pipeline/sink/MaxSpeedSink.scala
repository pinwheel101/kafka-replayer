package com.example.replayer.pipeline.sink

import com.example.replayer.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

object MaxSpeedSink extends KafkaSink {
  override def write(spark: SparkSession, df: DataFrame, config: Config): Unit = {
    import spark.implicits._

    println(s"[*] Max speed replay mode")
    
    val startTime = System.currentTimeMillis()

    df.select(
      $"key".cast("string"),
      $"value".cast("binary")
    )
    .write
    .format("kafka")
    .option("kafka.bootstrap.servers", config.kafkaBootstrap)
    .option("topic", config.kafkaTopic)
    .save()

    val elapsed = (System.currentTimeMillis() - startTime) / 1000.0
    println(f"[✓] Max speed replay completed in $elapsed%.1fs")
  }
}
