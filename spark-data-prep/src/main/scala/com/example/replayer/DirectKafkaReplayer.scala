package com.example.replayer

import com.example.replayer.config.ReplayerConfig
import com.example.replayer.pipeline.{HiveSource, Transformer}
import com.example.replayer.pipeline.sink.{MaxSpeedSink, TimeSyncedSink}
import com.example.replayer.serialization.SerializationFactory
import org.apache.spark.sql.SparkSession

/**
 * Spark에서 직접 Kafka로 리플레이 (Main Entry Point)
 *
 * Source-Transformer-Sink 패턴 적용
 * - Source: HiveSource (Read from Hive)
 * - Transformer: Transformer (Schema Check & Serialization)
 * - Sink: KafkaSink (MaxSpeedSink or TimeSyncedSink)
 */
object DirectKafkaReplayer {

  def main(args: Array[String]): Unit = {
    ReplayerConfig.parse(args) match {
      case Some(config) =>
        val spark = createSparkSession()
        try {
          println(s"[*] Reading from ${config.sourceTable} for date ${config.targetDate}")
          println(s"[*] Serialization format: ${config.serializationFormat}")
          
          val strategy = SerializationFactory.createStrategy(config)
          try {
            // 1. Source
            val rawDf = HiveSource.read(spark, config)

            // 2. Transform (include timestamp for timing control)
            // TimeSyncedSink requires timestamp, MaxSpeedSink does not strictly require it but no harm
            val includeTimestamp = !config.maxSpeed
            val preparedDf = Transformer.transform(spark, rawDf, config, strategy, includeTimestamp)

            // 3. Sink
            if (config.maxSpeed) {
              MaxSpeedSink.write(spark, preparedDf, config)
            } else {
              TimeSyncedSink.write(spark, preparedDf, config)
            }
          } finally {
            strategy.cleanup()
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
}