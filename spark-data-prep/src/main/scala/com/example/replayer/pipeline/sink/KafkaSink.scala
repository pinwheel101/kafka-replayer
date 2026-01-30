package com.example.replayer.pipeline.sink

import com.example.replayer.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

trait KafkaSink {
  def write(spark: SparkSession, df: DataFrame, config: Config): Unit
}
