package com.example.replayer.pipeline

import com.example.replayer.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

object HiveSource {

  def read(spark: SparkSession, config: Config): DataFrame = {
    import spark.implicits._

    // Read from Hive and filter by target date
    spark.table(config.sourceTable)
      .filter($"dt" === config.targetDate)
  }
}
