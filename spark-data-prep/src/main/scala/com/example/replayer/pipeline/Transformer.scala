package com.example.replayer.pipeline

import com.example.replayer.config.Config
import com.example.replayer.serialization.{SerializationFactory, SerializationStrategy}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Transformer {

  /**
   * Prepare DataFrame for Kafka write with configurable serialization
   * @param includeTimestamp If true, includes timestamp column for timing control
   */
  def transform(
    spark: SparkSession,
    rawDf: DataFrame,
    config: Config,
    strategy: SerializationStrategy,
    includeTimestamp: Boolean = false
  ): DataFrame = {
    import spark.implicits._

    // 1. Auto-detect or validate key column
    val keyCol = config.keyColumn.getOrElse(SchemaAnalyzer.detectKeyColumn(rawDf))
    require(rawDf.columns.contains(keyCol),
      s"Key column '$keyCol' not found. Available: ${rawDf.columns.mkString(", ")}")

    println(s"[*] Using key column: $keyCol")

    // 2. Auto-detect or validate timestamp column if needed
    val timestampCol = if (includeTimestamp) {
      val col = config.timestampColumn.getOrElse(SchemaAnalyzer.detectTimestampColumn(rawDf))
      require(rawDf.columns.contains(col),
        s"Timestamp column '$col' not found. Available: ${rawDf.columns.mkString(", ")}")
      println(s"[*] Using timestamp column: $col")
      col
    } else ""

    // 3. Select value columns (exclude key, timestamp, and excluded columns)
    val valueColumns = rawDf.columns
      .filterNot(c => config.excludeColumns.contains(c))
      .filterNot(c => c == keyCol)
      .filterNot(c => includeTimestamp && c == timestampCol) // Exclude timestamp from value if needed separately

    // 4. Initialize serialization (fetch schema from registry)
    val schemaName = SerializationFactory.deriveSchemaName(config)
    strategy.initialize(schemaName)

    // 5. Serialize value columns
    val dfForValue = rawDf.select(valueColumns.map(col).toIndexedSeq: _*)
    val serializedDf = strategy.prepareForKafka(dfForValue, dfForValue.schema, schemaName)

    // 6. Add key column and optionally timestamp
    val dfWithKey = if (includeTimestamp) {
      // Convert timestamp to milliseconds (handle both TimestampType and LongType)
      val timestampExpr = rawDf.schema(timestampCol).dataType match {
        case _: org.apache.spark.sql.types.TimestampType =>
          (unix_timestamp(col(timestampCol)) * 1000).cast("long")
        case _: org.apache.spark.sql.types.LongType =>
          col(timestampCol)
        case other =>
          throw new IllegalArgumentException(s"Timestamp column '$timestampCol' has unsupported type: $other")
      }

      rawDf.select(
        col(keyCol).cast("string").as("key"),
        timestampExpr.as("event_time_ms")
      ).withColumn("row_id", monotonically_increasing_id())
    } else {
      rawDf.select(col(keyCol).cast("string").as("key"))
        .withColumn("row_id", monotonically_increasing_id())
    }

    // 7. Combine key + value (using row_id to ensure correct join)
    val serializedDfWithId = serializedDf.withColumn("row_id", monotonically_increasing_id())

    val result = dfWithKey.join(serializedDfWithId, "row_id")

    if (includeTimestamp) {
      result.select($"key", $"value", $"event_time_ms")
    } else {
      result.select($"key", $"value")
    }
  }
}
