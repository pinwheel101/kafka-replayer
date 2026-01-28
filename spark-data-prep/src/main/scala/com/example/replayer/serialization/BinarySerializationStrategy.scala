package com.example.replayer.serialization

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._

/**
 * Legacy binary serialization - simple cast to binary
 * Assumes a "payload" column exists with pre-serialized data
 */
class BinarySerializationStrategy extends SerializationStrategy {
  override def prepareForKafka(df: DataFrame, schema: StructType, schemaName: String): DataFrame = {
    import df.sparkSession.implicits._

    // Backward compatibility: look for "payload" column
    if (df.columns.contains("payload")) {
      df.select($"payload".cast("binary").as("value"))
    } else {
      throw new IllegalArgumentException(
        "Binary format requires a 'payload' column. " +
        "Use --serialization-format avro for full row serialization."
      )
    }
  }

  override def initialize(schemaName: String): Unit = {
    // No initialization needed for binary
  }
}
