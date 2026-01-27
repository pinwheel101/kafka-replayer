package com.example.replayer.serialization

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

/**
 * Strategy pattern for different serialization formats
 */
trait SerializationStrategy {
  /**
   * Prepare DataFrame for Kafka write by adding serialized "value" column
   * @param df Input DataFrame with all columns
   * @param schema Spark schema of the data to serialize
   * @param schemaName Logical name for the schema (e.g., "events.value")
   * @return DataFrame with "value" column ready for Kafka
   */
  def prepareForKafka(df: DataFrame, schema: StructType, schemaName: String): DataFrame

  /**
   * Initialize resources (e.g., register schema with registry)
   * @param schema Spark schema to register
   * @param schemaName Schema subject name
   */
  def initialize(schema: StructType, schemaName: String): Unit

  /**
   * Cleanup resources
   */
  def cleanup(): Unit = {}
}
