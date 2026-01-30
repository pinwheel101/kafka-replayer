package com.example.replayer.pipeline

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{LongType, TimestampType}

object SchemaAnalyzer {

  /**
   * Auto-detect key column from DataFrame
   * Priority: lot_id > event_key > first non-timestamp column
   */
  def detectKeyColumn(df: DataFrame): String = {
    val columns = df.columns
    if (columns.contains("lot_id")) "lot_id"
    else if (columns.contains("event_key")) "event_key"
    else if (columns.contains("eqp_id")) "eqp_id"
    else columns.head
  }

  /**
   * Auto-detect timestamp column from DataFrame
   * Priority: ts > event_time > create_dtts > start_dtts > first timestamp column
   */
  def detectTimestampColumn(df: DataFrame): String = {
    val columns = df.columns
    val schema = df.schema

    // Check priority columns
    if (columns.contains("ts")) return "ts"
    if (columns.contains("event_time")) return "event_time"
    if (columns.contains("create_dtts")) return "create_dtts"
    if (columns.contains("start_dtts")) return "start_dtts"

    // Find first timestamp column
    schema.fields.find { field =>
      field.dataType match {
        case _: TimestampType | _: LongType => columns.contains(field.name)
        case _ => false
      }
    }.map(_.name).getOrElse {
      throw new IllegalArgumentException(s"No timestamp column found in table. Available columns: ${columns.mkString(", ")}")
    }
  }
}
