package com.example.replayer.serialization

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import java.net.{HttpURLConnection, URL}
import scala.io.Source

/**
 * Protobuf serialization using schema fetched from Apicurio Schema Registry 3.x
 *
 * NOTE: This is a simplified implementation that converts DataFrame to JSON first.
 * For production use with true Protobuf binary serialization, you would need:
 * 1. Pre-compiled .proto files with protoc
 * 2. Generated Java classes from .proto
 * 3. Or use spark-protobuf with descriptor files
 *
 * This implementation fetches schema from registry but uses JSON encoding.
 * This demonstrates the registry integration pattern while avoiding protoc dependency.
 */
class ProtobufSerializationStrategy(registryUrl: String) extends SerializationStrategy {

  private var protoSchema: Option[String] = None

  override def initialize(schemaName: String): Unit = {
    try {
      // Use v2 API for compatibility
      val url = new URL(s"$registryUrl/apis/registry/v2/groups/default/artifacts/$schemaName")
      val conn = url.openConnection().asInstanceOf[HttpURLConnection]
      conn.setRequestMethod("GET")
      conn.setRequestProperty("Accept", "application/x-protobuf")

      val code = conn.getResponseCode
      if (code == 200) {
        val schemaContent = Source.fromInputStream(conn.getInputStream).mkString
        conn.disconnect()

        protoSchema = Some(schemaContent)
        println(s"[Schema Registry] Fetched Protobuf schema: $schemaName")
        println(s"[Schema Registry] Note: Using JSON encoding (protoc-free implementation)")
      } else {
        val errorMsg = if (code >= 400) Source.fromInputStream(conn.getErrorStream).mkString else ""
        conn.disconnect()
        throw new RuntimeException(s"Failed to fetch Protobuf schema '$schemaName': HTTP $code - $errorMsg")
      }
    } catch {
      case e: Exception =>
        println(s"[Schema Registry] Failed to fetch Protobuf schema '$schemaName': ${e.getClass.getName}: ${e.getMessage}")
        throw e
    }
  }

  override def prepareForKafka(df: DataFrame, schema: StructType, schemaName: String): DataFrame = {
    import df.sparkSession.implicits._

    protoSchema.getOrElse(
      throw new IllegalStateException("Schema not initialized. Call initialize() first.")
    )

    // Simplified implementation: Convert to JSON
    // In production, you would use compiled Protobuf messages
    df.select(
      to_json(struct(df.columns.map(col).toIndexedSeq: _*))
        .cast("binary")
        .as("value")
    )
  }

  override def cleanup(): Unit = {
    // No cleanup needed
  }
}


