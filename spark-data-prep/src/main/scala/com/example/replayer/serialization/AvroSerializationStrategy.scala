package com.example.replayer.serialization

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.avro.functions.to_avro
import org.apache.spark.sql.functions._
import org.apache.avro.Schema
import java.net.{HttpURLConnection, URL}
import scala.io.Source

/**
 * Avro serialization using schema fetched from Apicurio Schema Registry 3.x
 */
class AvroSerializationStrategy(registryUrl: String) extends SerializationStrategy {

  private var avroSchema: Option[Schema] = None

  override def initialize(schemaName: String): Unit = {
    try {
      // Use v2 API for compatibility
      val url = new URL(s"$registryUrl/apis/registry/v2/groups/default/artifacts/$schemaName")
      val conn = url.openConnection().asInstanceOf[HttpURLConnection]
      conn.setRequestMethod("GET")
      conn.setRequestProperty("Accept", "application/json")

      val code = conn.getResponseCode
      if (code == 200) {
        val schemaJson = Source.fromInputStream(conn.getInputStream).mkString
        conn.disconnect()

        avroSchema = Some(new Schema.Parser().parse(schemaJson))
        println(s"[Schema Registry] Fetched Avro schema: $schemaName")
      } else {
        val errorMsg = if (code >= 400) Source.fromInputStream(conn.getErrorStream).mkString else ""
        conn.disconnect()
        throw new RuntimeException(s"Failed to fetch schema '$schemaName': HTTP $code - $errorMsg")
      }
    } catch {
      case e: Exception =>
        println(s"[Schema Registry] Failed to fetch schema '$schemaName': ${e.getClass.getName}: ${e.getMessage}")
        throw e
    }
  }

  override def prepareForKafka(df: DataFrame, schema: StructType, schemaName: String): DataFrame = {
    import df.sparkSession.implicits._

    val avroSchemaStr = avroSchema.getOrElse(
      throw new IllegalStateException("Schema not initialized. Call initialize() first.")
    ).toString

    df.select(
      to_avro(struct(df.columns.map(col).toIndexedSeq: _*), avroSchemaStr).as("value")
    )
  }

  override def cleanup(): Unit = {
    // No cleanup needed for HTTP client
  }
}
