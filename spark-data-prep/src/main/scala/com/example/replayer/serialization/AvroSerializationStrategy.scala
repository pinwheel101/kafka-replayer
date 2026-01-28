package com.example.replayer.serialization

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.avro.functions.to_avro
import org.apache.spark.sql.functions._
import org.apache.avro.Schema
import io.apicurio.registry.rest.client.RegistryClient
import io.kiota.http.vertx.VertXRequestAdapter
import io.vertx.core.Vertx
import scala.io.Source

/**
 * Avro serialization using schema fetched from Apicurio Schema Registry 3.x
 */
class AvroSerializationStrategy(registryUrl: String) extends SerializationStrategy {

  private val vertx = Vertx.vertx()
  private val requestAdapter = {
    val adapter = new VertXRequestAdapter(vertx)
    adapter.setBaseUrl(s"$registryUrl/apis/registry/v3")
    adapter
  }
  private val registryClient = new RegistryClient(requestAdapter)
  private var avroSchema: Option[Schema] = None

  override def initialize(schemaName: String): Unit = {
    val inputStream = registryClient
      .groups().byGroupId("default")
      .artifacts().byArtifactId(schemaName)
      .versions().byVersionExpression("latest")
      .content().get()

    val schemaJson = Source.fromInputStream(inputStream).mkString
    inputStream.close()

    avroSchema = Some(new Schema.Parser().parse(schemaJson))
    println(s"[Schema Registry] Fetched Avro schema: $schemaName")
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
    vertx.close()
  }
}
