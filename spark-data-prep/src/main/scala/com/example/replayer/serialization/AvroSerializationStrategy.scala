package com.example.replayer.serialization

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.avro.functions.to_avro
import org.apache.spark.sql.functions._
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import io.apicurio.registry.rest.client.RegistryClientFactory
import io.apicurio.registry.types.ArtifactType
import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import scala.util.{Try, Success, Failure}

/**
 * Avro serialization with Apicurio Schema Registry
 */
class AvroSerializationStrategy(registryUrl: String) extends SerializationStrategy {

  private val registryClient = RegistryClientFactory.create(registryUrl)
  private var avroSchema: Option[Schema] = None

  override def initialize(schema: StructType, schemaName: String): Unit = {
    // Generate Avro schema from Spark schema
    val avroSchemaObj = convertSparkSchemaToAvro(schema, schemaName)
    avroSchema = Some(avroSchemaObj)

    // Register schema with Apicurio
    Try {
      val schemaContent = new ByteArrayInputStream(avroSchemaObj.toString.getBytes(StandardCharsets.UTF_8))
      val metadata = registryClient.createArtifact(
        "default", // group
        schemaName,
        schemaContent
      )
      println(s"[Schema Registry] Registered Avro schema: $schemaName (version: ${metadata.getVersion})")
    } match {
      case Success(_) =>
        // Schema registered successfully
      case Failure(e) =>
        println(s"[Schema Registry] Schema already exists or registration warning: ${e.getMessage}")
        // Continue - schema might already be registered
    }
  }

  override def prepareForKafka(df: DataFrame, schema: StructType, schemaName: String): DataFrame = {
    import df.sparkSession.implicits._

    val avroSchemaStr = avroSchema.getOrElse(
      throw new IllegalStateException("Schema not initialized. Call initialize() first.")
    ).toString

    // Convert entire row to struct, then to Avro
    df.select(
      to_avro(struct(df.columns.map(col): _*), avroSchemaStr).as("value")
    )
  }

  private def convertSparkSchemaToAvro(sparkSchema: StructType, recordName: String): Schema = {
    // Simple type mapping - assumes most fields are strings
    val schemaBuilder = SchemaBuilder.record(recordName).namespace("com.example.replayer")
    val fieldsBuilder = schemaBuilder.fields()

    sparkSchema.fields.foreach { field =>
      field.dataType.typeName match {
        case "string" =>
          if (field.nullable) fieldsBuilder.optionalString(field.name)
          else fieldsBuilder.requiredString(field.name)
        case "long" =>
          if (field.nullable) fieldsBuilder.optionalLong(field.name)
          else fieldsBuilder.requiredLong(field.name)
        case "integer" =>
          if (field.nullable) fieldsBuilder.optionalInt(field.name)
          else fieldsBuilder.requiredInt(field.name)
        case "double" =>
          if (field.nullable) fieldsBuilder.optionalDouble(field.name)
          else fieldsBuilder.requiredDouble(field.name)
        case "boolean" =>
          if (field.nullable) fieldsBuilder.optionalBoolean(field.name)
          else fieldsBuilder.requiredBoolean(field.name)
        case "timestamp" =>
          // Timestamp as long (milliseconds)
          if (field.nullable) fieldsBuilder.optionalLong(field.name)
          else fieldsBuilder.requiredLong(field.name)
        case other =>
          // Default to string for complex types
          println(s"[Schema] Converting unsupported type ${other} to string for field ${field.name}")
          if (field.nullable) fieldsBuilder.optionalString(field.name)
          else fieldsBuilder.requiredString(field.name)
      }
    }

    fieldsBuilder.endRecord()
  }

  override def cleanup(): Unit = {
    // No cleanup needed - HTTP client auto-closes
  }
}
