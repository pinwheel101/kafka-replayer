package com.example.replayer.serialization

import com.example.replayer.config.Config

/**
 * Factory to create appropriate serialization strategy based on configuration
 */
object SerializationFactory {

  def createStrategy(config: Config): SerializationStrategy = {
    config.serializationFormat.toLowerCase match {

      case "avro" =>
        val registryUrl = config.schemaRegistryUrl.getOrElse(
          throw new IllegalArgumentException("--schema-registry-url is required for Avro serialization")
        )
        new AvroSerializationStrategy(registryUrl)

      case "protobuf" =>
        val registryUrl = config.schemaRegistryUrl.getOrElse(
          throw new IllegalArgumentException("--schema-registry-url is required for Protobuf serialization")
        )
        new ProtobufSerializationStrategy(registryUrl)

      case unknown =>
        throw new IllegalArgumentException(
          s"Unknown serialization format: $unknown. Use one of: avro, protobuf"
        )
    }
  }

  /**
   * Generate schema name from table name if not provided
   */
  def deriveSchemaName(config: Config): String = {
    config.schemaName.getOrElse {
      // Extract table name: "mydb.events" -> "events"
      val tableName = config.sourceTable.split('.').last
      s"${tableName}.value"
    }
  }
}
