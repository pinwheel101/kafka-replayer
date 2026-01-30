package com.example.replayer.config

import scopt.OParser

case class Config(
  sourceTable: String = "",
  targetDate: String = "",
  kafkaBootstrap: String = "",
  kafkaTopic: String = "",
  speedFactor: Double = 1.0,
  batchSize: Int = 10000,
  maxSpeed: Boolean = false,
  // Serialization configuration
  serializationFormat: String = "avro",
  schemaRegistryUrl: Option[String] = None,
  schemaName: Option[String] = None,
  keyColumn: Option[String] = None,  // Auto-detect if not specified
  timestampColumn: Option[String] = None,  // Auto-detect if not specified
  excludeColumns: Seq[String] = Seq("dt", "fab")  // Partition columns
)

object ReplayerConfig {

  def parse(args: Array[String]): Option[Config] = {
    val builder = OParser.builder[Config]
    val parser = {
      import builder._
      OParser.sequence(
        programName("DirectKafkaReplayer"),
        head("DirectKafkaReplayer", "1.0"),
        opt[String]("source-table")
          .required()
          .action((x, c) => c.copy(sourceTable = x))
          .text("Hive source table (e.g., mydb.events)"),
        opt[String]("target-date")
          .required()
          .action((x, c) => c.copy(targetDate = x))
          .text("Target date (YYYY-MM-DD)"),
        opt[String]("kafka-bootstrap")
          .required()
          .action((x, c) => c.copy(kafkaBootstrap = x))
          .text("Kafka bootstrap servers"),
        opt[String]("topic")
          .required()
          .action((x, c) => c.copy(kafkaTopic = x))
          .text("Kafka topic"),
        opt[Double]("speed")
          .optional()
          .action((x, c) => c.copy(speedFactor = x))
          .text("Replay speed factor (default: 1.0)"),
        opt[Int]("batch-size")
          .optional()
          .action((x, c) => c.copy(batchSize = x))
          .text("Batch size for timing control (default: 10000)"),
        opt[Unit]("max-speed")
          .optional()
          .action((_, c) => c.copy(maxSpeed = true))
          .text("Max speed mode (ignore timing)"),
        opt[String]("serialization-format")
          .optional()
          .action((x, c) => c.copy(serializationFormat = x))
          .text("Serialization format: avro (default), protobuf")
          .validate(x =>
            if (Seq("avro", "protobuf").contains(x.toLowerCase)) success
            else failure("Format must be: avro or protobuf")),
        opt[String]("schema-registry-url")
          .optional()
          .action((x, c) => c.copy(schemaRegistryUrl = Some(x)))
          .text("Apicurio Schema Registry URL (required for avro)"),
        opt[String]("schema-name")
          .optional()
          .action((x, c) => c.copy(schemaName = Some(x)))
          .text("Schema name/subject (default: <table>.value)"),
        opt[String]("key-column")
          .optional()
          .action((x, c) => c.copy(keyColumn = Some(x)))
          .text("Column for Kafka key (auto-detect: lot_id, event_key, or first column)"),
        opt[String]("timestamp-column")
          .optional()
          .action((x, c) => c.copy(timestampColumn = Some(x)))
          .text("Timestamp column for replay timing (auto-detect: ts, event_time, or first timestamp column)"),
        opt[Seq[String]]("exclude-columns")
          .optional()
          .action((x, c) => c.copy(excludeColumns = x))
          .text("Columns to exclude from value (default: dt)")
      )
    }

    OParser.parse(parser, args, Config())
  }
}
