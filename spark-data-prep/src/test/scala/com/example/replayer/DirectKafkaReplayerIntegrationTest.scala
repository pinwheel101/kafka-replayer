package com.example.replayer

import com.dimafeng.testcontainers.{ForAllTestContainer, KafkaContainer}
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import org.testcontainers.utility.DockerImageName
import com.example.replayer.serialization._
import com.example.replayer.config.Config

/**
 * Integration test for DirectKafkaReplayer using Testcontainers
 *
 * Note: This requires Docker to be running
 */
class DirectKafkaReplayerIntegrationTest
  extends AnyFlatSpec
  with Matchers
  with BeforeAndAfterAll
  with ForAllTestContainer {

  override val container: KafkaContainer = KafkaContainer(
    DockerImageName.parse("confluentinc/cp-kafka:7.6.0")
  )

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Create Spark session for testing
    spark = SparkSession.builder()
      .appName("DirectKafkaReplayerTest")
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    super.afterAll()
  }

  it should "throw exception for avro without registry URL" in {
    val config = Config(
      sourceTable = "test.table",
      targetDate = "2024-01-01",
      kafkaBootstrap = "localhost:9092",
      kafkaTopic = "test",
      serializationFormat = "avro",
      schemaRegistryUrl = None
    )

    assertThrows[IllegalArgumentException] {
      SerializationFactory.createStrategy(config)
    }
  }

  it should "derive schema name from table name" in {
    val testConfig: Config = Config(
      sourceTable = "mydb.events",
      targetDate = "2024-01-01",
      kafkaBootstrap = "localhost:9092",
      kafkaTopic = "test"
    )

    val schemaName = SerializationFactory.deriveSchemaName(testConfig)
    schemaName shouldEqual "events.value"
  }

  it should "use custom schema name if provided" in {
    val testConfig: Config = Config(
      sourceTable = "mydb.events",
      targetDate = "2024-01-01",
      kafkaBootstrap = "localhost:9092",
      kafkaTopic = "test",
      schemaName = Some("custom.schema")
    )

    val schemaName = SerializationFactory.deriveSchemaName(testConfig)
    schemaName shouldEqual "custom.schema"
  }

  "Kafka integration" should "write messages to Kafka topic" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val bootstrapServers = container.bootstrapServers
    val topic = "test-topic"

    // Create test data
    val df = Seq(
      ("key1", "value1".getBytes),
      ("key2", "value2".getBytes)
    ).toDF("key", "value")

    // Write to Kafka
    df.write
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("topic", topic)
      .save()

    // Read back from Kafka to verify
    val result = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()

    result.count() shouldEqual 2
  }
}