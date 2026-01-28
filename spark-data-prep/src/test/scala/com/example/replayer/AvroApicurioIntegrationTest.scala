package com.example.replayer

import com.dimafeng.testcontainers.{GenericContainer, KafkaContainer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import org.testcontainers.containers.Network
import org.testcontainers.utility.DockerImageName
import com.example.replayer.serialization.AvroSerializationStrategy
import java.net.{HttpURLConnection, URL}
import scala.io.Source

/**
 * Avro + Apicurio Schema Registry 3.x integration test
 *
 * Requires Docker to be running.
 * Starts Kafka and Apicurio on a shared network, registers a test schema,
 * then verifies the full serialization and Kafka publish flow.
 */
class AvroApicurioIntegrationTest
  extends AnyFlatSpec
  with Matchers
  with BeforeAndAfterAll {

  private val network = Network.newNetwork()

  private val kafka = KafkaContainer(
    DockerImageName.parse("confluentinc/cp-kafka:8.0.3")
  )

  private val apicurio = GenericContainer(
    dockerImage = "apicurio/apicurio-registry:3.1.6",
    exposedPorts = Seq(8080),
    env = Map(
      "APICURIO_STORAGE_KIND" -> "kafkasql",
      "APICURIO_KAFKASQL_BOOTSTRAP_SERVERS" -> "kafka:9092"
    )
  )

  private var spark: SparkSession = _
  private var registryUrl: String = _

  private val testSchema =
    """{"type":"record","name":"TestEvent","namespace":"com.example.test","fields":[{"name":"event_key","type":"string"},{"name":"user_id","type":"string"},{"name":"event_type","type":"string"}]}"""

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Network 설정 (start 전에 완료해야 함)
    kafka.container.withNetwork(network)
    kafka.container.withNetworkAliases("kafka")
    apicurio.container.withNetwork(network)
    apicurio.container.dependsOn(kafka.container)

    kafka.start()
    apicurio.start()

    registryUrl = s"http://${apicurio.container.getHost}:${apicurio.container.getMappedPort(8080)}"

    waitForApicurio()
    registerTestSchema()

    spark = SparkSession.builder()
      .appName("AvroApicurioIntegrationTest")
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    apicurio.stop()
    kafka.stop()
    network.close()
    super.afterAll()
  }

  private def waitForApicurio(): Unit = {
    var ready = false
    var attempts = 0
    while (!ready && attempts < 30) {
      try {
        val conn = new URL(s"$registryUrl/health").openConnection().asInstanceOf[HttpURLConnection]
        if (conn.getResponseCode == 200) ready = true
        conn.disconnect()
      } catch { case _: Exception => }
      if (!ready) {
        Thread.sleep(2000)
        attempts += 1
      }
    }
    require(ready, "Apicurio Registry did not become ready in time")
  }

  private def registerTestSchema(): Unit = {
    val url = new URL(s"$registryUrl/apis/registry/v3/groups/default/artifacts")
    val conn = url.openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod("POST")
    conn.setDoOutput(true)
    conn.setRequestProperty("Content-Type", "application/json")
    conn.setRequestProperty("X-Registry-ArtifactID", "test-avro-schema")
    conn.setRequestProperty("X-Registry-ArtifactType", "AVRO")
    conn.getOutputStream.write(testSchema.getBytes)
    conn.getOutputStream.flush()

    val code = conn.getResponseCode
    require(code == 201 || code == 409,
      s"Failed to register test schema: HTTP $code - ${Source.fromInputStream(conn.getErrorStream).mkString}")
    conn.disconnect()
  }

  "AvroSerializationStrategy" should "fetch schema from Apicurio Registry" in {
    val strategy = new AvroSerializationStrategy(registryUrl)
    strategy.initialize("test-avro-schema")
    strategy.cleanup()
  }

  it should "serialize DataFrame to Avro using registry schema" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val strategy = new AvroSerializationStrategy(registryUrl)
    strategy.initialize("test-avro-schema")

    val df = Seq(
      ("key1", "user1", "click"),
      ("key2", "user2", "view")
    ).toDF("event_key", "user_id", "event_type")

    val result = strategy.prepareForKafka(df, df.schema, "test-avro-schema")

    result.columns should contain("value")
    result.count() shouldEqual 2

    strategy.cleanup()
  }

  it should "write Avro messages to Kafka and read them back" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val strategy = new AvroSerializationStrategy(registryUrl)
    strategy.initialize("test-avro-schema")

    val df = Seq(
      ("key1", "user1", "click"),
      ("key2", "user2", "view"),
      ("key3", "user3", "purchase")
    ).toDF("event_key", "user_id", "event_type")

    val serialized = strategy.prepareForKafka(df, df.schema, "test-avro-schema")

    serialized
      .withColumn("key", lit("test-key"))
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", kafka.bootstrapServers)
      .option("topic", "test-avro-topic")
      .save()

    val consumed = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", kafka.bootstrapServers)
      .option("subscribe", "test-avro-topic")
      .option("startingOffsets", "earliest")
      .load()

    consumed.count() shouldEqual 3

    strategy.cleanup()
  }
}
