package com.example.replayer

import com.dimafeng.testcontainers.{GenericContainer, KafkaContainer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import org.testcontainers.containers.Network
import org.testcontainers.utility.DockerImageName
import com.example.replayer.serialization.ProtobufSerializationStrategy
import java.net.{HttpURLConnection, URL}
import scala.io.Source

/**
 * Protobuf + Apicurio Schema Registry 3.x integration test
 *
 * Requires Docker to be running.
 * Starts Kafka and Apicurio on a shared network, registers a test proto schema,
 * then verifies the full serialization and Kafka publish flow.
 */
class ProtobufApicurioIntegrationTest
  extends AnyFlatSpec
  with Matchers
  with BeforeAndAfterAll {

  private val network = Network.newNetwork()

  private val kafka = KafkaContainer(
    DockerImageName.parse("confluentinc/cp-kafka:7.6.0")
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

  private val testProtoSchema =
    """syntax = "proto3";
      |
      |package com.example.test;
      |
      |option java_package = "com.example.test";
      |option java_outer_classname = "TestEventProto";
      |
      |message TestEvent {
      |  string event_key = 1;
      |  string user_id = 2;
      |  string event_type = 3;
      |}
      |""".stripMargin

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
      .appName("ProtobufApicurioIntegrationTest")
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
    // Use v2 API which is still supported in Apicurio 3.x
    val url = new URL(s"$registryUrl/apis/registry/v2/groups/default/artifacts")
    val conn = url.openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod("POST")
    conn.setDoOutput(true)
    conn.setRequestProperty("Content-Type", "application/x-protobuf")
    conn.setRequestProperty("X-Registry-ArtifactId", "test-proto-schema")
    conn.setRequestProperty("X-Registry-ArtifactType", "PROTOBUF")

    // Send proto schema as body
    conn.getOutputStream.write(testProtoSchema.getBytes)
    conn.getOutputStream.flush()

    val code = conn.getResponseCode
    if (code == 200 || code == 409) {
      println(s"[Test] Proto schema registered successfully: HTTP $code")
    } else {
      val errorMsg = if (code >= 400) Source.fromInputStream(conn.getErrorStream).mkString else ""
      println(s"[Test] Failed to register proto schema: HTTP $code - $errorMsg")
    }
    require(code == 200 || code == 409,
      s"Failed to register test proto schema: HTTP $code - ${if (code >= 400) Source.fromInputStream(conn.getErrorStream).mkString else ""}")
    conn.disconnect()
  }

  "ProtobufSerializationStrategy" should "fetch schema from Apicurio Registry" in {
    val strategy = new ProtobufSerializationStrategy(registryUrl)
    strategy.initialize("test-proto-schema")
    strategy.cleanup()
  }

  it should "serialize DataFrame to Protobuf using registry schema" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val strategy = new ProtobufSerializationStrategy(registryUrl)
    strategy.initialize("test-proto-schema")

    val df = Seq(
      ("key1", "user1", "click"),
      ("key2", "user2", "view")
    ).toDF("event_key", "user_id", "event_type")

    val result = strategy.prepareForKafka(df, df.schema, "test-proto-schema")

    result.columns should contain("value")
    result.count() shouldEqual 2

    strategy.cleanup()
  }

  it should "write Protobuf messages to Kafka and read them back" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val strategy = new ProtobufSerializationStrategy(registryUrl)
    strategy.initialize("test-proto-schema")

    val df = Seq(
      ("key1", "user1", "click"),
      ("key2", "user2", "view"),
      ("key3", "user3", "purchase")
    ).toDF("event_key", "user_id", "event_type")

    val serialized = strategy.prepareForKafka(df, df.schema, "test-proto-schema")

    serialized
      .withColumn("key", lit("test-key"))
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", kafka.bootstrapServers)
      .option("topic", "test-proto-topic")
      .save()

    val consumed = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", kafka.bootstrapServers)
      .option("subscribe", "test-proto-topic")
      .option("startingOffsets", "earliest")
      .load()

    consumed.count() shouldEqual 3

    strategy.cleanup()
  }
}
