package com.example.replayer.pipeline

import com.example.replayer.config.Config
import com.example.replayer.serialization.SerializationStrategy
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TransformerTest extends AnyFlatSpec with Matchers {

  val spark = SparkSession.builder()
    .appName("TransformerTest")
    .master("local[1]")
    .getOrCreate()
  
  import spark.implicits._

  // Mock strategy that just converts columns to string representation
  class MockStrategy extends SerializationStrategy {
    override def initialize(schemaName: String): Unit = {}
    override def prepareForKafka(df: DataFrame, schema: StructType, schemaName: String): DataFrame = {
       df.select(col(df.columns(0)).cast("string").as("value"))
    }
  }

  "Transformer" should "transform dataframe correctly with timestamp" in {
    val df = Seq(
      ("k1", 1000L, "data1"),
      ("k2", 2000L, "data2")
    ).toDF("event_key", "event_time", "data")

    val config = Config(
      keyColumn = Some("event_key"),
      timestampColumn = Some("event_time"),
      schemaName = Some("test.schema")
    )

    val result = Transformer.transform(spark, df, config, new MockStrategy(), includeTimestamp = true)

    result.columns should contain allOf ("key", "value", "event_time_ms")
    result.count() shouldEqual 2
    
    val rows = result.collect()
    rows.find(r => r.getString(0) == "k1").get.getLong(2) shouldEqual 1000L
  }

  it should "transform dataframe without timestamp" in {
    val df = Seq(
      ("k1", "data1")
    ).toDF("event_key", "data")

    val config = Config(
      keyColumn = Some("event_key"),
      schemaName = Some("test.schema")
    )

    val result = Transformer.transform(spark, df, config, new MockStrategy(), includeTimestamp = false)

    result.columns should contain allOf ("key", "value")
    result.columns should not contain "event_time_ms"
    result.count() shouldEqual 1
  }
}
