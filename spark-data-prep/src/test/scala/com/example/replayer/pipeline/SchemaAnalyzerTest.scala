package com.example.replayer.pipeline

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SchemaAnalyzerTest extends AnyFlatSpec with Matchers {

  val spark = SparkSession.builder()
    .appName("SchemaAnalyzerTest")
    .master("local[1]")
    .getOrCreate()

  import spark.implicits._

  "detectKeyColumn" should "prioritize lot_id" in {
    val df = Seq((1, "key", "val")).toDF("lot_id", "event_key", "other")
    SchemaAnalyzer.detectKeyColumn(df) shouldEqual "lot_id"
  }

  it should "prioritize event_key if lot_id is missing" in {
    val df = Seq((1, "val")).toDF("event_key", "other")
    SchemaAnalyzer.detectKeyColumn(df) shouldEqual "event_key"
  }

  it should "prioritize eqp_id if others are missing" in {
    val df = Seq((1, "val")).toDF("eqp_id", "other")
    SchemaAnalyzer.detectKeyColumn(df) shouldEqual "eqp_id"
  }

  it should "fallback to first column" in {
    val df = Seq(("val1", "val2")).toDF("col1", "col2")
    SchemaAnalyzer.detectKeyColumn(df) shouldEqual "col1"
  }

  "detectTimestampColumn" should "prioritize ts" in {
    val df = Seq((1L, 2L)).toDF("ts", "event_time")
    SchemaAnalyzer.detectTimestampColumn(df) shouldEqual "ts"
  }

  it should "prioritize event_time" in {
    val df = Seq((1L, 2L)).toDF("other", "event_time")
    SchemaAnalyzer.detectTimestampColumn(df) shouldEqual "event_time"
  }

  it should "find first timestamp type column" in {
    val schema = StructType(Seq(
      StructField("name", StringType),
      StructField("created_at", TimestampType)
    ))
    val rdd = spark.sparkContext.emptyRDD[org.apache.spark.sql.Row]
    val df = spark.createDataFrame(rdd, schema)
    
    SchemaAnalyzer.detectTimestampColumn(df) shouldEqual "created_at"
  }
  
  it should "find first long type column if no explicit timestamp" in {
     val df = Seq((1, 1000L)).toDF("id", "timestamp_ms")
     SchemaAnalyzer.detectTimestampColumn(df) shouldEqual "timestamp_ms"
  }
}
