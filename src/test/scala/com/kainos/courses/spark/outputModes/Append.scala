package com.kainos.courses.spark.outputModes

import java.sql.Timestamp

import com.kainos.courses.spark.helper.{InMemoryKeyedStore, InMemoryStoreWriter, NoopForeachWriter}
import org.apache.log4j.Logger
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.scalatest.{FlatSpec, Matchers}

class Append extends FlatSpec with Matchers  {
  val log = Logger.getLogger(getClass.getName())

  val sparkSession: SparkSession = SparkSession
    .builder
    .master("local[1]")
    .appName("First steps")
    .getOrCreate()

  import sparkSession.implicits._
  implicit val ctx = sparkSession.sqlContext

  "the count on watermark column" should "be correctly computed in append mode" in {
    val testKey = "append-output-mode-with-watermark-aggregation"
    val inputStream = new MemoryStream[(Timestamp, Int)](2, sparkSession.sqlContext)
    val now = 5000L
    val aggregatedStream = inputStream.toDS().toDF("created", "number")
      .withWatermark("created", "1 seconds")
      .groupBy("created")
      .count()

    val query = aggregatedStream.writeStream.outputMode("append")
      .foreach(new InMemoryStoreWriter[Row](testKey,
        (processedRow) => s"${processedRow.getAs[Long]("created")} -> ${processedRow.getAs[Long]("count")}"))
      //.format("console")
      .start()

    new Thread(new Runnable() {
      override def run(): Unit = {
        // send the first batch - max time = 10 seconds, so the watermark will be 9 seconds
        inputStream.addData((new Timestamp(now+5000L), 1), (new Timestamp(now+5000L), 2), (new Timestamp(now+4000L), 3),
          (new Timestamp(now+5000L), 4))
        while (!query.isActive) {}
        Thread.sleep(10000)
        inputStream.addData((new Timestamp(4000L), 5), (new Timestamp(now+4000L), 6))
        inputStream.addData((new Timestamp(1100), 7))
        Thread.sleep(10000)
        inputStream.addData((new Timestamp(11000), 8))
      }
    }).start()

    query.awaitTermination(45000)

    val readValues = InMemoryKeyedStore.getValues(testKey)
    // As you can see, only the result for now+5000 was emitted. It's because the append output
    // mode emits the results once a new watermark is defined and the accumulated results are below
    // the new threshold
    readValues should have size 2
    readValues should contain allOf("1970-01-01 01:00:09.0 -> 1", "1970-01-01 01:00:10.0 -> 3")
  }


  "the count on non-watermark column" should "fail in append mode" in {
    val inputStream = new MemoryStream[(Timestamp, Int)](1, sparkSession.sqlContext)
    val aggregatedStream = inputStream.toDS().toDF("created", "number")
      .withWatermark("created", "1 second")
      .groupBy("number")
      .count()

    val exception = intercept[AnalysisException]{
      aggregatedStream.writeStream.outputMode("append")
        .foreach(new NoopForeachWriter[Row]()).start()
    }

    exception.message should include("Append output mode not supported when there are streaming aggregations on " +
      "streaming DataFrames/DataSets without watermark")
  }
}
