package com.kainos.courses.spark.outputModes

import java.sql.Timestamp

import com.kainos.courses.spark.helper.{InMemoryKeyedStore, InMemoryStoreWriter}
import org.apache.log4j.Logger
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{FlatSpec, Matchers}

class Update extends FlatSpec with Matchers {

  val log = Logger.getLogger(getClass.getName())

  val sparkSession: SparkSession = SparkSession
    .builder
    .master("local[1]")
    .appName("First steps")
    .getOrCreate()

  import sparkSession.implicits._
  implicit val ctx = sparkSession.sqlContext

  "the count on watermark column" should "be correctly computed in update mode" in {
    val testKey = "update-output-mode-with-watermark-aggregation"
    val inputStream = new MemoryStream[(Timestamp, Int)](1, sparkSession.sqlContext)
    val now = 5000L
    val aggregatedStream = inputStream.toDS().toDF("created", "number")
      .withWatermark("created", "1 second")
      .groupBy("created")
      .count()

    val query = aggregatedStream.writeStream.outputMode("update")
      .foreach(new InMemoryStoreWriter[Row](testKey,
        (processedRow) => s"${processedRow.getAs[Long]("created")} -> ${processedRow.getAs[Long]("count")}")).start()

    new Thread(new Runnable() {
      override def run(): Unit = {
        inputStream.addData((new Timestamp(now+5000), 1), (new Timestamp(now+5000), 2), (new Timestamp(now+5000), 3),
          (new Timestamp(now+5000), 4))
        while (!query.isActive) {}
        Thread.sleep(10000)
        inputStream.addData((new Timestamp(4000L), 5))
        inputStream.addData((new Timestamp(now), 6), (new Timestamp(11000), 7))
      }
    }).start()

    query.awaitTermination(45000)

    val readValues = InMemoryKeyedStore.getValues(testKey)
    readValues should have size 2
    readValues should contain allOf("1970-01-01 01:00:10.0 -> 4", "1970-01-01 01:00:11.0 -> 1")
  }

  "the count on non-watermark column" should "be correctly computed in update mode" in {
    val testKey = "update-output-mode-without-watermark-aggregation"
    val inputStream = new MemoryStream[(Timestamp, Int)](1, sparkSession.sqlContext)
    val now = 5000L
    val aggregatedStream = inputStream.toDS().toDF("created", "number")
      .withWatermark("created", "1 second")
      .groupBy("number")
      .count()

    val query = aggregatedStream.writeStream.outputMode("update")
      .foreach(new InMemoryStoreWriter[Row](testKey,
        (processedRow) => s"${processedRow.getAs[Long]("number")} -> ${processedRow.getAs[Long]("count")}")).start()

    new Thread(new Runnable() {
      override def run(): Unit = {
        inputStream.addData((new Timestamp(now+5000), 1), (new Timestamp(now+5000), 2), (new Timestamp(now+5000), 3),
          (new Timestamp(now+5000), 3))
        while (!query.isActive) {}
        Thread.sleep(10000)
        inputStream.addData((new Timestamp(4000L), 6))
        inputStream.addData((new Timestamp(now), 6), (new Timestamp(11000), 7))
      }
    }).start()

    query.awaitTermination(45000)

    val readValues = InMemoryKeyedStore.getValues(testKey)
    readValues should have size 5
    readValues should contain allOf("1 -> 1", "3 -> 2", "2 -> 1", "6 -> 2", "7 -> 1")
  }
}
