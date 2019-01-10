package com.kainos.courses.spark.SparkSite2_4_0

import java.sql.Timestamp

import org.apache.log4j.Logger
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.util.Random

object Watermarking {

  case class TimestampAndWord(eventTime: Timestamp, receivedTime:Timestamp, word:String )

  def main(args: Array[String]): Unit = {
    val log = Logger.getLogger(getClass.getName())

    val listOfWords = Seq("mobile","notebook","pc","navigation","camera")

    val sparkSession = SparkSession
      .builder
      .master("local[2]")
      .appName("First steps")
      .getOrCreate()

    import sparkSession.implicits._

    //read data from sink
    // streaming DataFrame with IOT device data with schema { device: string, deviceType: string, signal: double, time: string }
    val memoryStream: MemoryStream[TimestampAndWord] = new MemoryStream[TimestampAndWord](1, sparkSession.sqlContext)
    val startTime = 5000
    val random = new Random()

    val dsWithWatermark = memoryStream
      .toDS()
      .withWatermark("eventTime","2 seconds")
      .groupBy($"eventTime")
      .count


    val query = dsWithWatermark.writeStream
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .format("console")
      .option("truncate",false)
      .start()

    new Thread(new Runnable() {
      override def run(): Unit = {
        // send the first batch - max time = 10 seconds, so the watermark will be 9 seconds
        memoryStream.addData(new TimestampAndWord(new Timestamp(startTime),new Timestamp(startTime),"1"))
        // send the first batch - max time = 10 seconds, so the watermark will be 9 seconds
        memoryStream.addData(new TimestampAndWord(new Timestamp(startTime+2000),new Timestamp(startTime),"1"))
        while (!query.isActive) {}
        Thread.sleep(10000)
        memoryStream.addData(new TimestampAndWord(new Timestamp(startTime-3000),new Timestamp(startTime),"2"))
        memoryStream.addData(new TimestampAndWord(new Timestamp(startTime+1000),new Timestamp(startTime),"3"))
        memoryStream.addData(new TimestampAndWord(new Timestamp(startTime-5000),new Timestamp(startTime),"4"))

        memoryStream.addData(new TimestampAndWord(new Timestamp(startTime+2000),new Timestamp(startTime),"5"))
        Thread.sleep(10000)
        memoryStream.addData(new TimestampAndWord(new Timestamp(startTime),new Timestamp(startTime),"6"))
        memoryStream.addData(new TimestampAndWord(new Timestamp(startTime+4000),new Timestamp(startTime),"7"))


      }
    }).start()

    query.awaitTermination()
  }
}
