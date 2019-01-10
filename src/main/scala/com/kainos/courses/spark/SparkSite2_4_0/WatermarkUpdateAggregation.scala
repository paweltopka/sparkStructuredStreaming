package com.kainos.courses.spark.SparkSite2_4_0

import java.sql.Timestamp

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.streaming.Trigger

object WatermarkUpdateAggregation {

  case class TimestampAndWord(eventTime: Timestamp, word:String )

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
    val startTime = 0

    val dsWithWatermark = memoryStream
      .toDS()
      .flatMap(x=> x.word.split(" ").map(y => TimestampAndWord(x.eventTime,y)))
      .withWatermark("eventTime","5 seconds")
      .groupBy(  $"eventTime",$"word")
      .count


    val query = dsWithWatermark.writeStream
      .outputMode("update")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .format("console")
      .option("truncate",false)
      .start()

    new Thread(new Runnable() {
      override def run(): Unit = {

        memoryStream.addData(new TimestampAndWord(new Timestamp(startTime+2000),"cat dog"))
        memoryStream.addData(new TimestampAndWord(new Timestamp(startTime+3000),"dog dog"))
        while (!query.isActive) {}
        Thread.sleep(10000)
        memoryStream.addData(new TimestampAndWord(new Timestamp(startTime+7000),"owl cat"))
        Thread.sleep(10000)

        memoryStream.addData(new TimestampAndWord(new Timestamp(startTime+4000),"dog"))
        memoryStream.addData(new TimestampAndWord(new Timestamp(startTime+13000),"owl"))

        memoryStream.addData(new TimestampAndWord(new Timestamp(startTime+2000),"not included"))
        memoryStream.addData(new TimestampAndWord(new Timestamp(startTime+8000),"included"))
        Thread.sleep(10000)
        memoryStream.addData(new TimestampAndWord(new Timestamp(startTime+2000),"not included"))
        memoryStream.addData(new TimestampAndWord(new Timestamp(startTime+8000),"not included"))
        Thread.sleep(5000)


      }
    }).start()

    query.awaitTermination()
  }

}
