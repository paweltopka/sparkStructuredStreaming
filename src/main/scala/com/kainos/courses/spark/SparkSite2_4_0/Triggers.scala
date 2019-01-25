package com.kainos.courses.spark.SparkSite2_4_0

import java.sql.Timestamp

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.Trigger

object Triggers {

  case class sampleData(timestamp: Timestamp, value: Long)

  def main(args: Array[String]): Unit = {
    val log = Logger.getLogger(getClass.getName())

   val sparkSession = SparkSession
      .builder
      .master("local[2]")
      .appName("First steps")
      .getOrCreate()

    import sparkSession.implicits._

    //read data from sink
    // streaming DataFrame with IOT device data with schema { device: string, deviceType: string, signal: double, time: string }
    val ds = sparkSession
      .readStream
      .format("rate")
      .option("rowsPerSecond",1000)
      .load
      .as[(Timestamp, Long)]
      .map(x => new sampleData(new Timestamp(x._1.getTime/1000*1000),x._2))
      //.withWatermark("timestamp","0 seconds")
      //.groupBy("timestamp")
      //.count

    // Default trigger (runs micro-batch as soon as it can)
     val default =  ds.writeStream
      .format("console")
      .queryName("default")
      .start()
      .awaitTermination()

    // ProcessingTime trigger with two-seconds micro-batch interval
    val processingTime = ds.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .queryName("processing Time")
      .start()
      .awaitTermination()

    // One-time trigger
    val oneTime =  ds.writeStream
      .format("console")
      .trigger(Trigger.Once())
      .queryName("One-Time")
      .start()
      .awaitTermination()
    // Continuous trigger with one-second checkpointing interval
    //in console mode data are display each checkpointing interval - 1 second
    // if the sink will be kafka then latency will lower
    val continous = ds
      .coalesce(1)
      .writeStream
      .format("console")
      .trigger(Trigger.Continuous("1 second"))
      .start()
      .awaitTermination()



  }

}
