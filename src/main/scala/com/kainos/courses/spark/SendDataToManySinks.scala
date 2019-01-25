package com.kainos.courses.spark

import java.sql.Timestamp

import org.apache.log4j.Logger
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.util.Random

object SendDataToManySinks {

  case class DeviceData(device: String, deviceType: String, signal: Double, time: Timestamp)

  def main(args: Array[String]): Unit = {
    val log = Logger.getLogger(getClass.getName())

    val listOfDeviceType = Seq("mobile","notebook","pc","navigation","camera")
    val listOfDeviceName = Seq("cheese", "bread","ham","pork","apple","device")

    val random = Random

    val sparkSession = SparkSession
      .builder
      .master("local[2]")
      .appName("First steps")
      .getOrCreate()

    import sparkSession.implicits._

    val df = sparkSession.readStream
      .format("rate")
      .option("rowsPerSecond",100)
      .load
      .as[(Timestamp, Long)]
      .map(x => DeviceData(listOfDeviceName(random.nextInt(listOfDeviceName.size)),
        listOfDeviceType(random.nextInt(listOfDeviceType.size)),
        random.nextDouble(),
        x._1))

    //here you can write some transformations



    // Start running the query that prints the running counts to the console
      df.writeStream
      .format("console")
      .start()

    //start running query send data to kafka
    val query2 = df
      .select($"deviceType".as("key"),$"time".as("value"))
      .coalesce(1)
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .trigger(Trigger.Continuous("10 second"))
      .format("kafka")
      .option("kafka.bootstrap.servers", "127.0.0.1:9092")
      .option("topic", "topic2")
      .option("checkpointLocation", "checkpointLocation")
      .start()


    query2.awaitTermination()

  }

}
