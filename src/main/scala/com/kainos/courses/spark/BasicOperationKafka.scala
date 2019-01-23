package com.kainos.courses.spark

import java.sql.Timestamp

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object BasicOperationKafka extends Serializable {

  case class DeviceData(device: String, deviceType: String, signal: Double, time: Timestamp)

  def main(args: Array[String]): Unit = {
    val log = Logger.getLogger(getClass.getName())

    val listOfDeviceType = Seq("mobile","notebook","pc","navigation","camera")
    val listOfDeviceName = Seq("cheese", "bread","ham","pork","apple","device")

    val sparkSession = SparkSession
      .builder
      .master("local[2]")
      .appName("First steps")
      .getOrCreate()

    import sparkSession.implicits._




    //read data from sink
    // streaming DataFrame with IOT device data with schema { device: string, deviceType: string, signal: double, time: string }
    val df = sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "127.0.0.1:9092")
      .option("subscribe", "test")
      .load
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]

    val df1 = df.groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = df1
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()



    query.awaitTermination()

  }

}
