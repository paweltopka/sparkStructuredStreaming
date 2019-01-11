package com.kainos.courses.spark.SparkSite2_4_0

import java.sql.Timestamp

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.util.Random

object BasicOperation extends Serializable {

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
      .format("rate")
      .option("rowsPerSecond",100)
      .load
      .as[(Timestamp, Long)]


      .map(x => DeviceData(listOfDeviceName(new Random().nextInt(listOfDeviceName.size)),
        listOfDeviceType(new Random().nextInt(listOfDeviceType.size)),
        new Random().nextInt(100),
        x._1))
      .toDF()

    val ds: Dataset[DeviceData] = df.as[DeviceData]    // streaming Dataset with IOT device data

    // Select the devices which have signal more than 10
    val df2 = df.select("device") // using untyped APIs
      .where("signal > 10")

    val df3 = df.groupBy("device").avg("signal")

    val ds2 = ds.filter(_.signal > 10)  // using typed APIs
      .map(_.device)

    val ds3 = ds.groupByKey(_.deviceType).count()


    //You can use sql command
    //to do this first create temporary view of DataFrame / Dataset streaming
    df.createTempView("inputStream")

    //sql command create new DataFrame
    val dfFromView: DataFrame = sparkSession.sql("select * from inputStream")

    // Start running the query that prints the running counts to the console
    val query = dfFromView
      .writeStream
      .outputMode("append")
      .format("console")
      .start()



    query.awaitTermination()

  }

}
