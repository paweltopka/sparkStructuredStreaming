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

    val random = Random

    val sparkSession = SparkSession
      .builder
      .master("local[2]")
      .appName("First steps")
      .getOrCreate()

    import sparkSession.implicits._

    val df = sparkSession.readStream
        .format("rate")
        .option("rowsPerSecond",10)
        .load
        .as[(Timestamp, Long)]
        .map(x => DeviceData(listOfDeviceName(random.nextInt(listOfDeviceName.size)),
          listOfDeviceType(random.nextInt(listOfDeviceType.size)),
          random.nextDouble(),
          x._1))

    val ds = df.as[DeviceData]

    // Select the devices which have signal more than 10
    df.select("device").where("signal > 0.5")      // using untyped APIs

    ds.filter(_.signal > 0.5).map(_.device)         // using typed APIs

    // Running count of the number of updates for each device type
    df.groupBy("deviceType").count()                          // using untyped API

    // Running average signal for each device type
    import org.apache.spark.sql.expressions.scalalang.typed
    ds.groupByKey(_.deviceType).agg(typed.avg(_.signal))    // using typed API


    df.createOrReplaceTempView("updates")
    sparkSession.sql("select count(*) from updates")  // returns another streaming DF



    // Start running the query that prints the running counts to the console
    val query = df
      .writeStream
      .outputMode("update")
      .format("console")
      .start()



    query.awaitTermination()

  }

}
