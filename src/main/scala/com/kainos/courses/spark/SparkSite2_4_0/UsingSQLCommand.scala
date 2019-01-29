package com.kainos.courses.spark.SparkSite2_4_0

import java.sql.Timestamp

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import scala.util.Random

object UsingSQLCommand extends Serializable {

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
       //extend generated data to case class DeviceData
        .map(x => DeviceData(listOfDeviceName(random.nextInt(listOfDeviceName.size)),
          listOfDeviceType(random.nextInt(listOfDeviceType.size)),
          random.nextDouble(),
          x._1))

    df.createOrReplaceTempView("updates")
    val newDf = sparkSession.sql("select count(*) from updates")  // returns another streaming DF

    // Start running the query that prints the running counts to the console
    val query = newDf
      .writeStream
      .outputMode("update")
      .format("console")
      .start()

    query.awaitTermination()

  }

}
