package com.kainos.courses.spark.SparkSite2_4_0

import java.sql.Timestamp

import com.kainos.courses.spark.SparkSite2_4_0.KafkaSourceConsoleTarget.getClass
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import scala.util.Random

object StatefullOperation {

  case class DeviceData(device: String, deviceType: String, signal: Double, time: Timestamp)

  def main(args: Array[String]): Unit = {
    val log = Logger.getLogger(getClass.getName())

    val listOfDeviceType = Seq("mobile", "notebook", "pc", "navigation", "camera")
    val listOfDeviceName = Seq("cheese", "bread", "ham", "pork", "apple", "device")

    val sparkSession = SparkSession
      .builder
      .master("local[4]")
      .appName("First steps")
      .getOrCreate()

    import sparkSession.implicits._


    //read data from sink
    // streaming DataFrame with IOT device data with schema { device: string, deviceType: string, signal: double, time: string }
    val df = sparkSession
      .readStream
      .format("kafka")
      .option("rowsPerSecond", 100)
      .load
      .as[(Timestamp, Long)]
      .map(x => DeviceData(listOfDeviceName(new Random().nextInt(listOfDeviceName.size)),
        listOfDeviceType(new Random().nextInt(listOfDeviceType.size)),
        new Random().nextInt(100),
        x._1))

  }

}
