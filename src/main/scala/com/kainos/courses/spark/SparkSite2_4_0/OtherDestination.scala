package com.kainos.courses.spark.SparkSite2_4_0

import java.sql.Timestamp

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Dataset, ForeachWriter, SparkSession}

import scala.util.Random

object OtherDestination {

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
      .option("rowsPerSecond",1)
      .load
      .as[(Timestamp, Long)]
      .map(x => DeviceData(listOfDeviceName(random.nextInt(listOfDeviceName.size)),
        listOfDeviceType(random.nextInt(listOfDeviceType.size)),
        random.nextDouble(),
        x._1))


    // Start running the query that prints the running counts to the console
    val query = df.toDF()
      .writeStream
      .foreachBatch{ (batchDF: DataFrame, batchId: Long) =>{
        batchDF.foreach(
          x => println("I'm here and this is my row: " + x.toString())

        )
      }
      }
        .start()


    val foreachWriter = new ForeachWriter[DeviceData] {
      override def open(partitionId: Long, epochId: Long): Boolean = {
        println("Now writer is init")
        true
      }

      override def process(value: DeviceData): Unit = {
        println("foreach writer wants to show data: " + value.toString)
      }

      override def close(errorOrNull: Throwable): Unit = {
        println("connection closed")
      }
    }

    // Start running the query that prints the running counts to the console
  /* df.coalesce(1)
      .writeStream
      .foreach(foreachWriter)
      .start()*/

    query.awaitTermination()

  }


}
