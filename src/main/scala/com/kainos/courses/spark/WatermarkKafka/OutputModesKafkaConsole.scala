package com.kainos.courses.spark.WatermarkKafka

import java.sql.Timestamp
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import com.kainos.courses.spark.help.HelpfulMethods._
import org.apache.spark.sql.functions._
import scala.util.Random

object OutputModesKafkaConsole {
  case class DeviceData( eventTime: Timestamp,processingTime : Timestamp, value: String)

  def main(args: Array[String]): Unit = {
    val log = Logger.getLogger(getClass.getName())

    val sparkSession = SparkSession
      .builder
      .master("local[2]")
      .appName("Kafka Append Mode")
      .getOrCreate()

    import sparkSession.implicits._

    val valueStreamKafkaSource = sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "127.0.0.1:9092")
      .option("subscribe", "test")
      .load
      .selectExpr( "CAST(value AS STRING)")
      .as[ String]

      val deviceDataStream = valueStreamKafkaSource
        .map(x => x.split(" "))
        .filter(_.length==2)
        .map(x => (x.head,x.last))
        .map(x => (toIntOption(x._1).getOrElse(-1),x._2))
        .filter(_._1>=0)
        .map(x => DeviceData(new Timestamp(x._1*1000),
          new Timestamp(System.currentTimeMillis()),
          x._2))
        .as[DeviceData]
        .withWatermark("eventTime","2 minutes")
        .groupBy($"eventTime", $"value")
        .count()




    val query = deviceDataStream
      .withColumn("mode",lit("append"))
      .writeStream
      .outputMode("append")
      .format("console")
      .option("truncate",false)
      .start()

    deviceDataStream
      .withColumn("mode",lit("update"))
      .writeStream
      .outputMode("update")
      .format("console")
      .option("truncate",false)
      .start()

    deviceDataStream
      .withColumn("mode",lit("complete"))
      .writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate",false)
      .start()

    query.awaitTermination()

  }
}
