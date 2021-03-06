package com.kainos.courses.spark.WatermarkKafka

import java.sql.Timestamp

import com.kainos.courses.spark.help.HelpfulMethods.toIntOption
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object OutputModesWindowKafkaConsole {
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
      .map(x => DeviceData(new Timestamp(x._1*60000),
        new Timestamp(System.currentTimeMillis()),
        x._2))
      .as[DeviceData]
      .withWatermark("eventTime","10 minutes")
      .groupBy(window($"eventTime",
        "10 minutes","5 minutes"), $"value")
      .count()




    val query = deviceDataStream
      .withColumn("mode",lit("append"))
      .writeStream
      .outputMode("append")
      .format("console")
      .option("truncate",false)
      .trigger(Trigger.ProcessingTime("60 seconds"))
      .start()

//     deviceDataStream
//      .withColumn("mode",lit("update"))
//      .writeStream
//      .outputMode("update")
//      .format("console")
//      .option("truncate",false)
//      .trigger(Trigger.ProcessingTime("60 seconds"))
//      .start()

    /*deviceDataStream
      .withColumn("mode",lit("complete"))
      .writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate",false)
      .start()*/

    query.awaitTermination()

  }
}
