package com.kainos.courses.spark.SparkSite2_4_0

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

import scala.util.Random

object JoinStreamStream {

  case class click(clickTime: Timestamp, clickAdId:Int)
  case class impression(impressionTime:Timestamp, impressionAdId:Int)
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession
      .builder
      .master("local[2]")
      .appName("Join Stream with Stream")
      .getOrCreate()

    import sparkSession.implicits._

    val rand = Random

    val impressions = sparkSession.readStream
      .format("rate")
      .option("rowsPerSecond",1)
      .load
      .as[(Timestamp, Long)]
      .map(x => impression(x._1, rand.nextInt(20)))

    val clicks = sparkSession.readStream
      .format("rate")
      .option("rowsPerSecond",1)
      .load
      .as[(Timestamp, Long)]
      .map(x => click(x._1, (x._2%10).toInt))

    // Apply watermarks on event-time columns
    val impressionsWithWatermark = impressions.withWatermark("impressionTime", "2 minutes")
    val clicksWithWatermark = clicks.withWatermark("clickTime", "3 minutes")

    // Join with event-time constraints
   val joined = impressionsWithWatermark.join(
      clicksWithWatermark,
      expr("""
        clickAdId = impressionAdId AND
         clickTime >= impressionTime AND
         clickTime <= impressionTime + interval 1 hour
           """)
    )

    joined.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", false)
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()
      .awaitTermination()

 }
}
