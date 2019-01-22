package com.kainos.courses.spark.SparkSite2_4_0

import java.sql.{Struct, Timestamp}

import org.apache.spark.sql.expressions.{WindowSpec,Window}
import org.apache.spark.sql.{RelationalGroupedDataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StructType

import scala.util.Random

object JoinStreamStreamWindow {

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

    //Define new window partition to operate on row frame
    val partitionWindow = Window.partitionBy($"clickTime")
      .orderBy($"clickAdId".desc)
      .rowsBetween(Window.currentRow, Window.currentRow+1000)

    val rankTest = last($"clickTime").over(partitionWindow)

    val partitionWindow2 = Window.partitionBy($"impressionTime")
      .orderBy($"impressionAdId".desc)
      .rowsBetween(Window.currentRow, Window.currentRow+1000)
    val rankTest2 = last($"impressionTime").over(partitionWindow2)


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
    val impressionsWithWatermark = impressions
      .withWatermark("impressionTime", "2 minutes")
      .select($"*", rankTest2 as "rank")

    val clicksWithWatermark = clicks
      .withWatermark("clickTime", "3 minutes")
      .select($"*", rankTest as "rank")


    clicksWithWatermark.printSchema()
    // Join with event-time constraints
  val joined = impressionsWithWatermark.join(
      clicksWithWatermark
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
