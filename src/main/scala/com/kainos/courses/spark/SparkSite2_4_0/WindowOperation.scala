package com.kainos.courses.spark.SparkSite2_4_0

import java.sql.Timestamp

import com.kainos.courses.spark.SparkSite2_4_0.BasicOperation.DeviceData
import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset, SparkSession}
import org.apache.spark.sql.functions._
import java.sql.Timestamp

import org.apache.spark.sql.streaming.Trigger

import scala.util.Random

object WindowOperation extends Serializable {

  val listOfWords = Seq("mobile","notebook","pc","navigation","camera")

  case class TimestampAndWord(timestamp: Timestamp, word:String )

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession
      .builder
      .master("local[2]")
      .appName("First steps")
      .getOrCreate()

    import sparkSession.implicits._

    // streaming DataFrame of schema { timestamp: Timestamp, word: String }
    val wordsDF = sparkSession
      .readStream
      .format("rate")
      .option("rowsPerSecond",1)
      .load
      .as[(Timestamp, Long)]
      .map(x => TimestampAndWord(x._1, listOfWords(new Random().nextInt(listOfWords.size))))
      .toDF()


    // Group the data by window and word and compute the count of each group
    val windowedCounts = wordsDF
      .withWatermark("timestamp", "0 minutes")
      .groupBy(
        window($"timestamp", "30 seconds","20 seconds"),
        $"word")
      .count()



    // Start running the query that prints the running counts to the console
    val query = windowedCounts.writeStream
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .format("console")
      .option("truncate",false)
      .start()

    query.awaitTermination()
  }
}
