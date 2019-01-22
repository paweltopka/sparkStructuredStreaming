package com.kainos.courses.spark.SparkSite2_4_0

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession

object JoinStreamStatic {

  case class streamingSchema(timestamp: Timestamp, id:Int)

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession
      .builder
      .master("local[2]")
      .appName("Join Stream with Static")
      .getOrCreate()

    val staticDf = sparkSession.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("src/main/resources/basicExample.csv")

    import sparkSession.implicits._

staticDf.printSchema()

    val streamingDf = sparkSession
      .readStream
      .format("rate")
      .option("rowsPerSecond",10)
      .load
      .as[(Timestamp, Long)]
      .map(x => streamingSchema(x._1, (x._2%10).toInt))

    streamingDf.join(staticDf, "id")
      .writeStream
      .outputMode("append")
      .format("console")
      .start()
      .awaitTermination()



  }
}
