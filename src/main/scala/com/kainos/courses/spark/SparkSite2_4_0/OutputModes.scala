package com.kainos.courses.spark.SparkSite2_4_0

import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession


object OutputModes extends  Serializable {

  def main(args: Array[String]): Unit = {

    val log = Logger.getLogger(getClass.getName())

    //create local SparkSession
    val spark = SparkSession.builder
      .master("local[1]")
      .appName("Hello World")
      .getOrCreate()

    import spark.implicits._

    //send data by console: nc -lk 9999

    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    // Split the lines into words, groupBy words and count the words
    val wordCounts = lines.as[String]
      .flatMap(_.split(" "))
      .groupBy("value")
      .count()

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete") //change to append, update and check what happen
      .format("console")
      .start()

    query.awaitTermination()


  }
}
