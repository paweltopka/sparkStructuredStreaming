package com.kainos.courses.spark.SparkSite2_4_0

import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object ReadFromFile {
  def main(args: Array[String]): Unit = {

    val log = Logger.getLogger(getClass.getName())

    val schema = StructType(
      Array(
        StructField("id", IntegerType),
        StructField("code", StringType),
        StructField("local_code", IntegerType),
        StructField("name", StringType),
        StructField("continent", StringType),
        StructField("iso_country", StringType),
        StructField("Wiki_link", StringType),
        StructField("kewwords", StringType)

      ))

    //create local SparkSession
    val spark = SparkSession.builder
      .master("local[1]")
      .appName("Read From file")
      .getOrCreate()

    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val lines = spark.readStream
      .option("header",true)
      .schema(schema)
      .format("csv")
     // .option("")
      .load("src/main/resources/basePath")



    // Split the lines into words, groupBy words and count the words
    val wordCounts = lines
      .groupBy("id")
      .count()

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()


  }
}