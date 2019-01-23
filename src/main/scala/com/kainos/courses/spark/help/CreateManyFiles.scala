package com.kainos.courses.spark.help

import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.types.{DoubleType,IntegerType, StringType, StructField, StructType}
object CreateManyFiles {

  def main(args: Array[String]): Unit = {

    val log = Logger.getLogger(getClass.getName())

    val schema = StructType(
      Array(
        StructField("AirportId", IntegerType),
        StructField("Name", StringType),
        StructField("City", StringType),
        StructField("Country", StringType),
        StructField("IATA", StringType),
        StructField("ICAO", StringType),
        StructField("Latitude", DoubleType),
        StructField("Longitude", DoubleType),
        StructField("Altitude", DoubleType),
        StructField("Timezone", IntegerType),
        StructField("DST", StringType),
        StructField("TzDataBaseTimeZone", StringType),
        StructField("Type", StringType),
        StructField("Source", StringType)
      ))

    //create local SparkSession
    val spark = SparkSession.builder
      .master("local[1]")
      .appName("Read From file")
      .getOrCreate()

    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val data = spark.read
      .option("header",false)
      .schema(schema)
      .format("csv")
      .load("input/AirportsFullData.dat")
      .repartition(100)
      .selectExpr("*")



    // Start running the query that prints the running counts to the console
    val query = data.write
      .csv("output")




  }
}

