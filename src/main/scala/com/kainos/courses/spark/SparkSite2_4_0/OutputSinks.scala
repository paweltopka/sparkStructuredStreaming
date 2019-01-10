package com.kainos.courses.spark.SparkSite2_4_0

import java.sql.Timestamp

import org.apache.log4j.Logger
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object OutputSinks extends Serializable {

  def main(args: Array[String]): Unit = {
    val log = Logger.getLogger(getClass.getName())

    val sparkSession = SparkSession
      .builder
      .master("local[2]")
      .appName("First steps")
      .getOrCreate()

    import sparkSession.implicits._

    val df: Dataset[(Timestamp, Long)] = sparkSession
      .readStream
      .format("rate")
      .option("rowsPerSecond",100)
      .load
      .as[(Timestamp, Long)]

    //write to File Sink
    val dfWriteStream: DataStreamWriter[(Timestamp, Long)] = df.writeStream
      .format("parquet")        // can be "orc", "json", "csv", etc.
      .option("path", "path")
      .outputMode("append")
      //checkpointing is required because fault-tolerant exactly once
      .option("checkpointLocation", "path2")

    //write to Kafka sink
    val dfWriteStreamToKafka = df.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092") //more bootstrap servers are defined after coma "ip_address:port,ip_address:port,..."
        .option("topic", "test")

    //print stream in console
    val dfWriteStreamToConsole = df.writeStream
        .format("console")
        .outputMode("append")
      //default: true
      .option("truncate",false)
      //default: 20
      .option("numRows",10)

    //memory sink in tests

    dfWriteStream.start().awaitTermination()
    //dfWriteStreamToKafka.start().awaitTermination()
    //dfWriteStreamToConsole.start().awaitTermination()

  }

}
