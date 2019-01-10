package com.kainos.courses.spark

import java.sql.Timestamp

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

import scala.concurrent.duration._

object FirstSteps extends Serializable {

  case class SampleData(timestamp: Timestamp, number:Long)

  def main(args: Array[String]): Unit = {

    val log = Logger.getLogger(getClass.getName())

    //val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val spark: SparkSession = SparkSession
      .builder
      .master("local[2]")
      .appName("First steps")
      .getOrCreate()

    import spark.implicits._

    //read data from sink
    val simpleDataStream = spark
      .readStream
      .format("rate") //file, socket, kafka
      .option("rowsPerSecond",10)  // (key,value)
      .load

    //transform data
    val makeSomeTransformation = simpleDataStream
      .as[(Timestamp, Long)]
      .map(x => SampleData(x._1,x._2))
      .groupBy("timestamp")
        .count()


    //foreach
    //write data to target
    makeSomeTransformation
      .writeStream
      .format("console")
      .trigger(Trigger.ProcessingTime(1.seconds))
      .outputMode(OutputMode.Complete())
      .start
      .awaitTermination()


  }
}


