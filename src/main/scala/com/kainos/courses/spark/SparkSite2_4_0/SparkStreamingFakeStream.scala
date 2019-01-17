package com.kainos.courses.spark.SparkSite2_4_0

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.collection.mutable

object SparkStreamingFakeStream {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("spark-streaming-testing-example")

    val ssc = new StreamingContext(sparkConf, Seconds(1))



      val rddQueue = new mutable.Queue[RDD[Char]]()

      ssc.queueStream(rddQueue)
        .map(_.toUpper)
        .window(windowDuration = Seconds(3), slideDuration = Seconds(2))
        .print()

      ssc.start()

      for (c <- 'a' to 'z') {
        rddQueue += ssc.sparkContext.parallelize(List(c))
      }

      ssc.awaitTermination()
    }

}
