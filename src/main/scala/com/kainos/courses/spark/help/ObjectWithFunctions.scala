package com.kainos.courses.spark.help

import java.sql.Timestamp

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.util.Random


object ObjectWithFunctions {

  case class InputData(device: String, deviceType: String, signal: Double, time: Timestamp)
  case class OutputData(device: String, signal: Double)

  def someOperation(sparkSession:SparkSession,datasetIn:Dataset[InputData]):Dataset[OutputData] = {
    import sparkSession.implicits._
    datasetIn.filter(_.signal>=0.5).map(x => OutputData(x.device,x.signal))

  }

}
