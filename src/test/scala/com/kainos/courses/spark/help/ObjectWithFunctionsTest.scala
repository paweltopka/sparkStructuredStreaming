package com.kainos.courses.spark.help

import java.sql.Timestamp

import com.kainos.courses.spark.help.ObjectWithFunctions.{InputData, OutputData, someOperation}
import com.kainos.courses.spark.helper.{InMemoryKeyedStore, InMemoryStoreWriter}
import org.apache.log4j.Logger
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.scalatest.FunSuite
import org.scalatest.{FlatSpec, Matchers}

class ObjectWithFunctionsTest extends FlatSpec with Matchers {

  "some test" should "be correctly computed" in {
    val log = Logger.getLogger(getClass.getName())
    val testKey = "test of function"


    val sparkSession: SparkSession = SparkSession
      .builder
      .master("local[1]")
      .appName("Some test")
      .getOrCreate()
    import  sparkSession.implicits._
    implicit val ctx = sparkSession.sqlContext

    val inputStream: MemoryStream[InputData] = new MemoryStream[InputData](2, sparkSession.sqlContext)

    val query = someOperation(sparkSession,inputStream.toDS()).toDF().writeStream.outputMode("append")
      .foreach(new InMemoryStoreWriter[Row](testKey,
        (processedRow) => processedRow.toString()
      ))
      .start()

    new Thread(new Runnable() {
      override def run(): Unit = {
        inputStream.addData(InputData("ala","komputer",0.1,new Timestamp(3)))
        inputStream.addData(InputData("ala1","komputer",0.5,new Timestamp(2)))
        inputStream.addData(InputData("ala2","komputer",0.7,new Timestamp(1)))
      }
    }).start()



    query.awaitTermination(15000)
    val readValues = InMemoryKeyedStore.getValues(testKey)
    readValues should have size 2
    readValues should contain allOf("[ala1,0.5]","[ala2,0.7]")

  }

}
