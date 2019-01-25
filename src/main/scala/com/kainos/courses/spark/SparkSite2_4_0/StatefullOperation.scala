package com.kainos.courses.spark.SparkSite2_4_0

import java.sql.Timestamp

import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, SparkSession}

import org.apache.spark.sql.streaming._

import scala.util.Random

object StatefullOperation {

  case class DeviceData(device: String, deviceType: String, signal: Double, time: Timestamp)
  case class State( mapDeviceSignal: Map[String,Double])

  case class NewRowStructure(device: String, mapDeviceSignal: Map[String,Double])

  def main(args: Array[String]): Unit = {
    val log = Logger.getLogger(getClass.getName())

    val listOfDeviceType = Seq("mobile", "notebook", "pc", "navigation", "camera")
    val listOfDeviceName = Seq("cheese", "bread", "ham", "pork", "apple", "device")

    val sparkSession = SparkSession
      .builder
      .master("local[4]")
      .appName("First steps")
      .getOrCreate()

    import sparkSession.implicits._


    //read data from sink
    // streaming DataFrame with IOT device data with schema { device: string, deviceType: string, signal: double, time: string }
    val df: Dataset[DeviceData] = sparkSession
      .readStream
      .format("rate")
      .option("rowsPerSecond", 100)
      .load
      .as[(Timestamp, Long)]
      .map(x => DeviceData(listOfDeviceName(new Random().nextInt(listOfDeviceName.size)),
        listOfDeviceType(new Random().nextInt(listOfDeviceType.size)),
        new Random().nextInt(100000),
        x._1))

    val dfWithSate: Dataset[NewRowStructure] = df.groupByKey(_.deviceType)
      .mapGroupsWithState(GroupStateTimeout.ProcessingTimeTimeout())(updateAcrossEvents)


    // Start running the query that prints the running counts to the console
    val query = dfWithSate
      .writeStream
      .outputMode("update")
      .format("console")
      .option("truncate",false)
      .start()

    query.awaitTermination()


  }

  def updateAcrossEvents(deviceType: String,
                         inputs: Iterator[DeviceData],
                         oldState: GroupState[State]): NewRowStructure = {

    val state: State = if (oldState.exists) oldState.get
    else   State( Map())


    if(!inputs.isEmpty) {
      val newState = updateState(inputs, state)
      oldState.update(newState)
      oldState.setTimeoutDuration(60000L)
      NewRowStructure(deviceType, newState.mapDeviceSignal)
    }
    else
      NewRowStructure(deviceType, state.mapDeviceSignal)

  }

  def updateState(inputs:Iterator[DeviceData],state:State):State={
    val inputRow = inputs.next()
    if(inputs.hasNext)
      updateState(inputs,calculateNewState(inputRow,state))
    else {
      calculateNewState(inputRow,state)
    }
  }

  def calculateNewState(inputRow:DeviceData,state:State):State={
    if(inputRow.signal > state.mapDeviceSignal.getOrElse(inputRow.device,0.0)) {
      State( state.mapDeviceSignal + (inputRow.device -> inputRow.signal))
    }
    else {
      state
    }
  }
}
