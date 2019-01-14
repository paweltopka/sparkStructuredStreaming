package com.kainos.courses.spark.helper

object InMemoryKeyedStore {
  var mutMap : scala.collection.mutable.Map[String,scala.collection.mutable.ListBuffer[String]] =
    scala.collection.mutable.Map.empty[String,scala.collection.mutable.ListBuffer[String]]

  def putValues(key:String,value:String): Unit ={
    val tmpList = mutMap.getOrElse(key,scala.collection.mutable.ListBuffer.empty[String]) += value
    mutMap.put(key,tmpList)
    println(tmpList)
  }

  def getValues(key :String): List[String] = {
    mutMap.getOrElse(key,Nil).toList
  }
}
