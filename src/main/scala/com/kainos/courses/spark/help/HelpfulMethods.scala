package com.kainos.courses.spark.help

object HelpfulMethods {
  def toIntOption(s:String):Option[Int]={
    try {
      Some(s.toInt)
    } catch {
      case e: NumberFormatException => None
    }
  }

}
