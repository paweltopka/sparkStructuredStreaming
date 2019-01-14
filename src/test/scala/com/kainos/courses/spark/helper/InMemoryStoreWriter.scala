package com.kainos.courses.spark.helper

import org.apache.spark.sql.ForeachWriter

class InMemoryStoreWriter[T](test:String, a:T => String) extends ForeachWriter[T]{

  override def open(partitionId: Long, epochId: Long): Boolean = {true}

  override def process(value: T): Unit = {
    InMemoryKeyedStore.putValues(test,a.apply(value))
  }

  override def close(errorOrNull: Throwable): Unit = {}
}
