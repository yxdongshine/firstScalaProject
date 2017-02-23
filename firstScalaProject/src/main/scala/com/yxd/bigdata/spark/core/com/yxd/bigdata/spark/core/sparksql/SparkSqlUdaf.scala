package com.yxd.bigdata.spark.core.com.yxd.bigdata.spark.core.sparksql

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{StructType, DataType}

/**
 * Created by 20160905 on 2017/2/23.
 *
 * 实现统计按照平台23点的浏览总数和平均数
 */
object SparkSqlUdaf extends  UserDefinedAggregateFunction{
  override def inputSchema: StructType = ???

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = ???

  override def bufferSchema: StructType = ???

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = ???

  override def initialize(buffer: MutableAggregationBuffer): Unit = ???

  override def deterministic: Boolean = ???

  override def evaluate(buffer: Row): Any = ???

  override def dataType: DataType = ???
}
