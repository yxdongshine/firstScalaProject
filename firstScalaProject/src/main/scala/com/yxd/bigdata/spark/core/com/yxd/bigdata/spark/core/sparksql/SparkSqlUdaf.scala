package com.yxd.bigdata.spark.core.com.yxd.bigdata.spark.core.sparksql

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
 * Created by 20160905 on 2017/2/23.
 * 多对一 聚合函数
 * 实现统计按照平台23点的浏览总数和平均数
 */
object SparkSqlUdaf extends  UserDefinedAggregateFunction{
  /**
   * 输入格式
   * @return
   */
  override def inputSchema: StructType = {

    StructType(Array(StructField("inputData",IntegerType)))
  }

  /**
   * 每读一条 就迭代更新函数
   * @param buffer 中间缓存的数据结构
   * @param input 输入一行
   */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //一行一行的相加 加入到buffer
    val totalData =buffer.getInt(0)+ input.getInt(0)
    val countData =buffer.getInt(1)+1
    //设置更新值
    buffer.update(0,totalData)
    buffer.update(1,countData)
  }

  /**
   * 中间缓存格式
   * @return
   */
  override def bufferSchema: StructType = {
    StructType(Array(
      StructField("totalData",IntegerType) ,
      StructField("countData",IntegerType)
    ))
  }

  /**
   *多个区合并数据
   * @param buffer1 合并到数据结构
   * @param buffer2 被合并的数据结构
   */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //一行一行的相加 加入到buffer
    val totalData =buffer1.getInt(0)+ buffer2.getInt(0)
    val countData =buffer1.getInt(1)+buffer2.getInt(1)//唯一区别 ：每个分区中数据和条数key-value键值对保存
    //设置更新值
    buffer1.update(0,totalData)
    buffer1.update(1,countData)
  }

  /**
   * 初始化函数 中间变量初始化 按照添加的顺序 一次下标
   * @param buffer
   */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {

    buffer.update(0,0)//总数
    buffer.update(1,0)//条数 row 每次添加一
  }

  /**
   * 是否允许近视计算 如果true ,每次返回数据一致；false 允许近似计算，返回值每次不一样
   * @return
   */
  override def deterministic: Boolean = {
    false
  }

  /**
   * 计算最后的结果，根据被给的数据结构
   * @param buffer
   * @return
   */
  override def evaluate(buffer: Row): Any = {
    ( buffer.getInt(0) / buffer.getInt(1)).toDouble
  }

  /**
   * 返回的数据结构
   * @return
   */
  override def dataType: DataType = {
    DoubleType //直接指定返回数据类型
  }
}
