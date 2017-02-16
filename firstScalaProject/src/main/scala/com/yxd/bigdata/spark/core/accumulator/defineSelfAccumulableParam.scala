package com.yxd.bigdata.spark.core.accumulator

import akka.actor.FSM.->
import org.apache.spark.AccumulableParam

import scala.collection.mutable

/**
 * Created by Administrator on 2017/2/16 0016.
 */
object defineSelfAccumulableParam  extends AccumulableParam[mutable.Map[String,Int] , String]{


  /**
   * 分区中添加一个string的world来增加计数加1
   * @param r
   * @param t
   * @return
   */
  override def addAccumulator(r: mutable.Map[String, Int], t: String): mutable.Map[String, Int] = {

    r += t -> (r.getOrElse(t , 0) + 1)


  }

  /**
   * 用于两个分区合并时候的数据合并
   * @param r1
   * @param r2
   * @return
   */
  override def addInPlace(r1: mutable.Map[String, Int], r2: mutable.Map[String, Int]): mutable.Map[String, Int] = {

    r2.foldLeft(r1)(
      (r1 , b) =>{
        r1 += b._1 -> (r1.getOrElse(b._1 , 0) + b._2)
      }
    )
    null
  }

  /**
   * 初始值
   * @param initialValue
   * @return
   */
  override def zero(initialValue: mutable.Map[String, Int]): mutable.Map[String, Int] = initialValue

}
