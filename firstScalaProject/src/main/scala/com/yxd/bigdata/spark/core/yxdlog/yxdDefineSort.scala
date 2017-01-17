package com.yxd.bigdata.spark.core.yxdlog

/**
 * Created by Administrator on 2017/1/17 0017.
 */
object yxdDefineSort {

  object TupleSort extends scala.math.Ordering[(Int,Int)]{
    override def compare(x: (Int, Int), y: (Int, Int)): Int = {
      x._2 .compare( y._2)
    }
  }
}
