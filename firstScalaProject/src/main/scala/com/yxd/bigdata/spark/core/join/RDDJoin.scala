package com.yxd.bigdata.spark.core.join

import org.apache.spark.{SparkConf, SparkContext}

/**
  * RDD数据Join相关API讲解
  * Created by ibf on 02/09.
  */
object RDDJoin {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("RDD-Join")
    val sc = SparkContext.getOrCreate(conf)

    // ==================具体代码======================
    // 模拟数据产生
    val rdd1 = sc.parallelize(Array(
      (1, "张三1"),
      (1, "张三2"),
      (2, "李四"),
      (3, "王五"),
      (4, "Tom"),
      (5, "Gerry"),
      (6, "莉莉")
    ), 1)

    val rdd2 = sc.parallelize(Array(
      (1, "上海"),
      (2, "北京1"),
      (2, "北京2"),
      (3, "南京"),
      (4, "纽约"),
      (6, "深圳"),
      (7, "香港")
    ), 1)

    // 调用RDD API实现内连接
    val joinResultRDD = rdd1.join(rdd2).map {
      case (id, (name, address)) => {
        (id, name, address)
      }
    }
    println("----------------")
    joinResultRDD.foreachPartition(iter => {
      iter.foreach(println)
    })
    // 调用RDD API实现左外连接
    val leftJoinResultRDd = rdd1.leftOuterJoin(rdd2).map {
      case (id, (name, addressOption)) => {
        (id, name, addressOption.getOrElse("NULL"))
      }
    }
    println("----------------")
    leftJoinResultRDd.foreachPartition(iter => {
      iter.foreach(println)
    })
    // 左外连接稍微变化一下：需要左表出现，右表不出现的数据(not in)
    println("----------------")
    rdd1.leftOuterJoin(rdd2).filter(_._2._2.isEmpty).map {
      case (id, (name, _)) => (id, name)
    }.foreachPartition(iter => {
      iter.foreach(println)
    })

    // 右外连接
    println("----------------")
    rdd1
      .rightOuterJoin(rdd2)
      .map {
        case (id, (nameOption, address)) => {
          (id, nameOption.getOrElse("NULL"), address)
        }
      }
      .foreachPartition(iter => iter.foreach(println))

    // 全外连接
    println("----------------")
    rdd1
      .fullOuterJoin(rdd2)
      .map {
        case (id, (nameOption, addressOption)) => {
          (id, nameOption.getOrElse("NULL"), addressOption.getOrElse("NULL"))
        }
      }
      .foreachPartition(iter => iter.foreach(println))

    // 假设rdd2的数据比较少，将rdd2的数据广播出去
    val leastRDDCollection = rdd2.collect()
    val broadcastRDDCollection = sc.broadcast(leastRDDCollection)

    println("++++++++++++++++++")
    // 类似Inner Join的操作，Inner Join的功能：将两个表都出现的数据合并
    println("-------------------")
    rdd1
      // 过滤rdd1中的数据，只要在rdd1中出现的数据，没有出现的数据过滤掉
      .filter(tuple => broadcastRDDCollection.value.map(_._1).contains(tuple._1))
      // 数据合并，由于一条rdd1的数据可能在rdd2中存在多条对应数据，所以使用flatMap
      .flatMap {
      case (id, name) => {
        broadcastRDDCollection.value.filter(_._1 == id).map {
          case (_, address) => {
            (id, name, address)
          }
        }
      }
    }
      .foreachPartition(iter => iter.foreach(println))

    // 左外连接
    println("---------------------")
    rdd1
      .flatMap {
        case (id, name) => {
          // 从右表所属的广播变量中获取对应id的集合列表
          val list = broadcastRDDCollection.value.filter(_._1 == id)
          // 对应id的集合可能为空，也可能数据有多个
          if (list.nonEmpty) {
            // 存在多个
            list.map(tuple => (id, name, tuple._2))
          } else {
            // id在右表中不存在，填默认值
            (id, name, "NULL") :: Nil
          }
        }
      }
      .foreachPartition(iter => iter.foreach(println))

    // 右外连接
    /**
      * rdd2中所有数据出现，由于rdd2中的数据在driver中可以存储，可以认为rdd1和rdd2通过right join之后的数据也可以在driver中保存下
      **/
    println("---------------------")
    // 将rdd1中符合条件的数据过滤出来保存到driver中
    val stage1 = rdd1
      .filter(tuple => broadcastRDDCollection.value.map(_._1).contains(tuple._1))
      .collect()
    // 将driver中两个集合进行right join
    val stage2 = leastRDDCollection.flatMap {
      case (id, address) => {
        val list = stage1.filter(_._1 == id)
        if (list.nonEmpty) {
          list.map(tuple => (id, tuple._2, address))
        } else {
          Iterator.single((id, "NULL", address))
        }
      }
    }
    stage2.foreach(println)

    // TODO: 全外连接，不写代码，因为代码比较复杂

    //====================================
    // 左半连接：只出现左表数据(要求数据必须在右表中也出现过)，如果左表的数据在右表中出现多次，最终结果只出现一次
    println("+++++++++++++++++")
    println("-----------------------")
    rdd1
      .join(rdd2)
      .map {
        case (id, (name, _)) => (id, name)
      }
      .distinct()
      .foreachPartition(iter => iter.foreach(println))
    println("------------------------")
    rdd1
      .filter(tuple => broadcastRDDCollection.value.map(_._1).contains(tuple._1))
      .foreachPartition(iter => iter.foreach(println))

    // 休眠为了看4040页面
        Thread.sleep(1000000)
  }
}
