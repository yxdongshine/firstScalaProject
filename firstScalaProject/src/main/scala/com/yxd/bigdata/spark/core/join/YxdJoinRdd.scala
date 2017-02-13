package com.yxd.bigdata.spark.core.join

import org.apache.spark.{SparkContext, SparkConf}


/**
 * Created by 20160905 on 2017/2/10.
 */
object YxdJoinRdd {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("YxdJoinRdd")

    val sc = SparkContext.getOrCreate(sparkConf)
    //具体代码
    val rdd1 = sc.parallelize(
      Array(
        (1, "张三1"),
        (1, "张三2"),
        (2, "李四"),
        (3, "王五"),
        (4, "Tom"),
        (5, "Gerry"),
        (6, "莉莉")
      ), 1
    )

    val rdd2 = sc.parallelize(Array(
      (1, "上海"),
      (2, "北京1"),
      (2, "北京2"),
      (3, "南京"),
      (4, "纽约"),
      (6, "深圳"),
      (7, "香港")
    ), 1)


    //下面是join连接代码展示
    //自然连接  按照key排序
    val innerjoin = rdd1.join(rdd2)
      .sortBy(tuple => tuple._1)
      .map(
        tuple => {
          val key = tuple._1
          val vThis = tuple._2._1
          val vOther = tuple._2._2
          (key, vThis, vOther)
        }
      )
      .collect()

    //左外连接
    val leftJoin = rdd1.leftOuterJoin(rdd2)
      .map {
    case (key, (vThis, vOther)) =>
    {
      (key, vThis, vOther.getOrElse("NULL"))
    }
    }
    .collect()


    //全连接
    val fullJoin = rdd1.fullOuterJoin(rdd2)
    .map{
      case (key,(vThis,vOther)) =>{
        (key,vThis.getOrElse("null"),vOther.getOrElse("null"))
      }
    }
    .collect()



    //广播数据
    val broadRdd = sc.broadcast(rdd2.collect())

    //手写内连接
    val innerjoinDefine = rdd1.filter(
    tuple => broadRdd.value.map(_._1).contains(tuple._1)
    )
    .map{
    case (id ,name) =>{
      broadRdd.value.filter(tuple => tuple._1 == id).map{
        case (oid,address) =>{
          (id,name,address)
        }
      }
      }
    }
    .collect()


    //自定义左外连接
    val leftjoinDefine = rdd1
      .map{
      case (id ,name) =>{
        val list =  broadRdd.value.filter(tuple => tuple._1 == id)
          if ( list.nonEmpty){
            list.map{
              case (oid,address) =>{
                (id,name,address)
              }
            }

          }else{
              (id,name,"null"):: Nil
          }

      }
    }
      .collect()


    Thread.sleep(1000000)
    sc.stop()





  }

}
