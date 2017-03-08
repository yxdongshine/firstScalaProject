package user_portrait.mock

import java.util.UUID

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import user_portrait.mode.OrdCart
import user_portrait.util.MockUtils

import scala.collection.mutable.ArrayBuffer

/**
  * 购物车数据模拟产生
  * Created by ibf on 02/04.
  */
object OrdCartDataMock {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("OrdCartDataMock")
      .setMaster("local[*]")
      .set("spark.eventLog.enabled","true")
      .set("spark.eventLog.dir","hdfs://hadoop1:9000/spark-history")
    val sc = SparkContext.getOrCreate(conf)

    // ==================================
    val random = MockUtils.random
    val startOrderCartId = 1000000L
    val dt = "2017-03-04"
    val dtLong = MockUtils.parseString2Long(dt)
    val dtEndLog = dtLong + MockUtils.dayOfMillis
    val ordCartSavePath = s"datas/ord_cart/${dt}"
    val fs = FileSystem.get(sc.hadoopConfiguration)
    fs.delete(new Path(ordCartSavePath), true)

    val datas = ArrayBuffer[OrdCart]()
    for (i <- 0 until random.nextInt(400000, 800000)) {
      val id: Long = startOrderCartId + i
      val sessionId: String = UUID.randomUUID().toString
      val userId: Long = random.nextLong(MockUtils.boundUserId)
      val goodId: Long = random.nextLong(MockUtils.boundGoodId)
      // 最少1个，最多999个
      val number: Long = random.nextLong(1, 1000)
      val addTime: String = MockUtils.getRandomSpecifyDaysAfterDate(dtEndLog, -30)
      val cancelTime: String = ""
      val submitTime: String = ""

      // 构建返回对象
      datas += OrdCart(
        id, sessionId, userId,
        goodId, number, addTime,
        cancelTime, submitTime)
    }

    // 65%的购物车商品被提交
    val datas2 = MockUtils.span[OrdCart](datas, cart => random.nextDouble(1) <= 0.65)
    val submitedCartDatas = datas2._1.map(cart => {
      // 该部分商品被提交，修改参数
      cart.updateSubmitTime()
      // 返回结果
      cart
    })
    // 75%的没有被提交的商品过期了
    val datas3 = MockUtils.span[OrdCart](datas2._2, cart => random.nextDouble(1) <= 0.75)
    val canceledCartDatas = datas3._1.map(cart => {
      // 修改参数
      cart.updateCancelTime()
      // 返回结果
      cart
    })

    // 合并修改好的最终购物车商品
    // 提交 ++ 取消 ++ 没有进行操作的
    val totalOrdCartDatas = submitedCartDatas ++ canceledCartDatas ++ datas3._2

    // 构建rdd
    val rdd = sc.parallelize(totalOrdCartDatas)

    // 结果保存到文件
    rdd
      .map(cart => cart.formatted(MockUtils.splitSymbol))
      .repartition(1)
      .saveAsTextFile(ordCartSavePath)
  }
}
