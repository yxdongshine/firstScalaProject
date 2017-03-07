package user_portrait.mock

import java.util.UUID

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import user_portrait.mode.{ActOrderActive, OrdOrderGoods, OrdOrderDesc, OrdOrder}
import user_portrait.util.MockUtils

import scala.collection.mutable.ArrayBuffer

/**
  * 产生订单相关模拟数据
  * Created by ibf on 03/05.
  */
object OrdOrderDataMock {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("ord_order_data_mock")
      .set("spark.eventLog.enabled","true")
      .set("spark.eventLog.dir","hdfs://hadoop1:9000/spark-history")
    val sc = SparkContext.getOrCreate(conf)

    // ==================================
    val startOrderId = 10000000L
    val random = MockUtils.random
    val dt = "2017-03-04"
    val dtLong = MockUtils.parseString2Long(dt)
    val dtEndLong = dtLong + MockUtils.dayOfMillis
    val ordOrderDataSavePath = s"datas/bdb_ord_order/${dt}"
    val ordOrderDescDataSavePath = s"datas/bdb_ord_order_desc/${dt}"
    val ordOrderGoodsSavePath = s"datas/bdb_ord_order_good/${dt}"
    val actOrderActiveSavePath = s"datas/bdb_act_order_active/${dt}"
    val fs = FileSystem.get(sc.hadoopConfiguration)
    fs.delete(new Path(ordOrderDataSavePath), true)
    fs.delete(new Path(ordOrderDescDataSavePath), true)
    fs.delete(new Path(ordOrderGoodsSavePath), true)
    fs.delete(new Path(actOrderActiveSavePath), true)

    val datas = ArrayBuffer[OrdOrder]()
    // 最少产生10001数据 最多1000000数据
    for (i <- 0 to random.nextInt(10000, 1000000)) {
      val orderId: Long = startOrderId + i
      val orderNo: String = UUID.randomUUID().toString
      // 产生订单的时间， 一百天以内的任何一天
      val orderDate: String = MockUtils.getRandomSpecifyDaysAfterDate(dtEndLong, -100, MockUtils.datePattern)
      val createTime: String = MockUtils.getRandomSpecifyDaysAfterDate(MockUtils.parseString2Long(orderDate))
      val userId: Long = random.nextLong(MockUtils.boundUserId)
      val userName: String = s"user_${userId}"
      val orderMoney: Double = MockUtils.formatDoubleOfScale(random.nextDouble(10, 1000))
      val orderType: String = MockUtils.orderTypes(random.nextInt(MockUtils.orderTypesSize))
      val orderStatus: String = "1" // 订单产生
      val payType: String = "" // 订单支付类型
      val payStatus: String = "1" // 订单未支付
      val orderSource: String = MockUtils.orderSources(random.nextInt(MockUtils.orderSourcesSize))
      val lastUpdateTime: String = MockUtils.getRandomSpecifyDaysAfterDate(dtLong)

      // 数据添加到队列中
      datas += new OrdOrder(
        orderId, orderNo, orderDate, createTime, userId,
        userName, orderMoney, orderType, orderStatus,
        payType, payStatus, orderSource, lastUpdateTime
      )
    }

    // 95%的订单开始支付行为
    val datas2 = MockUtils.span[OrdOrder](datas, order => random.nextDouble(1) <= 0.95)
    // 没有支付行为的订单
    val noPayedOrders = datas2._2
    val payedOrders = datas2._1.map(order => {
      // 对支付的订单进行数据更改操作
      val payType = MockUtils.payTypes(random.nextInt(MockUtils.payTypesSize))
      val payStatus = "2" // 正在支付中
      order.payStatus = payStatus
      order.payType = payType
      order.updateLastUpdateTime()

      order
    })

    // 90%的订单支付成功，10%支付失败
    val datas3 = MockUtils.span[OrdOrder](payedOrders, order => random.nextDouble(1) <= 0.9)
    val paySuccessOrders = datas3._1.map(order => {
      val payStatus = "3" // 支付成功
      val orderStatus = "2" // 支付成功
      order.payStatus = payStatus
      order.orderStatus = orderStatus
      order.updateLastUpdateTime()

      order
    })
    val payFailureOrders = datas3._2.map(order => {
      val payStatus = "4" // 支付失败
      val orderStatus = "5" // 支付失败
      order.payStatus = payStatus
      order.orderStatus = orderStatus
      order.updateLastUpdateTime()

      order
    })

    // 15%的订单退款 5%的订单拒收
    val datas4 = MockUtils.span[OrdOrder](paySuccessOrders, order => random.nextDouble(1) <= 0.2)
    val datas5 = MockUtils.span[OrdOrder](datas4._1, order => random.nextDouble(1) <= 0.75)
    val returnOrders = datas5._1.map(order => {
      val orderStatus = "3" // 退款
      order.orderStatus = orderStatus
      order.updateLastUpdateTime()

      order
    })
    val rejectOrders = datas5._2.map(order => {
      val orderStatus = "4" // 拒收
      order.orderStatus = orderStatus
      order.updateLastUpdateTime()

      order
    })

    // 合并所有订单: 没有支付的订单 ++ 支付失败的订单 ++ 退款订单 ++ 拒收订单 ++ 最终成功订单
    val totalOrders = noPayedOrders ++ payFailureOrders ++ returnOrders ++ rejectOrders ++ datas4._2

    // 构建RDD
    val rdd = sc.parallelize(totalOrders)
    rdd.cache()

    // rdd数据输出
    rdd.map(order => order.formatted(MockUtils.splitSymbol))
      .repartition(1)
      .saveAsTextFile(ordOrderDataSavePath)

    // 订单详情产生
    rdd.map(order => {
      // 开始构建订单详情对象
      val orderID: Long = order.orderId
      val orderNO: String = order.orderNo
      val consignee: String = s"consignee_${order.userId}_${random.nextInt(3)}"
      val areaID: Long = random.nextLong(MockUtils.boundAreaId)
      val areaName: String = s"area_${areaID}"
      val address: String = s"address_${areaID}"
      val mobilephone: String = MockUtils.randomMobilephone._1
      val telphone: String = MockUtils.randomTelphone
      // 60%的几率使用了购物券
      val usedCoupon = random.nextDouble(1) <= 0.6
      val (couponId: Long, couponMoney: Double) = if (usedCoupon) {
        // 使用代金券
        val t1 = order.orderId
        val t2 = order.orderMoney * random.nextDouble(0.1, 0.5)
        (t1, MockUtils.formatDoubleOfScale(t2))
      } else {
        // -1 表示没有使用给代金券
        (-1L, 0.0)
      }

      // 60%的几率需要运费
      val needCarriageMoney = random.nextDouble(1) <= 0.6
      val carriageMoney: Double = if (needCarriageMoney) {
        // 运费是订单金额的1%-5%之间，并且最高不能超过50元，最低不能低于3元
        val t = order.orderMoney * random.nextDouble(0.01, 0.05)
        Math.min(50, Math.max(3, MockUtils.formatDoubleOfScale(t)))
      } else {
        0.0
      }

      val createTime: String = order.createTime
      val lastUpdateTime: String = order.lastUpdateTime
      // orderAddr的取值范围是:[0,1,2,3]
      val orderAddr: Long = random.nextLong(4)

      // 封装成为对象并返回
      new OrdOrderDesc(
        orderID, orderNO, consignee, areaID, areaName,
        address, mobilephone, telphone, couponId, couponMoney,
        carriageMoney, createTime, lastUpdateTime, orderAddr
      ).formatted(MockUtils.splitSymbol)
    })
      .repartition(1)
      .saveAsTextFile(ordOrderDescDataSavePath)

    // 开始产生订单商品数据
    rdd.flatMap(order => {
      val orderId = order.orderId
      val createTime = order.orderDate
      val lastUpdateTime = order.lastUpdateTime
      // 一个订单最少1个商品，最多9个商品
      val goodIds = MockUtils.mockGoodIds(random.nextInt(1, 10))

      goodIds.map(goodId => {
        // 产生一个从0到MockUtils.boundCategoryId-1的long型数据作为类型id
        val categoryId = goodId % MockUtils.boundCategoryId
        val categoryName = s"category_${categoryId}"
        val sizeId = random.nextLong(3) // 只有0 1 2这三种大小
        val colourId = random.nextLong(4) // 只有 0 1 2 3这四种颜色
        val goodsPrice = MockUtils.formatDoubleOfScale(random.nextDouble(1, 10000))
        val goodsAmount = random.nextLong(1, 1000)

        // 构建对象并返回
        OrdOrderGoods(
          orderId, goodId, categoryId, categoryName, sizeId, colourId,
          goodsPrice, goodsAmount, createTime, lastUpdateTime
        ).formatted(MockUtils.splitSymbol)
      })
    })
      .repartition(1)
      .saveAsTextFile(ordOrderGoodsSavePath)

    // 订单活动信息表数据产生
    rdd
      .flatMap(order => {
        val orderId = order.orderId
        val addTime = order.createTime
        // 一个订单可能包含多个促销类型，最多三个，最少0个
        (0 until random.nextInt(3)).map(indsex => {
          // index的值最多为：0,1,2， 最少为空
          val activeId = random.nextLong(MockUtils.boundActiveId) // 活动id
          ActOrderActive(orderId, activeId, addTime).formatted(MockUtils.splitSymbol)
        })
      })
      .repartition(1)
      .saveAsTextFile(actOrderActiveSavePath)

    // ==================================
    // sc.stop在jvm退出的时候会自动调用，而且SparkContext在当前jvm中只可能存在一个，所以如果手动调用的stop函数的情况，可能导致在其它线程中使用的SparkContext报错
    sc.stop()
  }
}
