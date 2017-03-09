package user_portrait.mock

import java.util.UUID

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import user_portrait.mode.PrdGoods
import user_portrait.util.MockUtils

import scala.collection.mutable.ArrayBuffer

/**
  * 商品数据模拟产生
  * Created by ibf on 02/09.
  */
object PrdGoodsDataMock {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("prd-goods-data")
      .setMaster("local[*]")
      .set("spark.executor.heartbeatInterval", "360s")
      .set("spark.eventLog.enabled","true")
      .set("spark.eventLog.dir","hdfs://hadoop1:9000/spark-history")
    val sc = SparkContext.getOrCreate(conf)

    // ==================================
    val random = MockUtils.random
    val prdGoodsSavePath = s"datas/prd_goods"
    val fs = FileSystem.get(sc.hadoopConfiguration)
    fs.delete(new Path(prdGoodsSavePath), true)

    // 包含startId，不包含endId
    def generateDatas(startId: Long, endId: Long): ArrayBuffer[PrdGoods] = {
      val prdGoodsDatas = new ArrayBuffer[PrdGoods]()
      for (i <- startId until endId) {
        val goodsId: Long = i //商品编号
        val goodsName: String = s"good_${goodsId}" //商品名称
        val goodsNo: String = UUID.randomUUID().toString //商品货号
        val goodsSn: String = goodsNo //商品条码
        val shopId: Long = random.nextLong(1000) //店铺ID
        val shopName: String = s"shop_${shopId}" //店铺名称
        val currPrice: Double = MockUtils.formatDoubleOfScale(random.nextDouble(1000)) //售卖价格
        val discount: Double = MockUtils.formatDoubleOfScale(random.nextDouble(1), 3) //折扣比率
        val marketPrice: Double = MockUtils.formatDoubleOfScale(currPrice * (1 + discount)) //市场价格
        val costPrice: Double = MockUtils.formatDoubleOfScale(currPrice * random.nextDouble(0.3, 1.05)) //成本价格
        val costType: String = s"cost_${random.nextInt(5)}" //成本类型
        val warehouse: String = s"warehouse_${random.nextInt(10)}" //所在仓库
        val stockCnt: Long = random.nextLong(10000) //进货数量
        val stockAmt: Double = MockUtils.formatDoubleOfScale(stockCnt * costPrice) //进货货值
        val categoryID: Long = goodsId % MockUtils.boundCategoryId //品类ID
        val categoryName: String = s"category_${categoryID}" //品类名称

        for (j <- 0 to random.nextInt(2)) {
          // skuId只能是0 1 2三种类型的值
          val skuId: Long = j.toLong //SKU编号, 同一件商品，不同尺码就是不同skuid的值
          val skuName: String = s"sku_${skuId}" //SKU名称
          val sizeId: Long = skuId //尺码编号
          val sizeName: String = s"size_${sizeId}" //尺码名称
          (0 until random.nextInt(1, 3)).foreach(colour => {
            val colourId = colour // 颜色ID
            // 构建对象并保存
            prdGoodsDatas += new PrdGoods(
              skuId, skuName, goodsId, goodsName,
              goodsNo, goodsSn, sizeId, sizeName, colourId,
              shopId, shopName, currPrice, marketPrice, discount,
              costPrice, costType, warehouse, stockCnt, stockAmt,
              categoryID, categoryName)
          })
        }
      }

      // 返回结果
      prdGoodsDatas
    }

    // TODO: bug 这里运行可能报错？由于在代码编写过程中可能存在一个商品对应多个具体子类的关系
    val step = MockUtils.boundGoodId / 100
    var j = 0
    for (i <- 0L.until(MockUtils.boundGoodId, step)) {
      sc.parallelize(generateDatas(i, i + step), 5)
        .map(prdGood => prdGood.formatted(MockUtils.splitSymbol))
        .saveAsTextFile(s"${prdGoodsSavePath}/${j}")
      j += 1
    }


  }
}
