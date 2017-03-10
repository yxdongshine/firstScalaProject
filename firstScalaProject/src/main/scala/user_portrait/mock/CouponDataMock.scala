package user_portrait.mock

import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.{SparkContext, SparkConf}
import user_portrait.mode.BdbCoupon
import user_portrait.util.MockUtils

import scala.collection.mutable.ArrayBuffer

/**
 * Created by 20160905 on 2017/3/10.
 */
object CouponDataMock {

  def main(args: Array[String]) {
    //设置spark
    val sparkConf = new SparkConf()
    .setAppName("CouponDataMock")
    .setMaster("local[*]")
      .set("spark.eventLog.enabled","true")
      .set("spark.eventLog.dir","hdfs://hadoop1:9000/spark-history")

    val  sparkContext = SparkContext.getOrCreate(sparkConf)
    val dt = "2017-03-04"
    val dtLong = MockUtils.parseString2Long(dt)
    val random = MockUtils.random
    val couponSavePath = s"datas/coupon/${dt}"
    val fs = FileSystem.get(sparkContext.hadoopConfiguration)
    fs.delete(new Path(couponSavePath ),true)

    //模拟产生数据
    var  couponDatas = ArrayBuffer[BdbCoupon]()
    //产生一万张代金券
    for (i <- 0L until MockUtils.boundUserId) {
      val userId = Math.min(random.nextLong(MockUtils.boundUserId),MockUtils.boundUserId)
      val couponId = 100000 +i
      val amt = random.nextDouble(1000.0)
      val status = random.nextInt(MockUtils.couponStatus.size)
      val endTime =MockUtils.getRandomSpecifyDaysAfterDate(dtLong, -100, MockUtils.datePattern)

      //构建数据
      couponDatas += new BdbCoupon(
      userId,
      couponId ,
      amt ,
      status ,
      endTime
      )
    }

    //数组转换成RDD
    val  rdd = sparkContext.parallelize(couponDatas)
    //转换保存
    rdd.map(
      coupon => coupon.formatted(MockUtils.splitSymbol)
    )
    .repartition(1)
    .saveAsTextFile(couponSavePath)

   // sparkContext.stop()
  }
}
