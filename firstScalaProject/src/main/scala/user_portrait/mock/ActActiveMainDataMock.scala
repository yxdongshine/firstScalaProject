package user_portrait.mock

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import user_portrait.mode.ActActiveMain
import user_portrait.util.MockUtils

import scala.collection.mutable.ArrayBuffer

/**
  * 活动主表数据模拟产生
  * Created by ibf on 02/13.
  */
object ActActiveMainDataMock {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("act-active-data")
      .setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)

    // =====================================================
    val random = MockUtils.random
    val dt = "2017-03-04"
    val dtLong = MockUtils.parseString2Long(dt)
    val dtEndLong = dtLong + MockUtils.dayOfMillis
    val actActiveMainSavePath = s"datas/act_active_main/${dt}"
    val fs = FileSystem.get(sc.hadoopConfiguration)
    fs.delete(new Path(actActiveMainSavePath), true)

    val datas = ArrayBuffer[ActActiveMain]()
    for (i <- 0L until MockUtils.boundActiveId) {
      val activeId: Long = i
      val activeName: String = s"active_${activeId}"
      // 九十天前到今天之间的一个时间，格式为yyyy-MM-dd HH:mm:ss SSS
      val addTime: String = MockUtils.getRandomSpecifyDaysAfterDate(dtEndLong, -90, pattern = MockUtils.timePattern)
      // 开始时间到开始时间十天后的一个区间中，但是不能超过今天
      val addTimeLong = MockUtils.parseString2Long(addTime, pattern = MockUtils.timePattern)
      var beginTimeLong = addTimeLong + random.nextInt(10) * MockUtils.dayOfMillis
      val timeLong = Math.min(dtLong, beginTimeLong)
      val beginTime: String = MockUtils.getRandomSpecifyDaysAfterDate(timeLong, 1, pattern = MockUtils.timePattern)
      beginTimeLong = MockUtils.parseString2Long(beginTime, pattern = MockUtils.timePattern)
      val endTimeLong = beginTimeLong + random.nextInt(1, 16) * MockUtils.dayOfMillis
      val endTime: String = MockUtils.getRandomSpecifyDaysAfterDate(endTimeLong, 1, pattern = MockUtils.timePattern)
      val activeTypeId: Long = activeId % 6 // 活动类型为6中，编号分别为: 0 1 2 3 4 5

      // 返回结果
      datas += ActActiveMain(
        activeId, activeName, addTime, beginTime, endTime, activeTypeId
      )
    }

    sc.parallelize(datas)
      .map(active => active.formatted(MockUtils.splitSymbol))
      .repartition(1)
      .saveAsTextFile(actActiveMainSavePath)
  }
}
