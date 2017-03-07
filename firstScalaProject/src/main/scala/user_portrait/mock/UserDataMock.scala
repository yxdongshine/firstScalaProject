package user_portrait.mock

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import user_portrait.mode.User
import user_portrait.util.MockUtils

import scala.collection.mutable.ArrayBuffer

/**
  * 用户模拟数据产生
  * Created by ibf on 02/04.
  */
object UserDataMock {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("test1")
      .setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)

    // =============================
    val dt = "2017-03-04"
    val dtLong = MockUtils.parseString2Long(dt)
    val dtEndLong = dtLong + MockUtils.dayOfMillis
    val random = MockUtils.random
    val userSavePath = s"datas/user/${dt}"
    val fs = FileSystem.get(sc.hadoopConfiguration)
    fs.delete(new Path(userSavePath), true)

    val userDatas = ArrayBuffer[User]()
    for (i <- 0L until MockUtils.boundUserId) {
      val userId = i
      val userName = s"user_${userId}"
      // 40%的几率是男，40%的几率是女
      val userSex = if (random.nextDouble(1) <= 0.8) {
        if (random.nextBoolean()) "男"
        else "女"
      } else ""
      val userBirthday = MockUtils.randomBirthday
      val userAge = MockUtils.calculateAgeByBirthday(userBirthday)
      val constellation = MockUtils.constellations(random.nextInt(MockUtils.constellationsSize))
      val bigAreaName = s"area_${random.nextInt(10)}"
      val country = s"country_${random.nextInt(100)}"
      val province = s"province_${random.nextInt(20)}"
      val city = s"city_${random.nextInt(10)}"
      val cityLevel = random.nextInt(1, 6).toString
      val (hexMail, opMail) = MockUtils.randomMail
      val (hexPhone, forePhone, opPhone) = MockUtils.randomMobilephone
      val addTime = MockUtils.getRandomSpecifyDaysAfterDate(dtEndLong, -90)
      val loginIp = MockUtils.randomLoginIp
      val logSource = s"source_${random.nextInt(10)}"
      val requestUserId =
        if (random.nextBoolean()) {
          val t = random.nextLong(MockUtils.boundUserId)
          if (t == userId) -1 else t
        } else -1
      val totalMark = random.nextLong(0, 1000000)
      val usedMark = random.nextLong(0, totalMark + 1)
      val levelName = random.nextInt(1, 6).toString
      val blacklist =
        if (random.nextDouble(1) <= 0.95) {
          // 95%是正常用户
          0
        } else {
          // 5%的是黑名单用户
          1
        }
      val t = random.nextDouble(1)
      val isMarried =
        if (t <= 0.4) {
          // 40%的用户未婚
          0
        } else if (t <= 0.75) {
          // 35%的用户已婚
          1
        } else if (t <= 0.95) {
          // 20%的用户离异
          2
        } else if (t <= 0.97) {
          // 2%的用户丧偶
          3
        } else {
          // 3%的用户未知
          4
        }
      val education = s"education_${random.nextInt(5)}"
      val profession = s"profession_${random.nextInt(50)}"

      // 构建用户
      userDatas += User(userId, userName, userSex, userBirthday, userAge, constellation, bigAreaName, country, province, city, cityLevel, hexMail, opMail, hexPhone, forePhone, opPhone, addTime, loginIp, logSource, requestUserId, totalMark, usedMark, levelName, blacklist, isMarried, education, profession)
    }

    // 构建RDD
    val rdd = sc.parallelize(userDatas)

    // rdd数据结果保存
    rdd
      .map(user => user.formatted(MockUtils.splitSymbol))
      .repartition(1)
      .saveAsTextFile(userSavePath)
  }

}
