package user_portrait.util

import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.concurrent.ThreadLocalRandom

import scala.collection.mutable.ArrayBuffer
import scala.math.BigDecimal.RoundingMode

/**
  * Created by ibf on 02/04.
  */
object MockUtils {
  // 随机对象
  val random = ThreadLocalRandom.current()
  // 数据分割符号
  val splitSymbol = ","
  // 最多10万个用户， ID上限
  val boundUserId = 100000L
  // 最多100个地域，ID上限
  val boundAreaId = 100L
  // 最多100万个商品，ID上限
  val boundGoodId = 1000000L
  // 最多类目，ID上限
  val boundCategoryId = 100L
  // 最多活动，ID上限
  val boundActiveId = 100000L
  // 订单类型
  val orderTypes = Array("后台订单", "PC端订单", "移动端订单", "线上订单", "第三方订单", "其他订单")
  val orderTypesSize = orderTypes.length
  // 订单来源
  val orderSources = Array("Web端(android)", "Web端(ios)", "Web端(其他)", "PC端", "线下门店", "微商城")
  val orderSourcesSize = orderSources.length
  // 订单支付方式
  val payTypes = Array("weixin", "alipay", "信用卡", "财付通", "一网通", "其它")
  val payTypesSize = payTypes.length
  // 用户星座(用于随机)
  val constellations = Array("白羊座", "金牛座", "双子座", "巨蟹座", "狮子座", "处女座", "天枰座", "天蝎座", "射手座", "摩羯座", "水瓶座", "双鱼座")
  val constellationsSize = constellations.length
  // 原始字符串随机值
  val allCharters = "qazwsxedcrfvtgbyhnujmikolp0123456789".toCharArray
  val allChartersSize = allCharters.length
  // 邮箱运营商列表
  val opMails = Array("qq", "163", "139", "hotmail")
  val opMailsSize = opMails.length
  // 手机号码运营商列表
  val opPhones = Array("电信", "移动", "联通")
  val opPhonesSize = opPhones.length

  // 时间格式
  val timePattern = "yyyy-MM-dd HH:mm:ss SSS"
  val datePattern = "yyyy-MM-dd"

  // 一天的毫秒数
  val dayOfMillis = 86400000

  //优惠券使用状态
  val couponStatus = Array(0,1)



  /**
    * 随机产生n个不重复的商品id
    *
    * @param n
    * @return
    */
  def mockGoodIds(n: Int): Array[Long] = {
    if (n > this.boundGoodId) throw new IllegalArgumentException

    import scala.collection.mutable
    val result = mutable.Set[Long]()
    def addGoodIds(): Unit = {
      // 数据累加
      (0 until (n - result.size)).foreach(i => {
        val goodId = random.nextLong(this.boundGoodId)
        result += goodId
      })

      // 如果小于的话，那么累加
      if (result.size < n) addGoodIds()
    }

    // 开始添加商品ID
    addGoodIds()

    // 返回结果
    result.toArray
  }

  /**
    * 进行数据分割，符合给定要求的数据放到返回二元组的第一个元素位置；不符合要求的放到第二个元素位置
    *
    * @param datas
    * @param p
    * @tparam A
    * @return
    */
  def span[A](datas: ArrayBuffer[A], p: A => Boolean): (ArrayBuffer[A], ArrayBuffer[A]) = {
    val result = (ArrayBuffer[A](), ArrayBuffer[A]())
    datas.foldLeft(result)((a, b) => {
      // 判断数据累加
      if (p(b)) a._1 += b
      else a._2 += b

      // 返回结果
      a
    })

    // 最终结果返回
    result
  }

  /**
    * 随机一个登陆IP地址
    *
    * @return
    */
  def randomLoginIp = {
    def randomeIpEndpoint = random.nextInt(0, 256)
    s"${randomeIpEndpoint}.${randomeIpEndpoint}.${randomeIpEndpoint}.${randomeIpEndpoint}"
  }

  /**
    * 随机一个邮箱
    *
    * @return
    */
  def randomMail = {
    val userMail = randomString(random.nextInt(2, 10))
    val opMail = this.opMails(random.nextInt(this.opMailsSize))
    val suffixMail = Array("com", "org", "cn")
    (s"${userMail}@${opMail}.${suffixMail(random.nextInt(suffixMail.length))}", opMail)
  }

  /**
    * 随机一个长度为n的字符串
    *
    * @param n
    * @return
    */
  def randomString(n: Int): String = (0 until n).foldLeft("")((a, b) => s"${a}${allCharters(random.nextInt(allChartersSize))}")

  /**
    * 随机一个电话号码
    *
    * @return
    */
  def randomMobilephone = {
    val forePhone = randomNumbersString(4)
    val opPhone = this.opPhones(random.nextInt(this.opPhonesSize))
    (s"${forePhone}${randomNumbersString(7)}", forePhone, opPhone)
  }

  /**
    * 随机一个固定电话
    *
    * @return
    */
  def randomTelphone = randomNumbersString(4) + "-" + randomNumbersString(8)

  /**
    * 随机一个n个数字组成的数字字符串
    *
    * @param n
    * @return
    */
  def randomNumbersString(n: Int): String = (0 until n - 1).foldLeft(s"${random.nextInt(1, 10)}")((a, b) => s"${a}${random.nextInt(10)}")

  /**
    * 随机出来一个生日日期，格式为:yyyy-MM-dd的字符串
    *
    * @return
    */
  def randomBirthday = {
    val cal = Calendar.getInstance()
    val year = random.nextInt(5, 85)
    cal.add(Calendar.YEAR, -year) // 往前推一些年份
    cal.set(Calendar.MONTH, random.nextInt(12)) // 随机一个月份
    cal.add(Calendar.DAY_OF_MONTH, random.nextInt(30)) // 随机添加一些天数
    new SimpleDateFormat(datePattern).format(cal.getTime)
  }

  /**
    * 计算一下年龄
    *
    * @param userBirthday 生日日期，格式为:yyyy-MM-dd
    * @return
    */
  def calculateAgeByBirthday(userBirthday: String): Long = {
    val cal = Calendar.getInstance()
    val yearNow = cal.get(Calendar.YEAR)
    val dayNow = cal.get(Calendar.DAY_OF_YEAR)

    cal.setTimeInMillis(this.parseString2Long(userBirthday))
    val yearBirthday = cal.get(Calendar.YEAR)
    val dayBirthday = cal.get(Calendar.DAY_OF_YEAR)

    val age =
      if (dayNow < dayBirthday) yearNow - yearBirthday - 1
      else yearNow - yearBirthday

    Math.max(0, age)
  }

  /**
    * 格式化double类型的数据，保留scale位小数点
    *
    * @param value
    * @param scale
    * @return
    */
  def formatDoubleOfScale(value: Double, scale: Int = 2): Double = {
    BigDecimal.valueOf(value).setScale(scale, RoundingMode.HALF_UP).doubleValue()
  }

  /**
    * 将给定pattern格式的date字符串转换为long类型
    *
    * @param date
    * @param pattern
    * @return
    */
  def parseString2Long(date: String, pattern: String = datePattern): Long = {
    new SimpleDateFormat(pattern).parse(date).getTime
  }

  /**
    * 获取一个指定特殊天数的日期
    *
    * @param date
    * @param specifyDays
    * @param pattern
    * @return
    */
  def getRandomSpecifyDaysAfterDate(date: Long, specifyDays: Int = 1, pattern: String = timePattern): String = {
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(date)
    cal.add(Calendar.DAY_OF_YEAR, specifyDays)
    cal.set(Calendar.MILLISECOND, 0)
    cal.set(Calendar.SECOND, 0)
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.HOUR_OF_DAY, 0)
    val endDate = cal.getTimeInMillis
    val startDateMillis = Math.min(date, endDate)
    val endDateMillis = Math.max(date, endDate)
    val randomeDate = random.nextLong(startDateMillis, endDateMillis)
    cal.setTimeInMillis(randomeDate)
    new SimpleDateFormat(pattern).format(cal.getTime)
  }
}
