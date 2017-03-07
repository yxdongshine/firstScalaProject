package user_portrait.mode

import user_portrait.util.MockUtils


/**
  * Created by ibf on 02/04.
  */
case class OrdOrder(orderId: Long,
                    orderNo: String,
                    orderDate: String,
                    createTime: String,
                    userId: Long,
                    userName: String,
                    orderMoney: Double,
                    orderType: String,
                    var orderStatus: String,
                    var payType: String,
                    var payStatus: String,
                    orderSource: String,
                    var lastUpdateTime: String
                   ) {
  /**
    * 更新修改时间
    */
  def updateLastUpdateTime(): Unit = {
    this.lastUpdateTime = MockUtils.getRandomSpecifyDaysAfterDate(
      MockUtils.parseString2Long(this.lastUpdateTime, MockUtils.timePattern))
  }

  /**
    * 按照给定分隔符分割数据返回字符串
    *
    * @param splitSymbol
    * @return
    */
  def formatted(splitSymbol: String): String = {
    s"""${orderId}${splitSymbol}${orderNo}${splitSymbol}${orderDate}${splitSymbol}${createTime}${splitSymbol}${userId}${splitSymbol}${userName}${splitSymbol}${orderMoney}${splitSymbol}${orderType}${splitSymbol}${orderStatus}${splitSymbol}${payType}${splitSymbol}${payStatus}${splitSymbol}${orderSource}${splitSymbol}${lastUpdateTime}"""
  }
}

