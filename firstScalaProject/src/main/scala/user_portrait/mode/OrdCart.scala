package user_portrait.mode

import user_portrait.util.MockUtils


/**
  * Created by ibf on 02/04.
  */
case class OrdCart(id: Long,
                   sessionId: String,
                   userId: Long,
                   goodId: Long,
                   number: Long,
                   addTime: String,
                   var cancelTime: String,
                   var submitTime: String) {

  /**
    * 更新提交时间
    */
  def updateSubmitTime(): Unit = {
    this.submitTime = MockUtils.getRandomSpecifyDaysAfterDate(
      MockUtils.parseString2Long(this.addTime, MockUtils.timePattern))
  }

  /**
    * 更新取消时间
    */
  def updateCancelTime(): Unit = {
    this.cancelTime = MockUtils.getRandomSpecifyDaysAfterDate(
      MockUtils.parseString2Long(this.addTime, MockUtils.timePattern))
  }

  /**
    * 按照给定分隔符分割数据返回字符串
    *
    * @param splitSymbol
    * @return
    */
  def formatted(splitSymbol: String): String = {
    s"${id}${splitSymbol}${sessionId}${splitSymbol}${userId}${splitSymbol}${goodId}${splitSymbol}${number}${splitSymbol}${addTime}${splitSymbol}${cancelTime}${splitSymbol}${submitTime}"
  }
}
