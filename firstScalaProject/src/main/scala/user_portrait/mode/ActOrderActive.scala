package user_portrait.mode

/**
  * Created by ibf on 02/13.
  */
case class ActOrderActive(
                           orderId: Long,
                           activeId: Long,
                           addTime: String
                         ) {
  /**
    * 根据给定的分隔符格式化数据，形成字符串
    *
    * @param splitSymbol
    * @return
    */
  def formatted(splitSymbol: String): String = {
    s"${orderId}${splitSymbol}${activeId}${splitSymbol}${addTime}"
  }

}
