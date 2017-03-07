package user_portrait.mode

/**
  * Created by ibf on 02/13.
  */
case class ActActiveMain(
                          activeId: Long,
                          activeName: String,
                          addTime: String,
                          beginTime: String,
                          endTime: String,
                          activeTypeId: Long
                        ) {
  /**
    * 根据给定的分隔符格式化数据，形成字符串
    *
    * @param splitSymbol
    * @return
    */
  def formatted(splitSymbol: String): String = {
    s"${activeId}${splitSymbol}${activeName}${splitSymbol}${addTime}${splitSymbol}${beginTime}${splitSymbol}${endTime}${splitSymbol}${activeTypeId}"
  }
}
