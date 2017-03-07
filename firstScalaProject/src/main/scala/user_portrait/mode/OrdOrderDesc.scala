package user_portrait.mode

/**
  * Created by ibf on 02/04.
  */
case class OrdOrderDesc(
                         orderID: Long,
                         orderNO: String,
                         consignee: String,
                         areaID: Long,
                         areaName: String,
                         address: String,
                         mobilephone: String,
                         telphone: String,
                         couponId: Long,
                         couponMoney: Double,
                         carriageMoney: Double,
                         createTime: String,
                         lastUpdateTime: String,
                         orderAddr: Long
                       ) {
  /**
    * 根据给定的分隔符格式化数据，形成字符串
    *
    * @param splitSymbol
    * @return
    */
  def formatted(splitSymbol: String): String = {
    s"""${orderID}${splitSymbol}${orderNO}${splitSymbol}${consignee}${splitSymbol}${areaID}${splitSymbol}${areaName}${splitSymbol}${address}${splitSymbol}${mobilephone}${splitSymbol}${telphone}${splitSymbol}${couponId}${splitSymbol}${couponMoney}${splitSymbol}${carriageMoney}${splitSymbol}${createTime}${splitSymbol}${lastUpdateTime}${splitSymbol}${orderAddr}"""
  }
}
