package user_portrait.mode

/**
  * Created by ibf on 02/07.
  */
case class OrdOrderGoods(orderId: Long,
                         goodId: Long,
                         categoryId: Long,
                         categoryName: String,
                         sizeId: Long,
                         colourId: Long,
                         goodsPrice: Double,
                         goodsAmount: Long,
                         createTime: String,
                         lastUpdateTime: String) {
  /**
    * 根据给定的分隔符格式化数据，形成字符串
    *
    * @param splitSymbol
    * @return
    */
  def formatted(splitSymbol: String): String = {
    s"${orderId}${splitSymbol}${goodId}${splitSymbol}${categoryId}${splitSymbol}${categoryName}${splitSymbol}${sizeId}${splitSymbol}${colourId}${splitSymbol}${goodsPrice}${splitSymbol}${goodsAmount}${splitSymbol}${createTime}${splitSymbol}${lastUpdateTime}"
  }
}
