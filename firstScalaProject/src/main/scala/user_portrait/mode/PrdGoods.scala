package user_portrait.mode

/**
  * Created by ibf on 02/09.
  */
case class PrdGoods(
                     skuId: Long, //SKU编号, 同一件商品，不同尺码就是不同skuid的值
                     skuName: String, //SKU名称
                     goodsId: Long, //商品编号
                     goodsName: String, //商品名称
                     goodsNo: String, //商品货号
                     goodsSn: String, //商品条码
                     sizeId: Long, //尺码编号
                     sizeName: String, //尺码名称
                     colourId: Long, //颜色ID
                     shopId: Long, //店铺ID
                     shopName: String, //店铺名称
                     currPrice: Double, //售卖价格
                     marketPrice: Double, //市场价格
                     discount: Double, //折扣比率
                     costPrice: Double, //成本价格
                     costType: String, //成本类型
                     warehouse: String, //所在仓库
                     stockCnt: Long, //进货数量
                     stockAmt: Double, //进货货值
                     categoryID: Long, //品类ID
                     categoryName: String //品类名称
                   ) {
  /**
    * 根据给定的分隔符格式化数据，形成字符串
    *
    * @param splitSymbol
    * @return
    */
  def formatted(splitSymbol: String): String = {
    s"${skuId}${splitSymbol}${skuName}${splitSymbol}${goodsId}${splitSymbol}${goodsName}${splitSymbol}${goodsNo}${splitSymbol}${goodsSn}${splitSymbol}${sizeId}${splitSymbol}${sizeName}${splitSymbol}${colourId}${splitSymbol}${shopId}${splitSymbol}${shopName}${splitSymbol}${currPrice}${splitSymbol}${marketPrice}${splitSymbol}${discount}${splitSymbol}${costPrice}${splitSymbol}${costType}${splitSymbol}${warehouse}${splitSymbol}${stockCnt}${splitSymbol}${stockAmt}${splitSymbol}${categoryID}${splitSymbol}${categoryName}"
  }
}

