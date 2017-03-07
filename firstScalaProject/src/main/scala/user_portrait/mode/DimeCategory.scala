package user_portrait.mode

/**
  * Created by ibf on 02/08.
  */
case class DimeCategory(
                         categoryId: Long,
                         categoryName: String,
                         firstLevelCategoryId: Long,
                         firstLevelCategoryName: String,
                         secondLevelCategoryId: Long,
                         secondLevelCategoryName: String,
                         thirdLevelCategoryId: Long,
                         thirdLevelCategoryName: String
                       ) {
  /**
    * 根据给定的分隔符格式化数据，形成字符串
    *
    * @param splitSymbol
    * @return
    */
  def formatted(splitSymbol: String): String = {
    s"${categoryId}${splitSymbol}${categoryName}${splitSymbol}${firstLevelCategoryId}${splitSymbol}${firstLevelCategoryName}${splitSymbol}${secondLevelCategoryId}${splitSymbol}${secondLevelCategoryName}${splitSymbol}${thirdLevelCategoryId}${splitSymbol}${thirdLevelCategoryName}"
  }
}
