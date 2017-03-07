package user_portrait.mock

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import user_portrait.mode.DimeCategory
import user_portrait.util.MockUtils

import scala.collection.mutable.ArrayBuffer

/**
  * 类目维度表
  * Created by ibf on 02/07.
  */
object CategoryDimeDataMock {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("test1")
      .setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)

    // ==================================
    val random = MockUtils.random
    val categoryDimeSavePath = s"datas/dime_category/"
    val fs = FileSystem.get(sc.hadoopConfiguration)
    fs.delete(new Path(categoryDimeSavePath), true)

    val categoryDimeDatas = new ArrayBuffer[DimeCategory]()
    for (i <- 0L until MockUtils.boundCategoryId) {
      // 假设有
      // 4 * 5 * 5 = 100 => 4个一级的类目，每个一级类目有五个二级类目，每个二级类目有五个三级类目
      val categoryId = i
      val categoryName = s"category_${categoryId}"
      val firstLevelCategoryId = (i % 4)
      val firstLevelCategoryName = s"first_${firstLevelCategoryId}"
      val secondLevelCategoryId = (i / 4) / 5
      val secondLevelCategoryName = s"second_${firstLevelCategoryId}_${secondLevelCategoryId}"
      val thirdLevelCategoryId = (i / 4) % 5
      val thirdLevelCategoryName = s"third_${firstLevelCategoryId}_${secondLevelCategoryId}_${thirdLevelCategoryId}"

      categoryDimeDatas += DimeCategory(categoryId, categoryName, firstLevelCategoryId, firstLevelCategoryName, secondLevelCategoryId, secondLevelCategoryName, thirdLevelCategoryId, thirdLevelCategoryName)
    }

    val rdd = sc.parallelize(categoryDimeDatas)

    rdd.map(category => category.formatted(MockUtils.splitSymbol))
      .repartition(1)
      .saveAsTextFile(categoryDimeSavePath)

  }
}
