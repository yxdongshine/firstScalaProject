package com.yxd.bigdata.spark.core.com.yxd.bigdata.spark.core.sparksql

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Administrator on 2017/2/22 0022.
 */
object SparkHiveWindow {

  def main(args: Array[String]) {

    //基本结构
    val sparkConf  = new SparkConf().setMaster("local[*]").setAppName("JdbcSparkSql")
    val sc = SparkContext.getOrCreate(sparkConf)
    //SqlContext 和 HiveContext 区别 后者继承了sqlContext ;不需要hive相关内容就
    //使用前者，需要就使用后者；后者缺陷就是易产生perm menmery 内存溢出
    val sqlContext = new HiveContext(sc)
    //注册临时表
    sqlContext
      .read
      .table("stats_hourly")
    .registerTempTable("tmp_stats_hourly")


    //读hive表数据进入展示
    sqlContext
      .read
      .table("stats_hourly")
      .show()


    println("=========================================")
    //统计各个平台 在十一点小于104指标的全部信息
    val  sql1 =
      "select " +
      "tsh.platform_dimension_id , tsh.date_dimension_id,tsh.kpi_dimension_id,tsh.hour11, " +
      " sum(tsh.hour11) over(partition by tsh.platform_dimension_id , tsh.hour11)" +
        "from tmp_stats_hourly as tsh "/* +
        "where tsh.hour11 = 104 "*/
    sqlContext
      .sql(sql1)
    .show()


    println("=============lag============================")
    /**
     * LAG(col,n,DEFAULT) 用于统计窗口内往上第n行值
第一个参数为列名，第二个参数为往上第n行（可选，默认为1），第三个参数为默认值（当往上第n行为NULL时候，取默认值，如不指定，则为NULL）
     */
    val  sql2  =
      "select " +
        "tsh.platform_dimension_id , tsh.date_dimension_id,tsh.kpi_dimension_id,tsh.hour11, " +
        " lag(tsh.hour11 , 1,0) over(partition by tsh.platform_dimension_id) " +
        "from tmp_stats_hourly as tsh "
    sqlContext
      .sql(sql2)
      .show()

    println("=============组内  排序 topN============================")
    /**
     * dense_rank和rank都是排名函数，区别在于dense_rank是连续排名，rank遇到排名并列时，下一列排名跳空
     */
    val  sql3  =
    "select * from ("+
      "select " +
        "tsh.platform_dimension_id , tsh.date_dimension_id,tsh.kpi_dimension_id,tsh.hour11, " +
        " rank()  over(partition by tsh.platform_dimension_id order by hour11 desc ), " +
        " ROW_NUMBER() over(partition by tsh.platform_dimension_id order by hour11 desc ) as rn "+
        "from tmp_stats_hourly as tsh "+
    " ) t " +
      "where t.rn < 2 "
    sqlContext
      .sql(sql3)
      .show()



    println("=============udf============================")

    /**
     * 在上面的组内排序的基础上 将platform_dimension_id 映射成具体的平台名称
     */

    //注册udf函数


    sqlContext.udf.register("platformMap",
      (s: String) => {
        s match {
          case "1" =>("浏览器")
          case "2" =>("手机")
          case "3" =>("系统")
          case _ =>("其他")
        }

      }
    )

    val  sql4 =
      "select * from ("+
        "select " +
        "platformMap(tsh.platform_dimension_id) , tsh.date_dimension_id,tsh.kpi_dimension_id,tsh.hour11, " +
        " rank()  over(partition by tsh.platform_dimension_id order by hour11 desc ), " +
        " ROW_NUMBER() over(partition by tsh.platform_dimension_id order by hour11 desc ) as rn "+
        "from tmp_stats_hourly as tsh "+
        " ) t " +
        "where t.rn < 2 "
    sqlContext
      .sql(sql4)
      .show()


    println("=============udaf============================")
    //实现统计按照平台23点的浏览总数和平均数
    val  sql5 =
      "select avg(t.hour23), avgUdaf(t.hour23) from ("+
        "select " +
          "platformMap(tsh.platform_dimension_id) as pname, tsh.date_dimension_id,tsh.kpi_dimension_id,tsh.hour23, " +
          " rank()  over(partition by tsh.platform_dimension_id order by hour23 desc ), " +
          " ROW_NUMBER() over(partition by tsh.platform_dimension_id order by hour23 ) as rn  "+
          "from tmp_stats_hourly as tsh "+
      " ) t " +
        "group by t.hour23"
    //注册udaf
    sqlContext.udf.register("avgUdaf",SparkSqlUdaf)

    sqlContext
    .sql(sql5)
    .show()
  }

}
