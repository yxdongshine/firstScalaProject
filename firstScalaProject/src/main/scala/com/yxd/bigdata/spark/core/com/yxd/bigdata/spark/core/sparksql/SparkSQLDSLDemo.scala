package com.yxd.bigdata.spark.core.com.yxd.bigdata.spark.core.sparksql

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.math.BigDecimal.RoundingMode

/**
  * DSL语句测试讲解
  * Created by ibf on 02/19.
  */
object SparkSQLDSLDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("dsl")
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = new HiveContext(sc)
    sqlContext.udf.register(
      "doubleValueFormat", // 自定义函数名称
      (value: Double, scale: Int) => {
        // 自定义函数处理的代码块
        BigDecimal.valueOf(value).setScale(scale, RoundingMode.HALF_DOWN).doubleValue()
      })
    sqlContext.udf.register("selfAvg",SparkSqlUdaf)

    import sqlContext.implicits._
    import org.apache.spark.sql.functions._

    // =================================================
    sqlContext.sql(
      """
        | SELECT
        |  deptno as no,
        |  SUM(sal) as sum_sal,
        |  AVG(sal) as avg_sal,
        |  SUM(mgr) as sum_mgr,
        |  AVG(mgr) as avg_mgr
        | FROM common.emp
        | GROUP BY deptno
        | ORDER BY deptno DESC
        | LIMIT 2
      """.stripMargin).show()

    // 读取数据形成DataFrame，并缓存DataFrame
    val df = sqlContext.read.table("common.emp")
    df.cache()

    df.select("deptno", "sal", "mgr")
      .groupBy("deptno")
      .agg(
        sum("sal").as("sum_sal"),
        avg("sal").as("avg_sal"),
        sum("mgr").as("sum_mgr"),
        avg("mgr").as("avg_mgr")
      )
      .orderBy($"deptno".desc)
      .limit(2)
      .show()

    /**
      * DataFrame相关API(DSL)调用的前提是：DataFrame中有存在的列名
      * df.where("sal > 2000").select("empno", "ename").show
      * df.select("empno", "ename").where("sal > 2000").show ===> 异常：sal不存在dataframe中
      **/


    // select语句
    df.select("empno", "ename", "deptno").show()
    df.select(col("empno").as("id"), $"ename".as("name"), df("deptno")).show()
    df.select($"empno".as("id"), substring($"ename", 0, 1).as("name")).show()
    df.selectExpr("empno as id", "substring(ename,0,1) as name").show()
    df.selectExpr("doubleValueFormat(sal,2)").show()
    // where语句 <====> filter语句 ====> 注意一点：操作符的优先级
    df.where("sal > 1000 and sal < 2000").show()
    df.where($"sal" > 1000 && $"sal" < 2000).show()

    // groupBy语句
    df.groupBy("deptno").agg(
      "sal" -> "min", // 求min(sal)
      "sal" -> "max", // 求max(sal) ===> 会覆盖同列的其他聚合函数，解决方案：重新命名
      "mgr" -> "max" // 求max(mgr)
    ).show()
    df.groupBy("deptno").agg(
      "sal" -> "selfAvg"
    ).toDF("deptno", "self_avg_sal").show()
    df.groupBy("deptno").agg(
      min("sal").as("min_sal"),
      max("sal").as("max_sal"),
      max("mgr")
    ).where("min_sal > 1200").show()

    // 数据排序
    // sort、orderBy ==> 全局有序
    // repartition ==> 局部数据有序
    df.sort("sal").select("empno", "sal").show()
    df.repartition(3).sort($"sal".desc).select("empno", "sal").show()
    df.repartition(3).orderBy($"sal".desc).select("empno", "sal").show()
    df.repartition(3).sortWithinPartitions($"sal".desc).select("empno", "sal").show()

    // limit
    df.limit(5).show()

    // Hive的窗口分析函数
    // 必须使用HiveContext来构建DataFrame
    // 通过row_number函数来实现分组排序TopN的需求: 先按照某些字段进行数据分区，然后分区的数据在分区内进行topN的获取
    val window = Window.partitionBy("deptno").orderBy($"sal".desc)
    df.select(
      $"empno",
      $"ename",
      $"deptno",
      row_number().over(window).as("rnk")
    ).where("rnk <= 3").show()
  }
}
