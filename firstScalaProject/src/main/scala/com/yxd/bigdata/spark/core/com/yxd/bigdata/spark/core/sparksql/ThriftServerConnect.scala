package com.yxd.bigdata.spark.core.com.yxd.bigdata.spark.core.sparksql

import java.sql.DriverManager

/**
 * Created by 20160905 on 2017/2/20.
 */
object ThriftServerConnect {

  def main(args: Array[String]) {
    Class.forName("org.apache.hive.jdbc.HiveDriver")

    //链接属性 其中用户名和密码可以随便写
    val  (url,name,password) =("jdbc:hive2://hadoop1:10000","spark","spark")
    //获取连接
    val connection = DriverManager.getConnection(url,name,password)
    //获取statement 切换数据库
    connection.prepareStatement("use default").execute()
    //查询数据
    val sql = "select * from  stats_hourly"
    val stmt = connection.prepareStatement(sql)
    val rs = stmt.executeQuery()
    while (rs.next()){
          println(s"${rs.getInt("hour20")}:${rs.getInt("hour23")}")
    }

    //依次关闭连接资源
    rs.close()
    stmt.close()
    connection.close()
  }

}
