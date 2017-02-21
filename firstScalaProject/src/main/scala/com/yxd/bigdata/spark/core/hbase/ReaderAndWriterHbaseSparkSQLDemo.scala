//package com.yxd.bigdata.spark.core.hbase
//
//import org.apache.spark.sql.{SQLContext, SaveMode}
//import org.apache.spark.{SparkConf, SparkContext}
//
///**
//  * 读写HBase SparkSQL Demo案例
//  * Created by ibf on 02/16.
//  */
//object ReaderAndWriterHbaseSparkSQLDemo {
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf()
//      .setMaster("local[*]")
//      .setAppName("hbase-reader")
//      .set("spark.hadoop.hbase.zookeeper.quorum", "hadoop-senior01:2181")
//    val sc = SparkContext.getOrCreate(conf)
//    val sqlContext = new SQLContext(sc)
//
//    // ==========================================
//    sqlContext
//      .read
//      .format("com.ibeifeng.spark.sql.hbase")
//      .option("sparksql_table_schema", "(rowkey string, ca string, cb string, cc string)")
//      .option("hbase_table_name", "test")
//      .option("hbase_table_schema", "(:key,info:a,info:b,info:c)")
//      .load()
//      .registerTempTable("t_abc")
//
//    sqlContext.sql("select * from t_abc").show()
//
//    sqlContext.sql("select * from t_abc").toDF("row_key", "v1", "v2", "v3")
//      .write
//      .mode(SaveMode.Overwrite)
//      .format("com.ibeifeng.spark.sql.hbase")
//      .option("hbase_table_name", "test2")
//      .option("hbase_table_family", "info")
//      .save() // 要求实现CreatableRelationProvider接口
//
//    sqlContext.createDataFrame(Array(
//      (1, "gerry", "13166291750", 12345L),
//      (2, "Tom", "132521412", 12121L),
//      (3, "张三", "1232512542", 125215L),
//      (4, "李四", "1235215421", 12351L)
//    )).toDF("row_key", "name", "phone", "salary")
//      .write
//      .mode(SaveMode.Append)
//      .format("com.ibeifeng.spark.sql.hbase")
//      .option("hbase_table_name", "test3")
//      .option("hbase_table_family", "info")
//      .save()
//
//    sqlContext
//      .read
//      .format("com.ibeifeng.spark.sql.hbase")
//      .option("sparksql_table_schema", "(id string, name string, phone string, salary long)")
//      .option("hbase_table_name", "test3")
//      .option("hbase_table_schema", "(:key,info:name,info:phone,info:salary)")
//      .load()
//      .show()
//  }
//}
