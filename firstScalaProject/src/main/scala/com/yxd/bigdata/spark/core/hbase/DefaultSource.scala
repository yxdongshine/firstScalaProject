/*
package com.yxd.bigdata.spark.core.hbase

import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{HBaseAdmin, Put}
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.{MRJobConfig, OutputFormat}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by ibf on 02/16.
  */
class DefaultSource
  extends RelationProvider with CreatableRelationProvider {
  /**
    * 创建获取数据时候构建的Relation
    *
    * @param sqlContext
    * @param parameters
    * @return
    */
  override def createRelation(
                               sqlContext: SQLContext,
                               parameters: Map[String, String]
                             ): BaseRelation = {
    HBaseRelation(parameters)(sqlContext)
  }

  /**
    * 创建保存数据时候的Relation
    *
    * @param sqlContext
    * @param mode
    * @param parameters
    * @param data
    * @return
    */
  override def createRelation(
                               sqlContext: SQLContext,
                               mode: SaveMode,
                               parameters: Map[String, String],
                               data: DataFrame): BaseRelation = {
    HBaseConfiguration.merge(sqlContext.sparkContext.hadoopConfiguration, HBaseConfiguration.create(sqlContext.sparkContext.hadoopConfiguration))
    val hbaseConf = sqlContext.sparkContext.hadoopConfiguration
    val hbaseAdmin = new HBaseAdmin(hbaseConf)
    try {
      val hbaseTableName = parameters.getOrElse("hbase_table_name", sys.error("not valid schema"))
      val family = parameters.getOrElse("hbase_table_family", sys.error("not valid schema"))
      // 获取schema
      val familyAndSchema = if (data.schema.map(_.name.toLowerCase).contains("row_key")) {
        sqlContext.sparkContext.broadcast((Bytes.toBytes(family), data.schema))
      } else {
        sys.error("no row_key column!!!")
      }
      // 过滤schema
      familyAndSchema.value._2.foreach {
        case StructField(_, dataType, _, _) => {
          dataType match {
            case dt: StringType =>
            case dt: DoubleType =>
            case dt: FloatType =>
            case dt: IntegerType =>
            case dt: LongType =>
            case dt: ShortType =>
            case _ => sys.error(s"Can't support those data type of ${dataType}")
          }
        }
      }

      val doSave = if (hbaseAdmin.tableExists(hbaseTableName)) {
        mode match {
          case SaveMode.Append => true
          case SaveMode.ErrorIfExists => sys.error(s"hbase table '$hbaseTableName' already exists.")
          case SaveMode.Ignore => false
          case SaveMode.Overwrite => {
            try
              // 将表设置为disable
              if (hbaseAdmin.isTableEnabled(hbaseTableName)) {
                hbaseAdmin.disableTable(hbaseTableName)
              }
            catch {
              case _: Exception => // nothings
            }
            // 删除表
            hbaseAdmin.deleteTable(hbaseTableName)
            true
          }
        }
      } else {
        true
      }
      if (doSave) {
        // 保存数据
        hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, hbaseTableName)
        hbaseConf.setClass(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, classOf[TableOutputFormat[Any]], classOf[OutputFormat[Any, Any]])

        // 创建表，如果表不存在
        if (!hbaseAdmin.tableExists(hbaseTableName)) {
          // 表不存在，创建一个表
          val desc = new HTableDescriptor(TableName.valueOf(hbaseTableName))
          desc.addFamily(new HColumnDescriptor(family))
          hbaseAdmin.createTable(desc)
        }

        data.rdd.map(row => {
          val family = familyAndSchema.value._1
          val schema = familyAndSchema.value._2

          val buffers = schema.map {
            case StructField(name, dataType, nullable, _) => {
              val isRowKey = "row_key".equalsIgnoreCase(name)
              val value = dataType match {
                case dt: StringType => Bytes.toBytes(row.getAs[String](name))
                case dt: DoubleType => Bytes.toBytes(row.getAs[Double](name))
                case dt: FloatType => Bytes.toBytes(row.getAs[Float](name))
                case dt: IntegerType => Bytes.toBytes(row.getAs[Integer](name))
                case dt: LongType => Bytes.toBytes(row.getAs[Long](name))
                case dt: ShortType => Bytes.toBytes(row.getAs[Short](name))
                case _ => sys.error(s"can't support those data type of ${dataType}")
              }
              // 返回结果
              (if (isRowKey) "row_key" else name, value)
            }
          }

          // 构建Put对象
          val rowKey = buffers.toMap.getOrElse("row_key", sys.error(""))
          val put = buffers.filter(!_._1.equals("row_key")).foldLeft(new Put(rowKey))((put, b) => {
            put.add(family, Bytes.toBytes(b._1), b._2)
            put
          })
          // 返回对象
          (null, put)
        }).saveAsNewAPIHadoopDataset(hbaseConf)
      }
    } finally {
      if (hbaseAdmin != null) hbaseAdmin.close()
    }

    // 返回结果
    HBaseRelation(parameters)(sqlContext)
  }
}
*/
