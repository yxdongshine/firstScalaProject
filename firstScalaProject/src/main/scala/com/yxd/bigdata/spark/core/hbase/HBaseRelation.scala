//package com.yxd.bigdata.spark.core.hbase
//
//import org.apache.hadoop.hbase.HBaseConfiguration
//import org.apache.hadoop.hbase.client.Result
//import org.apache.hadoop.hbase.mapreduce.TableInputFormat
//import org.apache.hadoop.hbase.util.Bytes
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
//import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, InsertableRelation, TableScan}
//import org.apache.spark.sql.types._
//
//import scala.collection.mutable.ArrayBuffer
//
///**
//  * Created by ibf on 02/16.
//  */
//case class HBaseRelation(@transient val parameters: Map[String, String])
//                        (@transient val sqlContext: SQLContext)
//  extends BaseRelation with Serializable with TableScan {
//
//  private case class SchemaType(dataType: DataType, nullable: Boolean)
//
//  private lazy val (hbaseTableName, hbaseTableFields, fieldsRelations, queryColumns, startRowKey, endRowKey) =
//    try {
//      val hbaseTableName = parameters.getOrElse("hbase_table_name", sys.error("not valid schema"))
//      val hbaseTableSchema = parameters.getOrElse("hbase_table_schema", sys.error("not valid schema"))
//      val registerTableSchema = parameters.getOrElse("sparksql_table_schema", sys.error("not valid schema"))
//      val rowRange = parameters.getOrElse("row_range", "->")
//      //get star row and end row
//      val range = rowRange.split("->", -1)
//      val startRowKey = range(0).trim
//      val endRowKey = range(1).trim
//
//      val tempHBaseFields = extractHBaseSchema(hbaseTableSchema) //do not use this, a temp field
//      val registerTableFields = extractRegisterSchema(registerTableSchema)
//      val tempFieldRelation = tableSchemaFieldMapping(tempHBaseFields, registerTableFields)
//
//      val hbaseTableFields = feedTypes(tempFieldRelation)
//      val fieldsRelations = tableSchemaFieldMapping(hbaseTableFields, registerTableFields)
//      val queryColumns = getQueryTargetCloumns(hbaseTableFields)
//
//      (hbaseTableName, hbaseTableFields, fieldsRelations, queryColumns, startRowKey, endRowKey)
//    } catch {
//      case e: RuntimeException => throw e
//      case e: Exception => sys.error(s"初始化HBaseRelation发生异常, 异常信息为:${e.getMessage}")
//    }
//
//  def feedTypes(mapping: Map[HBaseSchemaField, RegisteredSchemaField]): Array[HBaseSchemaField] = {
//    val hbaseFields = mapping.map {
//      case (k, v) =>
//        val field = k.copy(fieldType = v.fieldType)
//        field
//    }
//    hbaseFields.toArray
//  }
//
//  def isRowKey(field: HBaseSchemaField): Boolean = {
//    val cfColArray = field.fieldName.split(":", -1)
//    val cfName = cfColArray(0)
//    val colName = cfColArray(1)
//    if (cfName == "" && colName == "key") true else false
//  }
//
//  def getQueryTargetCloumns(hbaseTableFields: Array[HBaseSchemaField]): String = {
//    var str = ArrayBuffer[String]()
//    hbaseTableFields.foreach { field =>
//      if (!isRowKey(field)) {
//        str += field.fieldName
//      }
//    }
//    str.mkString(" ")
//  }
//
//  def tableSchemaFieldMapping(externalHBaseTable: Array[HBaseSchemaField], registerTable: Array[RegisteredSchemaField]): Map[HBaseSchemaField, RegisteredSchemaField] = {
//    if (externalHBaseTable.length != registerTable.length) sys.error("columns size not match in definition!")
//    val rs = externalHBaseTable.zip(registerTable)
//    rs.toMap
//  }
//
//  def extractRegisterSchema(registerTableSchema: String): Array[RegisteredSchemaField] = {
//    val fieldsStr = registerTableSchema.trim.drop(1).dropRight(1)
//    val fieldsArray = fieldsStr.split(",").map(_.trim)
//    fieldsArray.map { fildString =>
//      val splitedField = fildString.split("\\s+", -1)
//      RegisteredSchemaField(splitedField(0), splitedField(1))
//    }
//  }
//
//  def extractHBaseSchema(externalTableSchema: String): Array[HBaseSchemaField] = {
//    val fieldsStr = externalTableSchema.trim.drop(1).dropRight(1)
//    val fieldsArray = fieldsStr.split(",").map(_.trim)
//    fieldsArray.map(fildString => HBaseSchemaField(fildString, ""))
//  }
//
//  override lazy val schema: StructType = {
//    val fields = hbaseTableFields.map { field =>
//      val name = fieldsRelations.getOrElse(field, sys.error("table schema is not match the definition.")).fieldName
//      val relatedType = field.fieldType.toLowerCase match {
//        case "string" =>
//          SchemaType(StringType, nullable = false)
//        case "int" =>
//          SchemaType(IntegerType, nullable = false)
//        case "long" =>
//          SchemaType(LongType, nullable = false)
//        case e =>
//          sys.error(s"Can't support those field type of ${e}")
//      }
//      StructField(name, relatedType.dataType, relatedType.nullable)
//    }
//    StructType(fields)
//  }
//
//  override lazy val buildScan: RDD[Row] = {
//    val hbaseConf = HBaseConfiguration.create(sqlContext.sparkContext.hadoopConfiguration)
//    hbaseConf.set(TableInputFormat.INPUT_TABLE, hbaseTableName)
//    hbaseConf.set(TableInputFormat.SCAN_COLUMNS, queryColumns);
//    hbaseConf.set(TableInputFormat.SCAN_ROW_START, startRowKey);
//    hbaseConf.set(TableInputFormat.SCAN_ROW_STOP, endRowKey);
//    hbaseConf.set("hbase.zookeeper.quorum", "hadoop-senior01:2181")
//
//    val hbaseRdd = sqlContext.sparkContext.newAPIHadoopRDD(
//      hbaseConf,
//      classOf[org.apache.hadoop.hbase.mapreduce.TableInputFormat],
//      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
//      classOf[org.apache.hadoop.hbase.client.Result]
//    )
//
//
//    val rs = hbaseRdd.map(tuple => tuple._2).map(result => {
//      var values = new ArrayBuffer[Any]()
//      hbaseTableFields.foreach { field =>
//        values += Resolver.resolve(field, result)
//      }
//      Row.fromSeq(values.toSeq)
//    })
//
//    rs
//  }
//}
//
//object Resolver extends Serializable {
//
//  def resolve(hbaseField: HBaseSchemaField, result: Result): Any = {
//    val cfColArray = hbaseField.fieldName.split(":", -1)
//    val cfName = cfColArray(0)
//    val colName = cfColArray(1)
//    var fieldRs: Any = null
//    //resolve row key otherwise resolve column
//    if (cfName == "" && colName == "key") {
//      fieldRs = resolveRowKey(result, hbaseField.fieldType)
//    } else {
//      fieldRs = resolveColumn(result, cfName, colName, hbaseField.fieldType)
//    }
//    fieldRs
//  }
//
//  private def resolveBytes(bytes: Array[Byte], isFloatType: Boolean = false): Any = {
//    val length = bytes.length
//    try {
//      length match {
//        case 2 => Bytes.toShort(bytes)
//        case 4 => {
//          if (isFloatType) Bytes.toFloat(bytes)
//          else Bytes.toInt(bytes).toString
//        }
//        case 8 => {
//          if (isFloatType) Bytes.toDouble(bytes)
//          else Bytes.toLong(bytes)
//        }
//        case _ => Bytes.toString(bytes)
//      }
//    } catch {
//      case _: Exception => Bytes.toString(bytes)
//    }
//  }
//
//  private def resolveRowKey(result: Result, resultType: String): Any = {
//    val rowkey = resultType match {
//      case "string" =>
//        resolveBytes(result.getRow)
//      case "int" =>
//        resolveBytes(result.getRow)
//      case "long" =>
//        resolveBytes(result.getRow)
//      case "float" =>
//        resolveBytes(result.getRow, true)
//      case "double" =>
//        resolveBytes(result.getRow, true)
//    }
//    println(s"rowkey->${rowkey}")
//    rowkey
//  }
//
//  private def resolveColumn(result: Result, columnFamily: String, columnName: String, resultType: String): Any = {
//    val column = resultType match {
//      case "string" =>
//        resolveBytes(result.getValue(columnFamily.getBytes, columnName.getBytes))
//      case "int" =>
//        resolveBytes(result.getValue(columnFamily.getBytes, columnName.getBytes))
//      case "long" =>
//        resolveBytes(result.getValue(columnFamily.getBytes, columnName.getBytes))
//      case "float" =>
//        resolveBytes(result.getValue(columnFamily.getBytes, columnName.getBytes), true)
//      case "double" =>
//        resolveBytes(result.getValue(columnFamily.getBytes, columnName.getBytes), true)
//    }
//    column
//  }
//}