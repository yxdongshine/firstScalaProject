//package com.yxd.bigdata.spark.core
//
//import org.apache.spark.sql.SQLContext
//
//import scala.collection.immutable.HashMap
//
///**
//  * Created by ibf on 02/16.
//  */
//package object hbase {
//
//  abstract class SchemaField extends Serializable
//
//  case class RegisteredSchemaField(fieldName: String, fieldType: String) extends SchemaField with Serializable
//
//  case class HBaseSchemaField(fieldName: String, fieldType: String) extends SchemaField with Serializable
//
//  case class Parameter(name: String)
//
//  protected val SPARK_SQL_TABLE_SCHEMA = Parameter("sparksql_table_schema")
//  protected val HBASE_TABLE_NAME = Parameter("hbase_table_name")
//  protected val HBASE_TABLE_SCHEMA = Parameter("hbase_table_schema")
//  protected val ROW_RANGE = Parameter("row_range")
//
//  implicit class HBaseContext(sqlContext: SQLContext) {
//    def hbaseTable(sparksqlTableSchema: String, hbaseTableName: String, hbaseTableSchema: String, rowRange: String = "->") = {
//      var params = new HashMap[String, String]
//      params += (SPARK_SQL_TABLE_SCHEMA.name -> sparksqlTableSchema)
//      params += (HBASE_TABLE_NAME.name -> hbaseTableName)
//      params += (HBASE_TABLE_SCHEMA.name -> hbaseTableSchema)
//      //get star row and end row
//      params += (ROW_RANGE.name -> rowRange)
//      sqlContext.baseRelationToDataFrame(HBaseRelation(params)(sqlContext));
//    }
//  }
//
//}
