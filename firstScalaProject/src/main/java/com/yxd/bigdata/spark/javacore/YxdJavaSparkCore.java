package com.yxd.bigdata.spark.javacore;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by Administrator on 2017/2/16 0016.
 */
public class YxdJavaSparkCore {
    public static void main(String[] args) {

      //基本结构
        SparkConf sparkConf =  new SparkConf()
                .setMaster("local[*]")
                .setAppName("YxdJavaSparkCore");

        //sparkContent
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        String inputPath = "";
        String outHdfsPath = "";
        //加载文件形成RDD
        JavaRDD<String> javaDataRdd = jsc.textFile("");


    }
}
