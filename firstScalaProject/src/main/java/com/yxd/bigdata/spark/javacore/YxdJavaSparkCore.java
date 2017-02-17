package com.yxd.bigdata.spark.javacore;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

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

        String inputPath = "/eventLogs/2016/12/21/FlumeData.1482391467667.log";
        String outHdfsPath = "/sparkJavaCore/"+System.currentTimeMillis();
        //加载文件形成RDD
        JavaRDD<String> javaDataRdd = jsc.textFile(inputPath);


        //java Rdd 数据flat收集
        JavaRDD worldRdd = javaDataRdd.flatMap(
                new FlatMapFunction<String, Object>() {
                    public Iterable<Object> call(String s) throws Exception {
                        List list = null;
                        if (null != s)
                            list = Arrays.asList(s.split("/t"));
                        return list;
                    }
                }
        );

       //java rdd中不存在reducebykey函数 通过mapToPair转换成（key,value）
        JavaPairRDD javaPairRDD = worldRdd.mapToPair(
                new PairFunction <String, String, Integer>() {

                    public Tuple2<String, Integer> call(String s) throws Exception {
                        return new Tuple2<String, Integer>(s , 1);
                    }
                }
        );

        //聚合只能在javapairRdd
        JavaPairRDD javaReduceRDD = javaPairRDD.reduceByKey(
                new Function2<Integer, Integer, Integer>(){

            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }
        );

        //保存在hdfs上
        //javaReduceRDD.saveAsTextFile(outHdfsPath);

        //保存进数据库中
        javaReduceRDD.foreachPartition(
                new VoidFunction<Iterator<Tuple2<String, Integer>>>(){

                    public void call(Iterator<Tuple2<String, Integer>> tuple2Iterator) throws Exception {

                        while (tuple2Iterator.hasNext()){
                            Tuple2<String, Integer> dataTuple = tuple2Iterator.next();
                            //设置成dataEntry
                            DataEntry dataEntry = new DataEntry();
                            dataEntry.setKey(dataTuple._1());
                            dataEntry.setValue(dataTuple._2());

                            //获取连接
                            JavaJdbc.getInstance().insert(dataEntry);
                        }
                        //最后关闭连接
                        JavaJdbc.getInstance().closeConn();
                    }
                }
        );


    }
}
