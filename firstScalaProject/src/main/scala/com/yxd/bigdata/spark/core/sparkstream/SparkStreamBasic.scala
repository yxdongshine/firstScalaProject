package com.yxd.bigdata.spark.core.sparkstream

/**
 * Created by 20160905 on 2017/3/1.
 */
object SparkStreamBasic {

  import org.apache.spark._
  import org.apache.spark.streaming._
  import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3

  def main(args: Array[String]) {
      // Create a local StreamingContext with two working thread and batch interval of 1 second.
      // The master requires 2 cores to prevent from a starvation scenario.

      val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
      val ssc = new StreamingContext(conf, Seconds(1))

      // Create a DStream that will connect to hostname:port, like localhost:9999
      val lines = ssc.socketTextStream("hadoop1", 9999)

      // Split each line into words
      val words = lines.flatMap(_.split(" "))

      import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3
      // Count each word in each batch
      val pairs = words.map(word => (word, 1))
      val wordCounts = pairs.reduceByKey(_ + _)

      // Print the first ten elements of each RDD generated in this DStream to the console
      wordCounts.print()

      ssc.start()             // Start the computation
      ssc.awaitTermination()  // Wait for the computation to terminate
  }



}
