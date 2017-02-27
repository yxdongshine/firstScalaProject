package com.yxd.bigdata.spark.kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * Created by Administrator on 2017/2/27 0027.
 */
public class ConsumerPartitioner implements Partitioner {

    /**
     * 默认无参构造函数
     */
    public ConsumerPartitioner() {
        this(new VerifiableProperties());
    }

    /**
     * 该构造函数必须给定
     *
     * @param properties 初始化producer的时候给定的配置信息
     */
    public ConsumerPartitioner(VerifiableProperties properties) {
        // nothings
    }
    public int partition(Object key, int numPartitions) {
        System.out.println(key + ":" + numPartitions);
        String tmp = (String) key;
        int index = tmp.lastIndexOf('_');
        int number = Integer.valueOf(tmp.substring(index + 1));
        return number % numPartitions;
    }
}
