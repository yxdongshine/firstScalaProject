package com.yxd.bigdata.spark.kafka.consumer.high;

/**
 * Created by 20160905 on 2017/3/30.
 */
import com.yxd.bigdata.spark.kafka.FileStream.FileIo;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;

public class ConsumerTest implements Runnable {
    private KafkaStream m_stream;
    private int m_threadNumber;
    FileWriter fw = null ;
    FileIo fileIo = null ;
    public ConsumerTest(KafkaStream a_stream, int a_threadNumber) {
        m_threadNumber = a_threadNumber;
        m_stream = a_stream;

        try {
            fw =  new FileWriter("highConsumer.txt");
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            System.out.println(e.getMessage());
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        fileIo = new FileIo();

    }

    public void run() {
        ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
        while (it.hasNext()){
            String value = new String(it.next().message());
            fileIo.writeFile(value + "", fw);
            System.out.println("Thread " + m_threadNumber + ": " + value);
        }
        System.out.println("Shutting down Thread: " + m_threadNumber);
    }
}
