package cn.bxg.zuoye.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author zhangYu
 * @program hdfsDemo
 * @description 生产者
 * @create 2020-07-31 14:08
 **/
public class OrderProducer {

    public static void main(String[] args) throws InterruptedException {
        //配置生产者信息
        Properties props = new Properties();
        props.put("retries",0); //重试次数
        props.put("batch.size",16384);
        props.put("linger.ms",1);
        props.put("buffer.memory",33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //获取生产者对象实例
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer(props);

        //发送数据
        for (int i = 0; i < 1000; i++) {
            kafkaProducer.send(new ProducerRecord<>("order","订单"+i));
            Thread.sleep(100);
        }

        //关闭对象
        kafkaProducer.close();
    }
}
