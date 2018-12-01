package com.zql.KafkaLearn.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.zql.KafkaLearn.common.ProducerContext;

public class ProducerTest {

    public static void producerUtil() {
        // 1.构造一个java.util.Properties对象，至少指定bootstrap.servers、key.deserializer、value.deserializer的值；
        Properties props = ProducerContext.getProps();

        String topicName = "test-topic";
        // 2.使用上一步中创建的Properties实例构造KafkaProducer对象。
        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        /*
         * 创建producer的时候同时指定key和value的序列化类，则不需在Properties中指定了。
         */
        // Serializer<String> keySerializer = new StringSerializer();
        // Serializer<String> valueSerializer = new StringSerializer();
        // Producer<String, String> producer = new KafkaProducer<String,
        // String>(props, keySerializer, valueSerializer);

        for (int i = 0; i < 100; i++) {
            // 3.构造待发送的消息对象ProducerRecord，指定消息要被发送到的topic、分区及对应的key和value。注意，分区和key信息可以不用指定，有kafka自行确定分区。
            // 4.调用KafkaProducer的send方法发送消息。
            producer.send(new ProducerRecord<String, String>("zql-topic", Integer.toString(i), Integer.toString(i)));
        }
        // 5.关闭KafkaProducer。
        producer.close();
    }

}
