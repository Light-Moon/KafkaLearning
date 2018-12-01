package com.zql.KafkaLearn.consumer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zql.KafkaLearn.common.ConsumerContext;

public class ConsumerTest {
    private static final Logger LOG = LoggerFactory.getLogger(ConsumerTest.class);

    public static void consumerUtil() {
        // 1.构造一个java.util.Properties对象，至少指定bootstrap.servers、key.deserializer、value.deserializer和group.id的值；
        Properties properties = ConsumerContext.getProps();
        String topicName = "test-topic";
        // 2、使用上一步创建的Properties实例构造KafkaConsumer对象；
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        // 3、调用KafkaConsumer.subscribe方法订阅consumer group感兴趣的topic列表；
        consumer.subscribe(Arrays.asList(topicName));// 订阅topic
        try {
            while (true) {
                // 4、循环调用KafkaConsumer.poll方法获取封装在ConsumerRecord的topic消息；
                ConsumerRecords<String, String> records = consumer.poll(1000);
                // 5、处理获取到的ConsumerRecord对象；
                for (ConsumerRecord<String, String> record : records) {
                    LOG.info("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                }
            }
        } finally {
            // 6、关闭KafkaConsumer。
            consumer.close();
        }
    }

    public static void main(String[] args) {
        System.out.println(args[0]);
        System.out.println(args[1]);
    }
}
