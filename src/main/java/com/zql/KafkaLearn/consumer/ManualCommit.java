package com.zql.KafkaLearn.consumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.zql.KafkaLearn.common.ConsumerContext;

public class ManualCommit {

    public static void manualCommitUtil() {
        Properties properties = ConsumerContext.getProps();
        properties.remove("enable.auto.commit");
        properties.put("enable.auto.commit", "false");// 将原来的参数值设置为false
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList("test-topic"));
        final int minBatchSize = 500;
        List<ConsumerRecord<String, String>> buffer = new ArrayList<ConsumerRecord<String, String>>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                buffer.add(record);// consumer持续消费一批消息并把它们加入一个缓冲区中
            }
            if (buffer.size() >= minBatchSize) {// 当积累了足够多的消息即500条便插入到数据库总。
                // insertIntoDb(buffer);//只有被成功插入数据库中之后，这些消息才算是真正处理完
                consumer.commitSync();// 消息处理完后再调用commitSync方法进行手动位移提交
                buffer.clear();// 最后清空缓冲区以备缓存下一批消息。
            }
        }
    }

}
