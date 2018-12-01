package com.zql.KafkaLearn.producer.interceptor;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/*
 * 在消息发送前将时间戳信息加到消息value的最前部。
 */
public class TimeStampPrependerInterceptor implements ProducerInterceptor<String, String> {

    public void configure(Map<String, ?> configs) {
        // 做一些必要的初始化工作
    }

    // producer确保在消息被序列化以计算分区前调用该方法
    @SuppressWarnings("unchecked")
    public ProducerRecord onSend(ProducerRecord record) {
        return new ProducerRecord(record.topic(), record.partition(), record.timestamp(), record.key(),
                System.currentTimeMillis() + "," + record.value().toString());
    }

    // 该方法会在消息被应答执勤或消息发送失败时调用，并且通常都是在producer回调逻辑出发之前。
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    // 关闭interceptor，主要用于执行一些资源清理工作
    public void close() {

    }
}
