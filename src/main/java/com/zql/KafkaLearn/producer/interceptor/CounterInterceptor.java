package com.zql.KafkaLearn.producer.interceptor;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/*
 * 在消息发送后更新成功发送消息数或失败发送消息数。
 */
public class CounterInterceptor implements ProducerInterceptor<String, String> {
    private int errorCounter = 0;
    private int successCounter = 0;

    public void configure(Map<String, ?> configs) {

    }

    // producer确保在消息被序列化以计算分区前调用该方法
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    // 该方法会在消息被应答执勤或消息发送失败时调用，并且通常都是在producer回调逻辑出发之前。
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (null == exception) {
            successCounter++;
        } else {
            errorCounter++;
        }

    }

    // 关闭interceptor，主要用于执行一些资源清理工作
    public void close() {
        // 保存结果
        System.out.println("successful sent : " + successCounter);
        System.out.println("failed sent : " + errorCounter);
    }

}
