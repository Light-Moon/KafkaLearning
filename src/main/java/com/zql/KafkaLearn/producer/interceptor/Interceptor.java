package com.zql.KafkaLearn.producer.interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.zql.KafkaLearn.common.ProducerContext;

public class Interceptor {

    public static void interceptorTest() throws InterruptedException, ExecutionException {
        Properties properties = ProducerContext.getProps();
        // 构建拦截器链
        List<String> interceptors = new ArrayList<String>();
        interceptors.add("com.zql.KafkaLearn.producer.TimeStampPrependerInterceptor");
        interceptors.add("com.zql.KafkaLearn.producer.CounterInterceptor");

        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);

        String topic = "serializer-topic";
        Producer<String, String> producer = new KafkaProducer<String, String>(properties);
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, "message" + i);
            producer.send(record).get();// 同步发送消息
        }
        producer.close();// 一定要关闭producer,这样才会调用interceptor的close方法。
    }
}
