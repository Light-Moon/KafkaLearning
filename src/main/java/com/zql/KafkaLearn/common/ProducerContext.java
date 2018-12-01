package com.zql.KafkaLearn.common;

import java.util.Properties;

public class ProducerContext {

    private static Properties props = new Properties();
    {
        props.put("bootstrap.servers", "dfs1a1.ecld.com:9092,dfs1m1.ecld.com:9092,dfs1m2.ecld.com:9092");// 必须指定
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");// 必须指定
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");// 必须指定
        props.put("acks", "-1");
        props.put("retries", 3);
        props.put("batch.size", 323840);
        props.put("linger.ms", 10);
        props.put("buffer.memory", 33554432);
        props.put("max.block.ms", 3000);
    }

    public static Properties getProps() {
        return props;
    }

    private ProducerContext() {
        // Do nothing
    }
}
