package com.zql.KafkaLearn.common;

import java.util.Properties;

public class ConsumerContext {

    private static Properties props = new Properties();
    {
        props.put("bootstrap.servers", "dfs1a1.ecld.com:9092,dfs1m1.ecld.com:9092,dfs1m2.ecld.com:9092");// 必须指定
        props.put("group.id", "test-group");// 必须指定
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "earliest");// 从最早的消息开始读取
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");// 必须指定
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");// 必须指定

    }

    public static Properties getProps() {
        return props;
    }

    private ConsumerContext() {
        // Do nothing
    }
}
