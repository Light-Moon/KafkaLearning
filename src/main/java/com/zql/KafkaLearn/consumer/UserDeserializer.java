package com.zql.KafkaLearn.consumer;

import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;

import com.zql.KafkaLearn.common.User;

public class UserDeserializer {

    private ObjectMapper objectMapper;

    public void configure(Map configs, boolean isKey) {
        // 该方法实现必要资源的初始化工作
        objectMapper = new ObjectMapper();

    }

    public User deserialize(String topic, byte[] data) {
        User user = null;
        try {
            user = objectMapper.readValue(data, User.class);
        } finally {
            return user;
        }
    }

    public void close() {

    }
}
