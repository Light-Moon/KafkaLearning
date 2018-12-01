package com.zql.KafkaLearn.producer;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zql.KafkaLearn.common.ProducerContext;
import com.zql.KafkaLearn.common.User;

//创建自定义序列化类
public class UserSerializer implements Serializer {
    private static final Logger LOG = LoggerFactory.getLogger(UserSerializer.class);

    private ObjectMapper objectMapper;

    public void configure(Map configs, boolean isKey) {
        // 该方法实现必要资源的初始化工作
        objectMapper = new ObjectMapper();

    }

    public byte[] serialize(String topic, Object data) {
        byte[] ret = null;
        try {
            ret = objectMapper.writeValueAsString(data).getBytes("utf-8");
        } catch (Exception e) {
            LOG.info("failed to serialize the object:{}", data, e);
        }
        return ret;
    }

    public void close() {

    }

    public static void userSerializerTest() throws InterruptedException, ExecutionException {
        Properties properties = ProducerContext.getProps();
        properties.remove("value.serializer");
        properties.put("value.serializer", "com.zql.KafkaLearn.producer.UserSerializer");

        String topic = "serializer-topic";
        Producer<String, User> producer = new KafkaProducer<String, User>(properties);

        // 构建User实例
        User user = new User("zql", 26, "Guangzhou, China");
        // 构建ProducerRecord实例，注意范型格式是<String, User>
        ProducerRecord<String, User> record = new ProducerRecord<String, User>(topic, user);
        producer.send(record).get();
        producer.close();
    }
}
