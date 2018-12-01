package com.zql.KafkaLearn.producer;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import com.zql.KafkaLearn.common.ProducerContext;

/**
 * 需求：有一些消息用于审计功能，key中被固定分配一个字符串“zql”，让这类消息发送到topic的最后一个分区上便于后续统一处理，
 * 对于同topic下的其他消息则采用随机发送的策略发送到其他分区上。
 *
 * @author 111
 *
 */
public class ZqlPartitioner implements Partitioner {
    private Random random;

    public void configure(Map<String, ?> configs) {
        // 该方法实现必要资源的初始化工作
        random = new Random();
    }

    /**
     * 计算给定消息要被发送到哪个分区
     *
     * @param topic
     *            主题名称
     * @param keyObj
     *            消息键值或null
     * @param keyBytes
     *            消息键值序列化字节数组或null
     * @param value
     *            消息体或null
     * @param valueBytes
     *            消息体序列化字节数组或null
     * @param cluster
     *            集群元数据
     * @return
     */
    public int partition(String topic, Object keyObj, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        String key = (String) keyObj;
        List<PartitionInfo> partitionInfos = cluster.availablePartitionsForTopic(topic);// 从集群元数据中把属于该topic的所有分区信息都读取出来以供分区策略使用。
        int partitionCount = partitionInfos.size();
        int zqlPartition = partitionCount - 1;
        // 首先判断key，当它是空或是普通消息则发送到topic中除最后一个分区以外的其他分区中；若该消息属于zql消息，就固定发送到topic的最后一个分区。
        return key == null || key.isEmpty() || !key.contains("zql") ? random.nextInt(partitionCount - 1) : zqlPartition;
    }

    public void close() {
        // 该方法实现必要的资源清理工作
    }

    public static void partitionerTest() throws InterruptedException, ExecutionException {
        Properties properties = ProducerContext.getProps();
        properties.put("partitioner.class", "com.zql.KafkaLearn.producer.ZqlPartitioner");

        String topic = "partitioner-topic";
        Producer<String, String> producer = new KafkaProducer<String, String>(properties);
        ProducerRecord<String, String> nonKeyRecord = new ProducerRecord<String, String>(topic, "non-key record");
        ProducerRecord<String, String> zqlRecord = new ProducerRecord<String, String>(topic, "zql", "zql record");
        ProducerRecord<String, String> nonZqlRecord = new ProducerRecord<String, String>(topic, "other", "non-zql record");
        producer.send(nonKeyRecord).get();
        producer.send(nonZqlRecord).get();
        producer.send(zqlRecord).get();
        producer.send(nonKeyRecord).get();
        producer.send(nonZqlRecord).get();
        // 用脚本工具查询partitioner-topic每个分区的消息数
        // kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list
        // dfs1a1.ecld.com:9092,dfs1m1.ecld.com:9092,dfs1m2.ecld.com:9092
        // --topic partitioner-topic
    }
}
