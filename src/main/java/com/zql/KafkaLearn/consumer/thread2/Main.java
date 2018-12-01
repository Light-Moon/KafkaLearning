package com.zql.KafkaLearn.consumer.thread2;

public class Main {

    // 参考：https://www.cnblogs.com/huxi2b/p/7089854.html
    // 1.首先创建一个测试topic：test-topic，10个分区，并使用kafka-producer-perf-test.sh脚本生产50万条消息
    // 2.运行Main，假定group.id设置为test-group
    // 3.新开一个终端，不断地运行以下脚本监控consumer group的消费进度 bin/kafka-consumer-groups.sh
    // --bootstrap-server localhost:9092 --describe --group test-group

    public static void main(String[] args) {
        String brokerList = "localhost:9092";
        String topic = "test-topic";
        String groupID = "test-group";
        final ConsumerThreadHandler<byte[], byte[]> handler = new ConsumerThreadHandler<byte[], byte[]>(brokerList, groupID, topic);
        final int cpuCount = Runtime.getRuntime().availableProcessors();

        Runnable runnable = new Runnable() {
            public void run() {
                handler.consume(cpuCount);
            }
        };
        new Thread(runnable).start();

        try {
            // 20秒后自动停止该测试程序
            Thread.sleep(20000L);
        } catch (InterruptedException e) {
            // swallow this exception
        }
        System.out.println("Starting to close the consumer...");
        handler.close();
    }
}
