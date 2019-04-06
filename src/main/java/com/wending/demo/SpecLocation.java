package com.wending.demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * Created by shuiyu lei
 * date 2019/4/6
 * 从指定位置消费
 */
public class SpecLocation extends Thread {


    KafkaConsumer<Integer, String> consumer;

    public SpecLocation() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "centos-6:9092");  //连接地址
        props.put("group.id", "test");
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        this.consumer = new KafkaConsumer<Integer, String>(props);
    }


    @Override
    public void run() {
        TopicPartition partition = new TopicPartition("order", 0);
        consumer.assign(Arrays.asList(partition));
        //从指定位置开始消费
        consumer.seek(partition, 5310);
        while (true) {
            ConsumerRecords<Integer, String> poll = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<Integer, String> record : poll) {
                System.out.println(record.key() + "-------" + record.value());
            }
        }
    }

    public static void main(String[] args) {
        SpecLocation location = new SpecLocation();
        location.start();
    }
}
