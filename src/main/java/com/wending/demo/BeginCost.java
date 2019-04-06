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
 * kafka从0开始消费
 */
public class BeginCost extends Thread {


    KafkaConsumer<Integer, String> consumer;

    public BeginCost() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "centos-6:9092");  //连接地址
        props.put("group.id", "test1");
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
        consumer.seekToBeginning(Arrays.asList(partition));
        consumer.seekToEnd(Arrays.asList(partition));
        while (true) {
            ConsumerRecords<Integer, String> poll = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<Integer, String> record : poll) {
                System.out.println(record.key() + "-------" + record.value());
            }
        }
    }

    public static void main(String[] args) {
        BeginCost cost = new BeginCost();
        cost.start();
    }
}
