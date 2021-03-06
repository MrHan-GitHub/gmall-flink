package com.atguigu.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class MyKafkaUtils {

    private static String KAFKA_SERVER = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    private static Properties properties = new Properties();

    static {
        properties.put("bootstrap.servers",KAFKA_SERVER);
    }

    /**
     * 获取KafkaSource的方法
     * @param topic 主题
     * @param groupId 消费者组
     */
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic, String groupId) {

        //创建配置信息对象，并添加配置项
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        //获取KafkaSource
        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties);
    }

    public static FlinkKafkaProducer<String>  getKafkaSink(String topic){

        return new FlinkKafkaProducer<String>(topic,new SimpleStringSchema(),properties);
    }
}
