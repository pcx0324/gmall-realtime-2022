package com.atguigu.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;

public class MyKafkaUtil {


    private static Properties properties = new Properties();
    private static final String BOOTSTRAP_SERVERS = "192.168.10.100:9092";

    static {
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    }

    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic,String groupId) {

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(
                topic,
                new KafkaDeserializationSchema<String>() {
                    @Override
                    public boolean isEndOfStream(String s) {
                        return false;
                    }

                    @Override
                    public String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
                        if (consumerRecord != null && consumerRecord.value() != null){
                            return new String(consumerRecord.value());
                        }
                        return null;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                },
                properties
        );
        return consumer;
    }

    public static FlinkKafkaProducer<String> getKafkaProducer(String topic){
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<String>(
                topic,
                new SimpleStringSchema(),
                properties
        );
        return producer;
    }

    public static String getTopicDbDDL(String groupId){

        return "CREATE TABLE topic_db (" +
                " `database` String, " +
                " `table`  String, " +
                " `type` String, " +
                " `data` Map<String,String>, " +
                " `old` Map<String,String>, " +
                " `pt` As PROCTIME() " +
                ")" + MyKafkaUtil.getKafkaDDL("topic_db",groupId);
    }

    public static String getKafkaDDL(String topic,String groupId){
        return " with ('connector' = 'kafka', " +
                " 'topic' = '" + topic + "'," +
                " 'properties.bootstrap.servers' = '" + BOOTSTRAP_SERVERS + "', " +
                " 'properties.group.id' = '" + groupId + "', " +
                " 'format' = 'json', " +
                " 'scan.startup.mode' = 'latest-offset')";
    }

    public static String getUpsertKafkaDDL(String topic){
        return "WITH ( " +
                "  'connector' = 'upsert-kafka', " +
                "  'topic' = '" + topic + "', " +
                "  'properties.bootstrap.servers' = '" + BOOTSTRAP_SERVERS + "', " +
                "  'key.format' = 'json', " +
                "  'value.format' = 'json' " +
                ")";
    }
}
