package org.example.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author vision
 * @version 1.0
 * @date 2021/4/3 16:15
 */
public class FlinkSourceKafka {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092");
        prop.setProperty("group.id", "flink_source");
        prop.setProperty("auto.offset.reset", "earliest");
        DataStreamSource<String> source = env.addSource(new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), prop));

        source.print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
