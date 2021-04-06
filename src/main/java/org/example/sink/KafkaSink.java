package org.example.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

/**
 * @author vision
 * @version 1.0
 * @date 2021/4/4 19:54
 */
public class KafkaSink {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .socketTextStream("localhost", 9999)
                .addSink(new FlinkKafkaProducer<String>("kafka1:9092", "test", new SimpleStringSchema()));

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
