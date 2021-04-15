package org.example.sql.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author rcxid
 * @version 1.0
 * @date 2021/4/14 8:38
 */
public class KafkaToKafka {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // source
        tEnv.executeSql("" +
                " create table source_sensor ( " +
                " id string, " +
                " ts bigint, " +
                " vc int) " +
                " with (" +
                " 'connector' = 'kafka', " +
                " 'topic' = 'test', " +
                " 'properties.bootstrap.servers' = 'kafka1:9092,kafka2:9092,kafka:9093', " +
                " 'properties.group.id' = 'flink_kafka_source', " +
                " 'scan.startup.mode' = 'latest-offset', " +
                " 'format' = 'json')");

        // sink
        tEnv.executeSql("" +
                " create table sink_sensor ( " +
                " id string, " +
                " ts bigint, " +
                " vc int) " +
                " with (" +
                " 'connector' = 'kafka', " +
                " 'topic' = 'sensor', " +
                " 'properties.bootstrap.servers' = 'kafka1:9092,kafka2:9092,kafka:9093', " +
                " 'format' = 'json')");

        // op
        tEnv.executeSql("" +
                " insert into sink_sensor " +
                " select id, ts, vc " +
                " from source_sensor " +
                " where id = 'sensor_1' ");
    }
}
