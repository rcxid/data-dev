package org.example.sql.sql.time;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author rcxid
 * @version 1.0
 * @date 2021/4/14 18:39
 */
public class DdlEventTime {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 1. 创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("" +
                " create table sensor (" +
                " id string, " +
                " ts bigint, " +
                " vc int, " +
                " et as to_timestamp(from_unixtime(ts, 'yyyy-MM-dd HH:mm:ss')), " +
                " watermark for et as et -interval '5' second) " +
                " with ( " +
                " 'connector' = 'filesystem', " +
                " 'path' = 'input/sensor.txt', " +
                " 'format' = 'csv' " +
                ")");

        tableEnv.sqlQuery("" +
                " select " +
                " * " +
                " from sensor ").execute().print();

    }
}
