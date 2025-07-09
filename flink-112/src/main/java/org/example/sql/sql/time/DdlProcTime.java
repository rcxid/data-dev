package org.example.sql.sql.time;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author rcxid
 * @version 1.0
 * @date 2021/4/14 18:39
 */
public class DdlProcTime {
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
                " pt as PROCTIME())" +
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
