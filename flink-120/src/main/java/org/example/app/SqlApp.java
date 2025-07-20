package org.example.app;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class SqlApp {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql("create catalog paimon_hdfs with (\n" +
                "  'type' = 'paimon',\n" +
                "  'warehouse' = 'hdfs://cluster1:8020/data/paimon/fs'\n" +
                ");");
        tEnv.executeSql("show catalogs;").print();
        tEnv.executeSql("use catalog paimon_hdfs;");
        tEnv.executeSql("show databases;").print();
        tEnv.executeSql("show tables;").print();
    }
}
