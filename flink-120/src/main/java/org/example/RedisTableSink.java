package org.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class RedisTableSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("create table redis_table (name varchar, age int) \n" +
                "  with ('connector'='redis', 'host'='server.lan', 'port'='6379','password'='Rd.3098167254', \n" +
                "  'redis-mode'='single','command'='set');");


        tableEnv.executeSql("insert into redis_table select * from (values('test', 1));");
    }
}
