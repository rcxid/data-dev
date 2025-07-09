package org.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class RedisFeatureTableSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("create table redis_feature_table (key string, age int, exp_v bigint, name string, score float) \n" +
                "  with ('connector'='redis-feature', 'host'='server.lan', 'port'='6379','password'='Rd.3098167254', \n" +
                "  'ttl'='1');");


        tableEnv.executeSql("insert into redis_feature_table select * from (values('test-feature0', 1, 1000, 'tom', 1.99));");
    }
}
