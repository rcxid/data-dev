package org.example.function.custom;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author rcxid
 * @version 1.0
 * @date 2021/4/15 11:14
 */
public class ScalarFunctionTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // table api


        // 注册临时函数
        tableEnv.createTemporaryFunction("toUpper", ToUpperCase.class);

        tableEnv.sqlQuery("" +
                " select " +
                " toUpper('hello') ")
                .execute()
                .print();

    }
}
