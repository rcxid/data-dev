package org.example.function.custom;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author rcxid
 * @version 1.0
 * @date 2021/4/15 11:14
 */
public class TableFunctionTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> stream = env.fromElements("hello,atguigu,world", "aaa,bbbbb", "");

        Table table = tEnv.fromDataStream(stream, $("line"));

        tEnv.createTemporaryView("words", table);

        tEnv.createTemporaryFunction("split", Split.class);

        tEnv.sqlQuery("" +
                " select " +
                " line, word, len " +
                " from words " +
                " join lateral table(split(line)) on true")
                .execute()
                .print();

        tEnv.sqlQuery("" +
                " select " +
                " line, word, len " +
                " from words " +
                " left join lateral table(split(line)) on true")
                .execute()
                .print();

        tEnv.sqlQuery("" +
                " select " +
                " line, new_word, new_len " +
                " from words " +
                " left join lateral table(split(line)) " +
                " as T(new_word, new_len) on true")
                .execute()
                .print();
    }
}
