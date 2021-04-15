package org.example.sql.sql.window;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author rcxid
 * @version 1.0
 * @date 2021/4/14 19:51
 */
public class SqlOverWindow {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        tEnv.executeSql("" +
                " create table sensor ( " +
                " id string, " +
                " ts bigint, " +
                " vc int, " +
                " et as to_timestamp(from_unixtime(ts, 'yyyy-MM-dd HH:mm:ss')), " +
                " watermark for et as et -interval '5' second) " +
                " with ( " +
                " 'connector' = 'filesystem', " +
                " 'path' = 'input/sensor.txt', " +
                " 'format' = 'csv' " +
                " )");

//        tEnv.sqlQuery("" +
//                " select " +
//                " id, " +
//                " ts, " +
//                " vc, " +
//                " sum(vc) over(partition by id order by et) " +
//                " from sensor ")
//                .execute()
//                .print();

//        tEnv.sqlQuery("" +
//                " select " +
//                " id, " +
//                " ts, " +
//                " vc, " +
//                " sum(vc) over(partition by id order by et rows between 1 preceding and current row) " +
//                " from sensor ")
//                .execute()
//                .print();

        tEnv.sqlQuery("" +
                " select " +
                " id, " +
                " ts, " +
                " vc, " +
                " sum(vc) over w" +
                " from sensor " +
                " window w as (partition by id order by et rows between 1 preceding and current row)")
                .execute()
                .print();
    }
}
