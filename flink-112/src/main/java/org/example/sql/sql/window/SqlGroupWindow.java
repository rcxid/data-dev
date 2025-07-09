package org.example.sql.sql.window;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author rcxid
 * @version 1.0
 * @date 2021/4/14 19:51
 */
public class SqlGroupWindow {
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
//                " tumble_start(et, interval '5' second) as w_start, " +
//                " tumble_end(et, interval '5' second) as w_end, " +
//                " sum(vc) as sum_vc " +
//                " from sensor " +
//                " group by id, tumble(et, interval '5' second)")
//                .execute()
//                .print();

//        tEnv.sqlQuery("" +
//                " select " +
//                " id, " +
//                " hop_start(et, interval '2' second, interval '5' second) as w_start, " +
//                " hop_end(et, interval '2' second, interval '5' second) as w_end, " +
//                " sum(vc) as sum_vc " +
//                " from sensor " +
//                " group by id, hop(et, interval '2' second, interval '5' second)")
//                .execute()
//                .print();

        tEnv.sqlQuery("" +
                " select " +
                " id, " +
                " session_start(et, interval '2' second) as w_start, " +
                " session_end(et, interval '2' second) as w_end, " +
                " sum(vc) as sum_vc " +
                " from sensor " +
                " group by id, session(et, interval '2' second)")
                .execute()
                .print();
    }
}
