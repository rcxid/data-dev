package org.example.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author rcxid
 * @version 1.0
 * @date 2021/4/14 20:53
 */
public class TopN {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("" +
                " create table user_behavior( " +
                " user_id bigint, " +
                " item_id bigint, " +
                " category_id int, " +
                " behavior string, " +
                " ts bigint, " +
                " et as to_timestamp(from_unixtime(ts, 'yyyy-MM-dd HH:mm:ss')), " +
                " watermark for et as et - interval '5' second) " +
                " with (" +
                " 'connector' = 'filesystem', " +
                " 'path' = 'input/UserBehavior.csv', " +
                " 'format' = 'csv')");

        Table t1 = tableEnv.sqlQuery("" +
                " select " +
                " item_id, " +
                " count(*) item_count, " +
                " hop_end(et, interval '10' minute, interval '1' hour) as w_end " +
                " from user_behavior " +
                " where behavior = 'pv' " +
                " group by item_id, hop(et, interval '10' minute, interval '1' hour) ");

        tableEnv.createTemporaryView("t1", t1);

        Table t2 = tableEnv.sqlQuery("" +
                " select " +
                " item_id, " +
                " item_count, " +
                " w_end, " +
                " row_number() over(partition by w_end order by item_count desc) rk " +
                " from t1 ");

//        t2.execute().print();

        tableEnv.createTemporaryView("t2", t2);


        Table t3 = tableEnv.sqlQuery("" +
                " select " +
                " item_id, " +
                " item_count, " +
                " w_end, " +
                " rk " +
                " from t2 " +
                " where rk < 4");

        tableEnv.createTemporaryView("t3", t3);


        tableEnv.executeSql("" +
                " create table hot_item ( " +
                " item_id bigint, " +
                " item_count bigint, " +
                " rk bigint, " +
                " w_end timestamp(3), " +
                " primary key (w_end, rk) not enforced) " +
                " with ( " +
                " 'connector' = 'jdbc', " +
                " 'url' = 'jdbc:mysql://localhost:3306/test?characterEncoding=utf8&serverTimezone=GMT%2B8', " +
                " 'table-name' = 'hot_item', " +
                " 'driver' = 'com.mysql.cj.jdbc.Driver', " +
                " 'username' = 'root', " +
                " 'password' = '123456' " +
                " ) ");

        tableEnv.executeSql("" +
                " insert into hot_item " +
                " select " +
                " item_id, " +
                " item_count, " +
                " rk, " +
                " w_end " +
                " from t3 ");

    }
}
