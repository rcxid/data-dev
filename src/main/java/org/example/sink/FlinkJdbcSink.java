package org.example.sink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.example.pojo.Users;

/**
 * @author vision
 * @version 1.0
 * @date 2021/4/4 19:54
 */
public class FlinkJdbcSink {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .socketTextStream("localhost", 9999)
                .flatMap(new FlatMapFunction<String, Users>() {
                    @Override
                    public void flatMap(String line, Collector<Users> collector) throws Exception {
                        String[] split = line.split(",");
                        collector.collect(new Users(Integer.valueOf(split[0]), split[1], split[2]));
                    }
                })
                .addSink(JdbcSink.sink(
                        "replace into user (id, name, pwd) values (?, ?, ?)",
                        (ps, user) -> {
                            ps.setInt(1, user.getId());
                            ps.setString(2, user.getName());
                            ps.setString(3, user.getPwd());

                        },
                        new JdbcExecutionOptions.Builder()
                                .withBatchSize(1)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:mysql://localhost:3306/test?characterEncoding=utf8&serverTimezone=GMT%2B8")
                                .withDriverName("com.mysql.cj.jdbc.Driver")
                                .withUsername("root")
                                .withPassword("123456")
                                .build()
                ));


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
