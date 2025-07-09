package org.example.sink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.example.pojo.Users;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author vision
 * @version 1.0
 * @date 2021/4/4 19:54
 */
public class CustomSink {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        source
                .flatMap(new FlatMapFunction<String, Users>() {
                    @Override
                    public void flatMap(String line, Collector<Users> collector) throws Exception {
                        String[] split = line.split(",");
                        collector.collect(new Users(Integer.valueOf(split[0]), split[1], split[2]));
                    }
                })
                .addSink(new RichSinkFunction<Users>() {

                    private PreparedStatement ps;
                    private Connection conn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?characterEncoding=utf8&serverTimezone=GMT%2B8", "root", "123456");
                        ps = conn.prepareStatement("replace into user (id, name, pwd) values (?,?,?)");
                    }

                    @Override
                    public void close() throws Exception {
                        if (ps != null) {
                            ps.close();
                        }
                        if (conn != null) {
                            conn.close();
                        }
                    }

                    @Override
                    public void invoke(Users user, Context context) throws Exception {
                        ps.setInt(1, user.getId());
                        ps.setString(2, user.getName());
                        ps.setString(3, user.getPwd());
                        ps.execute();
                    }
                });

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
