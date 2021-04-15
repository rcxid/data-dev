package org.example.sql.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.example.pojo.WaterSensor;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author rcxid
 * @version 1.0
 * @date 2021/4/13 20:17
 */
public class Aggregate {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> waterSensorStream = env.fromElements(
                new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_2", 6000L, 60)
        );

        // TODO 1、创建表执行环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // TODO 2、创建表
        Table table = tEnv.fromDataStream(waterSensorStream);

        // TODO 3、操作表
        Table select = table
                .where($("vc").isGreaterOrEqual(20))
                .groupBy($("id"))
                .aggregate($("vc").sum().as("vc_sum"))
                .select($("id"), $("vc_sum"));

        select.execute().print();

        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tEnv.toRetractStream(select, Row.class);

        tuple2DataStream.print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
