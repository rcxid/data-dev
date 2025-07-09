package org.example.sql.sql.time;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.pojo.WaterSensor;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author rcxid
 * @version 1.0
 * @date 2021/4/14 18:35
 *
 * 时区存在问题
 */
public class SchemaProcTime {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> waterSensorStream =
                env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60));

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.fromDataStream(waterSensorStream,
                $("id"),
                $("ts"),
                $("vc"),
                $("pt").proctime()).execute().print();

    }
}
