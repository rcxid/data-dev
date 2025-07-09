package org.example.function.custom;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.pojo.WaterSensor;

/**
 * @author rcxid
 * @version 1.0
 * @date 2021/4/15 14:44
 */
public class AggregateFunctionTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStreamSource<WaterSensor> waterSensorStream =
                env.fromElements(
                        new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60));

        Table table = tEnv.fromDataStream(waterSensorStream);

        tEnv.createTemporaryView("sensor", table);

        tEnv.createTemporaryFunction("avgF", AvgFunction.class);

        tEnv.sqlQuery("" +
                " select " +
                " id, avgF(vc) " +
                " from sensor " +
                " group by id ")
                .execute()
                .print();
    }
}
