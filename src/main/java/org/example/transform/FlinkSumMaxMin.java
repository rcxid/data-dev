package org.example.transform;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.pojo.WaterSensor;

/**
 * @author vision
 * @version 1.0
 * @date 2021/4/4 10:44
 */
public class FlinkSumMaxMin {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<WaterSensor> source = env.fromElements(
                new WaterSensor("sensor_1", 1203456L, 10),
                new WaterSensor("sensor_1", 1234256L, 20),
                new WaterSensor("sensor_2", 1234356L, 200),
                new WaterSensor("sensor_1", 1234546L, 30),
                new WaterSensor("sensor_1", 1235456L, 10),
                new WaterSensor("sensor_2", 1237456L, 100),
                new WaterSensor("sensor_2", 1234856L, 10)
        );

        KeyedStream<WaterSensor, String> sensor = source.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor waterSensor) throws Exception {
                return waterSensor.getId();
            }
        });

//        sensor.sum("vc").print("sum");
//        sensor.max("vc").print("max");
//        sensor.min("vc").print("min");

        sensor.maxBy("vc", false).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
