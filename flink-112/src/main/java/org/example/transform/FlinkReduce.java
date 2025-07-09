package org.example.transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.example.pojo.WaterSensor;

/**
 * @author vision
 * @version 1.0
 * @date 2021/4/4 17:03
 */
public class FlinkReduce {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        source.flatMap(new FlatMapFunction<String, WaterSensor>() {
            @Override
            public void flatMap(String line, Collector<WaterSensor> collector) throws Exception {
                String[] split = line.split(",");
                collector.collect(new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2])));
            }
        }).keyBy(WaterSensor::getId)
                .reduce(new ReduceFunction<WaterSensor>() {
                    @Override
                    public WaterSensor reduce(WaterSensor w1, WaterSensor w2) throws Exception {
                        return new WaterSensor(w1.getId(), w2.getTs(), w1.getVc() + w2.getVc());
                    }
                }).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
