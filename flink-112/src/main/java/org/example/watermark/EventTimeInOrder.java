package org.example.watermark;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.pojo.WaterSensor;
import org.example.source.WaterSensorSourceInOrder;

/**
 * @author vision
 * @version 1.0
 * @date 2021/4/7 20:59
 */
public class EventTimeInOrder {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        WatermarkStrategy<WaterSensor> wss = WatermarkStrategy
                .<WaterSensor>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs();
                    }
                });

        env.addSource(new WaterSensorSourceInOrder(1))
                .assignTimestampsAndWatermarks(wss)
                .keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        long sum = 0L;
                        int count = 0;
                        for (WaterSensor element : elements) {
                            count++;
                            sum += element.getVc();
                        }
                        double avg = sum * 1.0 / count;
                        out.collect("key: " + key + " avg: " + avg);
                    }
                })
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
