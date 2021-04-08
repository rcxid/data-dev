package org.example.watermark;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.pojo.WaterSensor;
import org.example.source.WaterSensorSourceUnOrder;

import java.time.Duration;
import java.util.Date;

/**
 * @author vision
 * @version 1.0
 * @date 2021/4/7 21:22
 */
public class EventTimeUnOrder {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        WatermarkStrategy<WaterSensor> wss = WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs();
                    }
                });

        env.addSource(new WaterSensorSourceUnOrder(1, 5))
                .map(new MapFunction<WaterSensor, WaterSensor>() {
                    @Override
                    public WaterSensor map(WaterSensor value) throws Exception {
                        System.out.println("origin: " + value + " : " + new Date(value.getTs()));
                        return value;
                    }
                })
                .assignTimestampsAndWatermarks(wss)
                .keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(4)))
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        System.out.println("start: " + new Date(context.window().getStart()) + " end: " + new Date(context.window().getEnd()));
                        for (WaterSensor element : elements) {
                            out.collect("handle: " + element + " : " + new Date(element.getTs()));
                        }
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
