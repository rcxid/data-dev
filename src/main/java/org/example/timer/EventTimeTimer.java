package org.example.timer;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.pojo.WaterSensor;
import org.example.source.WaterSensorSourceInOrder;

/**
 * @author vision
 * @version 1.0
 * @date 2021/4/8 14:21
 */
public class EventTimeTimer {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new WaterSensorSourceInOrder(1))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                                    @Override
                                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                }))
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        if ("sensor_1".equals(value.getId())) {
                            System.out.println("创建定时器");
                            ctx.timerService().registerEventTimeTimer(ctx.timerService().currentWatermark() + 5000);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        System.out.println("定时器触发");
                    }
                }).print();


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
