package org.example.timer;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.pojo.WaterSensor;

/**
 * @author vision
 * @version 1.0
 * @date 2021/4/8 14:56
 */
public class WaterSensorWarning {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream("localhost", 9999)
                .flatMap(new FlatMapFunction<String, WaterSensor>() {
                    @Override
                    public void flatMap(String value, Collector<WaterSensor> out) throws Exception {
                        String[] split = value.split(",");
                        out.collect(new WaterSensor(
                                split[0],
                                Long.valueOf(split[1]),
                                Integer.valueOf(split[2])
                        ));
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.
                        <WaterSensor>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                            @Override
                            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                return element.getTs();
                            }
                        }))
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    boolean timerExist = false;
                    int lastVc;
                    long timerTimestamp;
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        if (!timerExist) {
                            System.out.println("定时器不存在，创建定时器");
                            timerExist = true;
                            timerTimestamp = ctx.timestamp() + 3000;
                            ctx.timerService().registerEventTimeTimer(timerTimestamp);
                        } else {
                            // 水位未上升，重新创建定时器
                            if (lastVc >= value.getVc()) {
                                System.out.println("水位未上升，重新创建定时器");
                                ctx.timerService().deleteEventTimeTimer(timerTimestamp);
                                timerTimestamp = ctx.timestamp() + 3000;
                                ctx.timerService().registerEventTimeTimer(timerTimestamp);
                            }
                        }
                        lastVc = value.getVc();
                        out.collect(value.toString());
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        timerExist = false;
                        System.out.println("水位连续3秒上升");
                    }
                }).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
