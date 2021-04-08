package org.example.watermark;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.pojo.WaterSensor;

import java.time.Duration;
import java.util.Date;

/**
 * @author vision
 * @version 1.0
 * @date 2021/4/8 10:02
 */
public class SideOutput {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        WatermarkStrategy<WaterSensor> ws = new WatermarkStrategy<WaterSensor>() {

            @Override
            public WatermarkGenerator<WaterSensor> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new CustomWatermarkGenerator(Duration.ofSeconds(3));
            }
        };
        WatermarkStrategy<WaterSensor> wss = ws.withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
            @Override
            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                return element.getTs();
            }
        });

        SingleOutputStreamOperator<String> res = env.socketTextStream("localhost", 9999)
                .flatMap(new FlatMapFunction<String, WaterSensor>() {
                    @Override
                    public void flatMap(String value, Collector<WaterSensor> out) throws Exception {
                        String[] split = value.split(",");
                        out.collect(new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2])));
                    }
                })
                .assignTimestampsAndWatermarks(wss)
                .keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(new OutputTag<WaterSensor>("late_data"){})
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        System.out.println("start: " + new Date(context.window().getStart()) + " end: " + new Date(context.window().getEnd()));
                        for (WaterSensor element : elements) {
                            out.collect(element.toString());
                        }
                    }
                });

        res.getSideOutput(new OutputTag<WaterSensor>("late_data"){}).print("late");
        res.print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
