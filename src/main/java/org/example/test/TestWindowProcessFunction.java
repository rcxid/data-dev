package org.example.test;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;


/**
 * @author rcxid
 * @version 1.0
 * @date 2021/4/12 18:15
 * <p>
 * 热门商品分析
 */
public class TestWindowProcessFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 只设一个并行度
        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        source.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] split = value.split(",");
                out.collect(Tuple2.of(split[0], Long.valueOf(split[1])));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                        return element.f1;
                    }
                }))
                .keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {

                    private ValueState<String> key;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        key = getRuntimeContext().getState(new ValueStateDescriptor<String>("key", String.class));
                    }

                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        if (key.value() == null) {
                            key.update(s);
                        }
                        System.out.println(s + ": " + s.equals(key.value()));
                        StringBuilder builder = new StringBuilder();
                        for (Tuple2<String, Long> element : elements) {
                            builder.append(element.f0).append(": ").append(element.f1).append(",");
                        }
                        out.collect(builder.toString());
                    }
                }).print();


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
