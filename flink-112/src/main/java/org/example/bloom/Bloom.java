package org.example.bloom;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
//import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
//import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.shaded.guava30.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava30.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.pojo.UserBehavior;

import java.time.Duration;

/**
 * @author rcxid
 * @version 1.0
 * @date 2021/4/15 10:40
 */
public class Bloom {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        String pv = "pv";

        WatermarkStrategy<UserBehavior> wms = WatermarkStrategy
                .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                    @Override
                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                        return element.getTimestamp();
                    }
                });

        env.readTextFile("input/UserBehavior.csv")
                .flatMap(new FlatMapFunction<String, UserBehavior>() {
                    @Override
                    public void flatMap(String value, Collector<UserBehavior> out) throws Exception {
                        String[] split = value.split(",");
                        if (pv.equals(split[3])) {
                            out.collect(new UserBehavior(
                                    Long.parseLong(split[0]),
                                    Long.parseLong(split[1]),
                                    Integer.parseInt(split[2]),
                                    split[3],
                                    Long.parseLong(split[4])
                            ));
                        }
                    }
                })
                .assignTimestampsAndWatermarks(wms)
                .keyBy(UserBehavior::getBehavior)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<UserBehavior, String, String, TimeWindow>() {

                    private ValueState<Long> valueState;
                    private ValueState<BloomFilter<Long>> bloomState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        this.bloomState = getRuntimeContext().getState(new ValueStateDescriptor<>
                                ("bloomState", TypeInformation.of(new TypeHint<BloomFilter<Long>>() {
                                })));

                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("valueState", Long.class));
                    }


                    @Override
                    public void process(String key, Context context, Iterable<UserBehavior> elements, Collector<String> out) throws Exception {
                        valueState.update(0L);
                        BloomFilter<Long> longBloomFilter = BloomFilter.create(Funnels.longFunnel(), 1000000, 0.01);
                        bloomState.update(longBloomFilter);

                        for (UserBehavior element : elements) {
                            if (!bloomState.value().mightContain(element.getUserId())) {
                                valueState.update(valueState.value() + 1);
                                bloomState.value().put(element.getUserId());
                            }
                        }
                        out.collect("窗口：" + context.window() + " uv: " + valueState.value());
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
