package org.example.demand;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.pojo.UserBehavior;

import java.time.Duration;

/**
 * @author vision
 * @version 1.0
 * @date 2021/4/6 19:02
 */
public class UserViewWithWindow {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.readTextFile("input/UserBehavior.csv");

        String pageView = "pv";
        int pageIndex = 3;

        source.flatMap(new FlatMapFunction<String, UserBehavior>() {
            @Override
            public void flatMap(String line, Collector<UserBehavior> collector) throws Exception {
                String[] split = line.split(",");
                if (pageView.equals(split[pageIndex])) {
                    collector.collect(new UserBehavior(
                            Long.valueOf(split[0]),
                            Long.valueOf(split[1]),
                            Integer.valueOf(split[2]),
                            split[3],
                            Long.valueOf(split[4])));
                }
            }
        })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                            @Override
                            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                return element.getTimestamp() * 1000;
                            }
                        }))
                .map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(UserBehavior userBehavior) throws Exception {
                        return Tuple2.of("uv", userBehavior.getUserId());
                    }
                }).keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(60)))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {

                    private MapState<Long, Object> mapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        mapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("mapState", Long.class, Object.class));
                    }

                    @Override
                    public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        // TODO
                        mapState.clear();
                        for (Tuple2<String, Long> element : elements) {
                            mapState.put(element.f1, null);
                        }
                        long sum = 0;
                        for (Long ignored : mapState.keys()) {
                            sum += 1;
                        }
                        out.collect(sum + "");
                    }
                }).print();


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
