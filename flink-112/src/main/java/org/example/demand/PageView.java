package org.example.demand;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.example.pojo.UserBehavior;

/**
 * @author vision
 * @version 1.0
 * @date 2021/4/6 19:02
 */
public class PageView {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.readTextFile("input/UserBehavior.csv");

        String pageView = "pv";
        int pageViewIndex = 3;

        source.flatMap(new FlatMapFunction<String, UserBehavior>() {
            @Override
            public void flatMap(String line, Collector<UserBehavior> collector) throws Exception {
                String[] split = line.split(",");
                if (pageView.equals(split[pageViewIndex])) {
                    collector.collect(new UserBehavior(
                            Long.valueOf(split[0]),
                            Long.valueOf(split[1]),
                            Integer.valueOf(split[2]),
                            split[3],
                            Long.valueOf(split[4])));
                }
            }
        })
                .map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(UserBehavior userBehavior) throws Exception {
                        return Tuple2.of("pv", 1L);
                    }
                })
                .keyBy(t -> t.f0)
                .sum(1)
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
