package org.example.windows.function;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author vision
 * @version 1.0
 * @date 2021/4/7 19:42
 */
public class WindowsAggregateFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String line) throws Exception {
                        String[] split = line.split(",");
                        return Tuple2.of(split[0], Integer.valueOf(split[1]));
                    }
                })
                .keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new AggregateFunction<Tuple2<String, Integer>, Tuple2<Long, Long>, Double>() {
                    @Override
                    public Tuple2<Long, Long> createAccumulator() {
                        return Tuple2.of(0L, 0L);
                    }

                    @Override
                    public Tuple2<Long, Long> add(Tuple2<String, Integer> value, Tuple2<Long, Long> acc) {
                        return Tuple2.of(acc.f0 + value.f1, acc.f1 + 1);
                    }

                    @Override
                    public Double getResult(Tuple2<Long, Long> acc) {
                        return acc.f0 * 1.0 / acc.f1;
                    }

                    @Override
                    public Tuple2<Long, Long> merge(Tuple2<Long, Long> a1, Tuple2<Long, Long> a2) {
                        return Tuple2.of(a1.f0 + a2.f0, a1.f1 + a2.f1);
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
