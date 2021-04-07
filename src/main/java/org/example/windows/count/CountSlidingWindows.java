package org.example.windows.count;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author vision
 * @version 1.0
 * @date 2021/4/7 18:54
 */
public class CountSlidingWindows {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String line) throws Exception {
                        return Tuple2.of(line, 1L);
                    }
                })
                .keyBy(t -> t.f0)
                .countWindow(5, 2)
                .sum(1)
                .print();


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
