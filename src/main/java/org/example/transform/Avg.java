package org.example.transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author vision
 * @version 1.0
 * @date 2021/4/4 11:36
 */
public class Avg {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Tuple3<String, Long, Long>> s = source.flatMap(new FlatMapFunction<String, Tuple3<String, Long, Long>>() {
            @Override
            public void flatMap(String line, Collector<Tuple3<String, Long, Long>> collector) throws Exception {
                String[] split = line.split(",");
                collector.collect(Tuple3.of(split[0], Long.valueOf(split[1]), 1L));
            }
        });

        KeyedStream<Tuple3<String, Long, Long>, String> ss = s.keyBy(new KeySelector<Tuple3<String, Long, Long>, String>() {
            @Override
            public String getKey(Tuple3<String, Long, Long> value) throws Exception {
                return value.f0;
            }
        });

        ss.process(new KeyedProcessFunction<String, Tuple3<String, Long, Long>, Tuple4<String, Long, Long, Double>>() {

            private Double sum = 0.0;
            private Long count = 0L;

            @Override
            public void processElement(Tuple3<String, Long, Long> value, Context context, Collector<Tuple4<String, Long, Long, Double>> collector) throws Exception {
                sum += value.f1;
                count += value.f2;

                collector.collect(Tuple4.of(value.f0, Math.round(sum), count, sum / count));
            }


        }).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
