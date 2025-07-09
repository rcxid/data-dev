package org.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountSocket {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketStream = env.socketTextStream("192.168.8.200", 9999);

        socketStream
                .flatMap(
                        new FlatMapFunction<String, Tuple2<String, Integer>>() {
                            @Override
                            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                                for (String word : value.split(" ")) {
                                    out.collect(Tuple2.of(word, 1));
                                }
                            }
                        })
                .keyBy(x -> x.f0)
                .sum(1)
                .print();

        env.execute();
    }
}
