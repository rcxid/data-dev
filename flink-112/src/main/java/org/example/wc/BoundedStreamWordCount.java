package org.example.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author vision
 */
public class BoundedStreamWordCount {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> file = env.readTextFile("input/words.txt");

        SingleOutputStreamOperator<Tuple2<String, Long>> wordOne = file.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Long>> collector) throws Exception {
                for (String word : line.split(" ")) {
                    collector.collect(Tuple2.of(word, 1L));
                }
            }
        });

        KeyedStream<Tuple2<String, Long>, String> keys = wordOne.keyBy(value -> value.f0);

        SingleOutputStreamOperator<Tuple2<String, Long>> result = keys.sum(1);

        result.print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
