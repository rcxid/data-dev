package org.example.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author vision
 */
public class BathWordCount {
    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> textFile = env.readTextFile("input/words.txt");

        FlatMapOperator<String, String> words = textFile.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                for (String word : line.split(" ")) {
                    collector.collect(word);
                }
            }
        });

        MapOperator<String, Tuple2<String, Long>> wordOne = words.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String word) throws Exception {
                return Tuple2.of(word, 1L);
            }
        });

        AggregateOperator<Tuple2<String, Long>> result = wordOne.groupBy(0).sum(1);

        try {
            result.print();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
