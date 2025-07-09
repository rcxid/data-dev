package org.example.demand;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.example.pojo.AdsClickLog;

/**
 * @author vision
 * @version 1.0
 * @date 2021/4/6 19:56
 */
public class AdsClick {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.readTextFile("input/AdClickLog.csv");
        source.flatMap(new FlatMapFunction<String, AdsClickLog>() {
            @Override
            public void flatMap(String line, Collector<AdsClickLog> collector) throws Exception {
                String[] split = line.split(",");
                collector.collect(new AdsClickLog(
                        Long.valueOf(split[0]),
                        Long.valueOf(split[1]),
                        split[2],
                        split[3],
                        Long.valueOf(split[4])
                ));
            }
        }).map(new MapFunction<AdsClickLog, Tuple2<Tuple2<String, Long>, Long>>() {
            @Override
            public Tuple2<Tuple2<String, Long>, Long> map(AdsClickLog adsClickLog) throws Exception {
                return Tuple2.of(Tuple2.of(adsClickLog.getProvince(), adsClickLog.getAdId()), 1L);
            }
        }).keyBy(new KeySelector<Tuple2<Tuple2<String, Long>, Long>, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> getKey(Tuple2<Tuple2<String, Long>, Long> tuple2LongTuple2) throws Exception {
                return tuple2LongTuple2.f0;
            }
        }).sum(1).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
