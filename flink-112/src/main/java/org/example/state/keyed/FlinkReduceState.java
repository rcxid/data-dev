package org.example.state.keyed;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author rcxid
 * @version 1.0
 * @date 2021/4/9 18:57
 */
public class FlinkReduceState {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.socketTextStream("localhost", 9999)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] split = value.split(",");
                        out.collect(Tuple2.of(split[0], Integer.valueOf(split[1])));
                    }
                })
                .keyBy(v -> v.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Integer>, String>() {

                    private ReducingState<Integer> sum;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sum = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Integer>("sum", Integer::sum, Integer.class));
                    }

                    @Override
                    public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<String> out) throws Exception {
                        sum.add(value.f1);
                        out.collect(sum.get() + "");
                    }
                }).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
