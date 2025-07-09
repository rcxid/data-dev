package org.example.state.keyed;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.pojo.WaterSensor;
import org.example.source.WaterSensorSourceInOrder;

/**
 * @author rcxid
 * @version 1.0
 * @date 2021/4/9 18:57
 */
public class FlinkAggState {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new WaterSensorSourceInOrder(1))
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {


                    private AggregatingState<WaterSensor, Double> avg;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        this.avg = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<WaterSensor, Tuple2<Long, Long>, Double>("avg", new AggregateFunction<WaterSensor, Tuple2<Long, Long>, Double>() {
                            @Override
                            public Tuple2<Long, Long> createAccumulator() {
                                return Tuple2.of(0L, 0L);
                            }

                            @Override
                            public Tuple2<Long, Long> add(WaterSensor value, Tuple2<Long, Long> acc) {
                                return Tuple2.of(acc.f0 + value.getVc(), acc.f1 + 1);
                            }

                            @Override
                            public Double getResult(Tuple2<Long, Long> accumulator) {
                                return accumulator.f0 * 1.0 / accumulator.f1;
                            }

                            @Override
                            public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
                                return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
                            }
                        }, Types.TUPLE(Types.LONG, Types.LONG)));
                        AggregatingState<WaterSensor, Double> avg = this.avg;
                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        avg.add(value);
                        out.collect(value.getId() + ": " + avg.get() + "");
                    }
                }).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
