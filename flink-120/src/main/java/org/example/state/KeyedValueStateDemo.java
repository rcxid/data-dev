package org.example.state;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.pojo.WaterSensor;
import org.example.source.WaterSensorSourceInOrder;

public class KeyedValueStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new WaterSensorSourceInOrder(1))
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    private ValueState<Integer> lastState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("lastValue", Integer.class));
                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        Integer last = lastState.value() == null ? 0 : lastState.value();
                        lastState.update(value.getVc());
                        if (Math.abs(value.getVc() - last) >= 10) {
                            out.collect("传感器: " + value.getId() + ", last: " + last + ", current: " + value.getVc());
                        }
                    }
                }).print();

        env.execute();
    }
}
