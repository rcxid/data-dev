package org.example.state.keyed;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.pojo.WaterSensor;
import org.example.source.WaterSensorSourceInOrder;

/**
 * @author vision
 * @version 1.0
 * @date 2021/4/8 17:19
 *
 * max
 */
public class FlinkValueState {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new WaterSensorSourceInOrder(1))
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    private ValueState<Integer> maxValue;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                            maxValue = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("maxValue", Integer.class));
                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        if (maxValue.value() == null) {
                            maxValue.update(0);
                        }
                        if (value.getVc() > maxValue.value()) {
                            maxValue.update(value.getVc());
                            out.collect("当前最大值" + value.getId() + ": " + value.getVc());
                        }
                    }
                }).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
