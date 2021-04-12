package org.example.state.keyed;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.pojo.WaterSensor;
import org.example.source.WaterSensorSourceInOrder;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @author rcxid
 * @version 1.0
 * @date 2021/4/9 18:31
 */
public class FlinkListState {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new WaterSensorSourceInOrder(1))
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {

                    private ListState<WaterSensor> waterSensor;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        waterSensor = getRuntimeContext().getListState(new ListStateDescriptor<WaterSensor>("waterSensor", WaterSensor.class));
                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        waterSensor.add(value);
                        List<WaterSensor> list = new ArrayList<>();
                        for (WaterSensor sensor : waterSensor.get()) {
                            list.add(sensor);
                        }
                        list.sort(Comparator.comparing(WaterSensor::getVc));
                        if (list.size() > 3) {
                            list.remove(3);
                        }
                        waterSensor.clear();
                        waterSensor.update(list);
                        out.collect(waterSensor.get().toString());
                    }
                }).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
