package org.example.state.keyed;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
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
 * @date 2021/4/9 19:25
 */
public class FlinkMapState {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new WaterSensorSourceInOrder(1))
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {

                    private MapState<Integer, Object> mapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        mapState = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, Object>("mapState", Integer.class, Object.class));
                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        mapState.put(value.getVc(), null);
                        List<Integer> list = new ArrayList<>();
                        for (Integer key : mapState.keys()) {
                            list.add(key);
                        }
                        list.sort(Comparator.reverseOrder());
                        if (list.size() > 3) {
                            list.remove(3);
                        }
                        mapState.clear();
                        for (Integer integer : list) {
                            mapState.put(integer, null);
                        }
                        out.collect(value.getId() + ": " + mapState.keys().toString());
                    }
                }).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
