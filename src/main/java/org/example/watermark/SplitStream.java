package org.example.watermark;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.pojo.WaterSensor;
import org.example.source.WaterSensorSourceInOrder;

/**
 * @author vision
 * @version 1.0
 * @date 2021/4/7 20:59
 */
public class SplitStream {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        OutputTag<String> sensor1 = new OutputTag<String>("sensor_1") {
        };
        OutputTag<String> sensor2 = new OutputTag<String>("sensor_2") {
        };
        OutputTag<String> sensor3 = new OutputTag<String>("sensor_3") {
        };
        OutputTag<String> sensor4 = new OutputTag<String>("sensor_4") {
        };

        SingleOutputStreamOperator<String> res = env.addSource(new WaterSensorSourceInOrder(1))
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        if ("sensor_1".equals(value.getId())) {
                            ctx.output(sensor1, value.toString());
                        } else if ("sensor_2".equals(value.getId())) {
                            ctx.output(sensor2, value.toString());
                        } else if ("sensor_3".equals(value.getId())) {
                            ctx.output(sensor3, value.toString());
                        } else {
                            ctx.output(sensor4, value.toString());
                        }
                    }
                });

        res.getSideOutput(sensor1).print("sensor_1");
//        res.getSideOutput(sensor2).print("sensor_2");
//        res.getSideOutput(sensor3).print("sensor_3");
//        res.getSideOutput(sensor4).print("sensor_4");


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
