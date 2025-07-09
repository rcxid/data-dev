package org.example.cep;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.pojo.WaterSensor;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author rcxid
 * @version 1.0
 * @date 2021/4/13 18:09
 */
public class BaseUse {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        SingleOutputStreamOperator<WaterSensor> waterSensorStream = env
                .readTextFile("input/sensor.txt")
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0],
                                Long.parseLong(split[1]) * 1000,
                                Integer.parseInt(split[2]));
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((element, recordTimestamp) -> element.getTs()));


        // TODO 1、定义模式
        Pattern<WaterSensor, WaterSensor> pattern = Pattern.<WaterSensor>begin("start")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return "sensor_1".equals(value.getId());
                    }
                });

        // TODO 2、在流上应用模式
        PatternStream<WaterSensor> waterSensorPS = CEP.pattern(waterSensorStream, pattern);
        // TODO 3、获取匹配到的结果
        waterSensorPS
                .select(new PatternSelectFunction<WaterSensor, String>() {
                    @Override
                    public String select(Map<String, List<WaterSensor>> map) throws Exception {
                        return map.toString();
                    }
                }).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
