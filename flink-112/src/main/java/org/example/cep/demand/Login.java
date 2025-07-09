package org.example.cep.demand;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.example.pojo.LoginEvent;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author rcxid
 * @version 1.0
 * @date 2021/4/13 19:45
 *
 * cep检测恶意登录
 */
public class Login {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String fail = "fail";
        String success = "success";

        KeyedStream<LoginEvent, Long> loginEventStream = env.readTextFile("input/LoginLog.csv")
                .flatMap(new FlatMapFunction<String, LoginEvent>() {
                    @Override
                    public void flatMap(String value, Collector<LoginEvent> out) throws Exception {
                        String[] split = value.split(",");
//                        if (split[index].equals(type)) {
                        out.collect(new LoginEvent(
                                Long.valueOf(split[0]),
                                split[1],
                                split[2],
                                Long.parseLong(split[3]) * 1000
                        ));
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                            @Override
                            public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                                return element.getEventTime();
                            }
                        }))
                .keyBy(LoginEvent::getUserId);

        // TODO 1、定义模式
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("start")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.getEventType().equals(fail);
                    }
                })
                .timesOrMore(2).consecutive()
                .until(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.getEventType().equals(success);
                    }
                })
                .within(Time.seconds(2));

        // TODO 2、应用模式
        PatternStream<LoginEvent> patternStream = CEP.pattern(loginEventStream, pattern);

        // TODO 3、获取结果
        patternStream.select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> map) throws Exception {
                return map.get("start").toString();
            }
        }).print();


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
