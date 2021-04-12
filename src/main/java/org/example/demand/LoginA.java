package org.example.demand;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.pojo.LoginEvent;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * @author rcxid
 * @version 1.0
 * @date 2021/4/12 20:01
 *
 * 恶意登录检查,存在问题
 */
@Deprecated
public class LoginA {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        int index = 2;
        String type = "fail";

        env.readTextFile("input/LoginLog.csv")
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
                .keyBy(LoginEvent::getUserId)
                .process(new KeyedProcessFunction<Long, LoginEvent, String>() {

                    private ListState<LoginEvent> loginState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        loginState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("loginState", LoginEvent.class));
                    }

                    @Override
                    public void processElement(LoginEvent value, Context ctx, Collector<String> out) throws Exception {
                        if (value.getEventType().equals(type)) {
                            loginState.add(value);
                            List<LoginEvent> list = new ArrayList<>();
                            for (LoginEvent loginEvent : loginState.get()) {
                                list.add(loginEvent);
                            }
                            if (list.size() == 2) {
                                if (list.get(1).getEventTime() - list.get(0).getEventTime() < 2000) {
                                    out.collect(value.getUserId() + "存在恶意登录");
                                    list.remove(0);
                                    loginState.update(list);
                                }
                            }
                        } else {
                            loginState.clear();
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
