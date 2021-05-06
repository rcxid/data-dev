package org.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author rcxid
 * @version 1.0
 * @date 2021/5/6 16:02
 *
 * 实时: 求最高在线和实时在线人数
 */
public class MaxOnline {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
//        ValueStateDescriptor<Long> longDesc = ;
        MapStateDescriptor<String, Object> mapDesc = new MapStateDescriptor<String, Object>("onlineState", String.class, Object.class);

        source.flatMap(new FlatMapFunction<String, LoginEvent>() {
            @Override
            public void flatMap(String value, Collector<LoginEvent> out) throws Exception {
                String[] split = value.split(",");
                if (split.length == 4) {
                    LoginEvent event = new LoginEvent();
                    event.setUserId(split[0]);
                    event.setEventType(split[1]);
                    event.setTs(Long.parseLong(split[2]));
                    event.setDt(split[3]);
                    out.collect(event);
                }
            }
        }).keyBy(LoginEvent::getDt)
                .process(new KeyedProcessFunction<String, LoginEvent, String>() {

                    private ValueState<Long> maxOnline;
                    private ValueState<Long> longValueState;
                    private MapState<String, Object> mapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        maxOnline = getRuntimeContext().getState(new ValueStateDescriptor<Long>("maxOnline", Long.class));
                        longValueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("online", Long.class));
                        mapState = getRuntimeContext().getMapState(mapDesc);
                    }

                    @Override
                    public void processElement(LoginEvent value, Context ctx, Collector<String> out) throws Exception {
                        if (longValueState.value() == null) {
                            longValueState.update(0L);
                        }
                        if (maxOnline.value() == null) {
                            maxOnline.update(0L);
                        }
                        if ("login".equals(value.getEventType())) {
                            if (!mapState.contains(value.getUserId())) {
                                longValueState.update(longValueState.value() + 1);
                                mapState.put(value.getUserId(), null);
                            }
                        } else if ("logout".equals(value.getEventType())) {
                            if (mapState.contains(value.getUserId())) {
                                longValueState.update(longValueState.value() - 1);
                                mapState.remove(value.getUserId());
                            }
                        }
                        if (longValueState.value() > maxOnline.value()) {
                            maxOnline.update(longValueState.value());
                        }
                        out.collect("当前在线人数" + longValueState.value() + " 最高在线：" + maxOnline.value());
                    }
                }).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class LoginEvent {
    private String userId;
    private String eventType;
    private long ts;
    private String dt;

    public LoginEvent() {
    }

    public LoginEvent(String userId, String eventType, long ts, String dt) {
        this.userId = userId;
        this.eventType = eventType;
        this.ts = ts;
        this.dt = dt;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public String getDt() {
        return dt;
    }

    public void setDt(String dt) {
        this.dt = dt;
    }

    @Override
    public String toString() {
        return "LoginEvent{" +
                "userId='" + userId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", ts=" + ts +
                ", dt='" + dt + '\'' +
                '}';
    }
}
