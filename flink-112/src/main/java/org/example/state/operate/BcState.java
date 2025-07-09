package org.example.state.operate;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author vision
 * @version 1.0
 * @date 2021/4/8 17:00
 */
public class BcState {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
        DataStreamSource<String> control = env.socketTextStream("localhost", 8888);

        MapStateDescriptor<String, String> state = new MapStateDescriptor<>("state", String.class, String.class);

        BroadcastStream<String> broadcast = control.broadcast(state);

        source.connect(broadcast)
                .process(new BroadcastProcessFunction<String, String, String>() {
                    private BroadcastState<String, String> broadcastState;

                    @Override
                    public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        String s = null;
                        if (broadcastState != null) {
                            s = broadcastState.get("switch");
                        }
                        if ("k1".equals(s)) {
                            out.collect("k1: " + value);
                        } else {
                            out.collect("other: " + value);
                        }
                    }

                    @Override
                    public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
                         broadcastState = ctx.getBroadcastState(state);
                         broadcastState.put("switch", value);
                    }
                }).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
