package org.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class ProcessWithTimer {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketStream = env.socketTextStream("cluster1", 3100);

        socketStream.flatMap(new FlatMapFunction<String, UserLog>() {
            @Override
            public void flatMap(String input, Collector<UserLog> collector) throws Exception {
                String[] split = input.split(",", 4);
                if (split.length == 4) {
                    collector.collect(new UserLog(split[0], split[1], split[2], split[3]));
                }
            }
        }).keyBy(new KeySelector<UserLog, String>() {
            @Override
            public String getKey(UserLog userLog) throws Exception {
                return userLog.requestId + "," + userLog.itemId;
            }
        }).process(new KeyedProcessFunction<String, UserLog, UserLogAgg>() {
            @Override
            public void processElement(UserLog userLog, KeyedProcessFunction<String, UserLog, UserLogAgg>.Context context, Collector<UserLogAgg> collector) throws Exception {

            }
        })

        ;
    }
}

class UserLog {
    public String userId;
    public String itemId;
    public String requestId;
    public String eventId;

    public UserLog(String userId, String itemId, String requestId, String eventId) {
        this.userId = userId;
        this.itemId = itemId;
        this.requestId = requestId;
        this.eventId = eventId;
    }
}

class UserLogAgg {
    public String userId;
    public String itemId;
    public String requestId;
    public Boolean click;
}
