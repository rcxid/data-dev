package org.example.transform;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @author vision
 * @version 1.0
 * @date 2021/4/4 10:33
 */
public class FlinkConnect {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> source1 = env.fromElements(1, 2, 3, 4);
        DataStreamSource<String> source2 = env.fromElements("A", "B", "C", "D", "E", "F", "G", "H");

        ConnectedStreams<Integer, String> cs = source1.connect(source2);

        cs.map(new CoMapFunction<Integer, String, String>() {
            @Override
            public String map1(Integer integer) throws Exception {
                return integer + "-A";
            }

            @Override
            public String map2(String s) throws Exception {
                return s + " B";
            }
        }).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
