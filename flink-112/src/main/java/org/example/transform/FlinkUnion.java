package org.example.transform;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author vision
 * @version 1.0
 * @date 2021/4/4 10:39
 */
public class FlinkUnion {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> source1 = env.fromElements(1, 3, 5, 7);
        DataStreamSource<Integer> source2 = env.fromElements(2, 4, 6, 8);

        DataStream<Integer> union = source1.union(source2);

        union.print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
