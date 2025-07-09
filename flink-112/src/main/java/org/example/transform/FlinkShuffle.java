package org.example.transform;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author vision
 * @version 1.0
 * @date 2021/4/4 10:28
 */
public class FlinkShuffle {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8);

        source.shuffle().print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
