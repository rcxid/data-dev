package org.example.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @author vision
 * @version 1.0
 * @date 2021/4/3 16:08
 */
public class FlinkSourceCollection {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<String> list = new ArrayList<>();
        list.add("A");
        list.add("B");
        list.add("C");

        env.fromCollection(list).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
