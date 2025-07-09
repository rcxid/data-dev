package org.example.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author vision
 * @version 1.0
 * @date 2021/4/3 13:47
 */
public class FlinkSourceHdfs {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> hdfs = env.readTextFile("hdfs://hadoop1:8020/input/a.txt");

        hdfs.print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
