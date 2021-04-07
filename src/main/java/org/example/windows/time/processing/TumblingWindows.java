package org.example.windows.time.processing;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.pojo.WaterSensor;

import java.util.Date;

/**
 * @author vision
 * @version 1.0
 * @date 2021/4/6 20:37
 */
public class TumblingWindows {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<WaterSensor> sensorData = source.flatMap(new FlatMapFunction<String, WaterSensor>() {
            @Override
            public void flatMap(String line, Collector<WaterSensor> collector) throws Exception {
                String[] split = line.split(",");
                collector.collect(new WaterSensor(
                        split[0],
                        Long.valueOf(split[1]),
                        Integer.valueOf(split[2])));
            }
        });

        sensorData
                .keyBy(WaterSensor::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        for (WaterSensor element : elements) {
                            out.collect("key: " + key + " start: " +
                                    new Date(context.window().getStart()) + " end: " +
                                    new Date(context.window().getEnd()) + " " + element.toString());
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
