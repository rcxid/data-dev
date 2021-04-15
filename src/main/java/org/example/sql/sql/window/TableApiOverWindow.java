package org.example.sql.sql.window;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Session;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.pojo.WaterSensor;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.*;

/**
 * @author rcxid
 * @version 1.0
 * @date 2021/4/14 19:11
 */
public class TableApiOverWindow {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> waterSensorStream = env
                .fromElements(
                        new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((element, recordTimestamp) -> element.getTs())
                );

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Table table = tableEnv.fromDataStream(waterSensorStream,
                $("id"),
                $("ts").rowtime(),
                $("vc"));

//        table.window(Tumble.over(lit(10).second()).on($("ts")).as("w"))
//                .groupBy($("id"), $("w"))
//                .select($("id"), $("w").start(), $("w").end(), $("vc").sum())
//                .execute()
//                .print();

//        table.window(Slide.over(lit(10).second()).every(lit(5).second()).on($("ts")).as("w"))
//                .groupBy($("id"), $("w"))
//                .select($("id"), $("w").start(), $("w").end(), $("vc").sum())
//                .execute()
//                .print();

        table.window(Over.partitionBy($("id")).orderBy($("ts")).preceding(UNBOUNDED_ROW).as("w"))
                .select($("id"), $("ts"), $("vc").sum().over($("w")).as("sum_vc"))
                .execute()
                .print();

        /*
        // 当事件时间向前算3s得到一个窗口
.window(Over.partitionBy($("id")).orderBy($("ts")).preceding(lit(3).second()).as("w"))
// 当行向前推算2行算一个窗口
.window(Over.partitionBy($("id")).orderBy($("ts")).preceding(rowInterval(2L)).as("w"))
         */

    }
}
