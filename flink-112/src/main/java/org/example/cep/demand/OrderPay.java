package org.example.cep.demand;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.example.pojo.OrderEvent;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author rcxid
 * @version 1.0
 * @date 2021/4/13 19:55
 */
public class OrderPay {
    public static void main(String[] args) {

        String create = "create";
        String pay = "pay";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 创建WatermarkStrategy
        WatermarkStrategy<OrderEvent> wms = WatermarkStrategy
                .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                    @Override
                    public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                        return element.getEventTime();
                    }
                });
        KeyedStream<OrderEvent, Long> orderKS = env
                .readTextFile("input/OrderLog.csv")
                .map(line -> {
                    String[] data = line.split(",");
                    return new OrderEvent(
                            Long.valueOf(data[0]),
                            data[1],
                            data[2],
                            Long.parseLong(data[3]) * 1000);

                })
                .assignTimestampsAndWatermarks(wms)
                .keyBy(OrderEvent::getOrderId);

        // TODO 1
        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("create")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return value.getEventType().equals(create);
                    }
                })
                .next("pay")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return value.getEventType().equals(pay);
                    }
                })
                .within(Time.minutes(15));

        // TODO 2
        PatternStream<OrderEvent> patternStream = CEP.pattern(orderKS, pattern);
        // TODO 3

        // 正常支付
        /*patternStream.select(new PatternSelectFunction<OrderEvent, String>() {
            @Override
            public String select(Map<String, List<OrderEvent>> map) throws Exception {
                return map.toString();
            }
        }).print();*/

        SingleOutputStreamOperator<String> result = patternStream.select(new OutputTag<String>("timeOut") {
                                                                         },
                new PatternTimeoutFunction<OrderEvent, String>() {
                    @Override
                    public String timeout(Map<String, List<OrderEvent>> map, long l) throws Exception {
                        System.out.println(l);
                        return map.toString();
                    }
                },
                new PatternSelectFunction<OrderEvent, String>() {
                    @Override
                    public String select(Map<String, List<OrderEvent>> map) throws Exception {
                        return map.toString();
                    }
                });

        result.getSideOutput(new OutputTag<String>("timeOut") {}).print();


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
