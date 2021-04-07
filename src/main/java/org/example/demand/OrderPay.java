package org.example.demand;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.example.pojo.OrderEvent;
import org.example.pojo.PayEvent;

import java.util.HashMap;
import java.util.Map;

/**
 * @author vision
 * @version 1.0
 * @date 2021/4/6 10:54
 */
public class OrderPay {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> order = env.readTextFile("input/OrderLog.csv");
        DataStreamSource<String> pay = env.readTextFile("input/ReceiptLog.csv");

        SingleOutputStreamOperator<OrderEvent> orderStream = order.flatMap(new FlatMapFunction<String, OrderEvent>() {
            @Override
            public void flatMap(String line, Collector<OrderEvent> collector) throws Exception {
                String[] split = line.split(",");
                if ("pay".equals(split[1])) {
                    collector.collect(new OrderEvent(
                            Long.valueOf(split[0]),
                            split[1],
                            split[2],
                            Long.valueOf(split[3])
                    ));
                }
            }
        });

        SingleOutputStreamOperator<PayEvent> payStream = pay.flatMap(new FlatMapFunction<String, PayEvent>() {
            @Override
            public void flatMap(String line, Collector<PayEvent> collector) throws Exception {
                String[] split = line.split(",");
                collector.collect(new PayEvent(
                        split[0],
                        split[1],
                        Long.valueOf(split[2])
                ));
            }
        });

        ConnectedStreams<OrderEvent, PayEvent> source = orderStream.connect(payStream);

        source.keyBy(new KeySelector<OrderEvent, String>() {
            @Override
            public String getKey(OrderEvent orderEvent) throws Exception {
                return orderEvent.getTxId();
            }
        }, new KeySelector<PayEvent, String>() {
            @Override
            public String getKey(PayEvent payEvent) throws Exception {
                return payEvent.getTxId();
            }
        }).process(new CoProcessFunction<OrderEvent, PayEvent, String>() {

            final Map<String, OrderEvent> orderEventMap = new HashMap<>(16);
            final Map<String, PayEvent> payEventMap = new HashMap<>(16);

            @Override
            public void processElement1(OrderEvent orderEvent, Context ctx, Collector<String> out) throws Exception {
                if (payEventMap.containsKey(orderEvent.getTxId())) {
                    out.collect(orderEvent.getOrderId() + " 已完成支付");
                    orderEventMap.remove(orderEvent.getTxId());
                } else {
                    orderEventMap.put(orderEvent.getTxId(), orderEvent);
                }
            }

            @Override
            public void processElement2(PayEvent payEvent, Context ctx, Collector<String> out) throws Exception {
                if (orderEventMap.containsKey(payEvent.getTxId())) {
                    out.collect(orderEventMap.get(payEvent.getTxId()).getOrderId() + " 已完成支付");
                    payEventMap.remove(payEvent.getTxId());
                } else {
                    payEventMap.put(payEvent.getTxId(), payEvent);
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