package org.example.demand;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.pojo.HotItem;
import org.example.pojo.UserBehavior;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * @author rcxid
 * @version 1.0
 * @date 2021/4/12 18:15
 * <p>
 * 热门商品分析
 */
public class PopularProductsAnalysis {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.readTextFile("input/UserBehavior.csv");

        String pageView = "pv";
        int pageIndex = 3;
        int topN = 3;

        source.flatMap(new FlatMapFunction<String, UserBehavior>() {
            @Override
            public void flatMap(String line, Collector<UserBehavior> collector) throws Exception {
                String[] split = line.split(",");
                if (pageView.equals(split[pageIndex])) {
                    collector.collect(new UserBehavior(
                            Long.valueOf(split[0]),
                            Long.valueOf(split[1]),
                            Integer.valueOf(split[2]),
                            split[3],
                            Long.valueOf(split[4])));
                }
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp() * 1000))
                .map(new MapFunction<UserBehavior, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> map(UserBehavior value) throws Exception {
                        return Tuple2.of(value.getItemId(), 1L);
                    }
                })
                .keyBy(t -> t.f0)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(new AggregateFunction<Tuple2<Long, Long>, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(Tuple2<Long, Long> value, Long accumulator) {
                        return accumulator + value.f1;
                    }

                    @Override
                    public Long getResult(Long accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        return a + b;
                    }
                }, new ProcessWindowFunction<Long, HotItem, Long, TimeWindow>() {
                    @Override
                    public void process(Long key, Context context, Iterable<Long> elements, Collector<HotItem> out) throws Exception {
                        out.collect(new HotItem(key, elements.iterator().next(), context.window().getEnd()));
                    }
                })
                .keyBy(HotItem::getWindowEndTime)
                .process(new KeyedProcessFunction<Long, HotItem, String>() {

                    private ListState<HotItem> productState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        productState = getRuntimeContext().getListState(new ListStateDescriptor<HotItem>("productState", HotItem.class));
                    }

                    @Override
                    public void processElement(HotItem value, Context ctx, Collector<String> out) throws Exception {
                        if (!productState.get().iterator().hasNext()) {
                            ctx.timerService().registerEventTimeTimer(value.getWindowEndTime() + 5000);
                        }
                        productState.add(value);
                        List<HotItem> list = new ArrayList<>();
                        for (HotItem hotItem : productState.get()) {
                            list.add(hotItem);
                        }
                        if (list.size() > topN) {
                            list.sort((o1, o2) -> o2.getCount().compareTo(o1.getCount()));
                            list.remove(topN);
                        }
                        productState.update(list);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        List<HotItem> list = new ArrayList<>();
                        for (HotItem hotItem : productState.get()) {
                            list.add(hotItem);
                        }
                        productState.clear();
                        out.collect(list.toString());
                    }
                }).print();



        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
