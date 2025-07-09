package org.example.demand;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.pojo.ApacheLog;
import org.example.pojo.PageCount;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * @author rcxid
 * @version 1.0
 * @date 2021/4/12 19:00
 */
public class HotPage {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        int topN = 10;


        env.readTextFile("input/apache.log")
                .flatMap(new FlatMapFunction<String, ApacheLog>() {
                    @Override
                    public void flatMap(String value, Collector<ApacheLog> out) throws Exception {
                        // java8新特性
                        DateTimeFormatter df = DateTimeFormatter.ofPattern("dd/MM/yyyy:HH:mm:ss");
                        String[] split = value.split(" ");
                        out.collect(new ApacheLog(
                                split[0],
                                LocalDateTime.parse(split[3], df).toEpochSecond(ZoneOffset.ofHours(8)) * 1000L,
                                split[5],
                                split[6]
                        ));
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<ApacheLog>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<ApacheLog>() {
                            @Override
                            public long extractTimestamp(ApacheLog element, long recordTimestamp) {
                                return element.getTs();
                            }
                        })
                )
                .keyBy(ApacheLog::getUrl)
                .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.seconds(5)))
                .aggregate(new AggregateFunction<ApacheLog, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(ApacheLog value, Long accumulator) {
                        return accumulator + 1;
                    }

                    @Override
                    public Long getResult(Long accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        return a + b;
                    }
                }, new ProcessWindowFunction<Long, PageCount, String, TimeWindow>() {
                    @Override
                    public void process(String value, Context context, Iterable<Long> elements, Collector<PageCount> out) throws Exception {
                        out.collect(new PageCount(value, elements.iterator().next(), context.window().getEnd()));
                    }
                })
                .keyBy(PageCount::getWindowEndTime)
                .process(new KeyedProcessFunction<Long, PageCount, String>() {

                    private ValueState<Integer> flag;
                    private ListState<PageCount> pageState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        flag = getRuntimeContext().getState(new ValueStateDescriptor<>("flag", Integer.class));
                        pageState = getRuntimeContext().getListState(new ListStateDescriptor<PageCount>("pageState", PageCount.class));
                    }

                    @Override
                    public void processElement(PageCount value, Context ctx, Collector<String> out) throws Exception {
                        if (flag.value() == null) {
                            ctx.timerService().registerEventTimeTimer(value.getWindowEndTime() + 5000L);
                            flag.update(0);
                        }
                        pageState.add(value);
                        List<PageCount> list = new ArrayList<>();
                        for (PageCount pageCount : pageState.get()) {
                            list.add(pageCount);
                        }
                        if (list.size() > topN) {
                            list.sort((o1, o2) -> o2.getCount().compareTo(o1.getCount()));
                            list.remove(topN);
                        }
                        pageState.update(list);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        List<PageCount> list = new ArrayList<>();
                        for (PageCount pageCount : pageState.get()) {
                            list.add(pageCount);
                        }
                        pageState.clear();
                        flag.clear();
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
