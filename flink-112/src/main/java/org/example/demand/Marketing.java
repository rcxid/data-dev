package org.example.demand;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.example.pojo.MarketingUserBehavior;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author vision
 * @version 1.0
 * @date 2021/4/6 19:31
 */
public class Marketing {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<MarketingUserBehavior> source = env.addSource(new MarketingSource());

        source.map(new MapFunction<MarketingUserBehavior, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(MarketingUserBehavior ub) throws Exception {
                return Tuple2.of(ub.getChannel() + "_" + ub.getBehavior(), 1L);
            }
        }).keyBy(t -> t.f0).sum(1).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class MarketingSource implements SourceFunction<MarketingUserBehavior> {

    private boolean flag = true;

    @Override
    public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
        Random random = new Random();
        List<String> channel = Arrays.asList("xiaomi", "huawei", "oppo", "vivo", "appstore");
        List<String> behavior = Arrays.asList("install", "update", "uninstall");
        while (flag) {
            MarketingUserBehavior userBehavior = new MarketingUserBehavior((long) random.nextInt(10000),
                    behavior.get(random.nextInt(behavior.size())),
                    channel.get(random.nextInt(channel.size())),
                    System.currentTimeMillis());
            TimeUnit.SECONDS.sleep(1);
            ctx.collect(userBehavior);
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}
