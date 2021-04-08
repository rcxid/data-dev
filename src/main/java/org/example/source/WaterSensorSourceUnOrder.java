package org.example.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.example.pojo.WaterSensor;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author vision
 * @version 1.0
 * @date 2021/4/7 20:51
 */
public class WaterSensorSourceUnOrder implements SourceFunction<WaterSensor> {

    /**
     * 产生数据时间间隔
     */
    private final long period;
    /**
     * 数据最大延迟时间
     */
    private final int delay;
    /**
     * 停止数据源标志
     */
    private boolean flag = true;

    public WaterSensorSourceUnOrder(long period, int delay) {
        this.period = period;
        this.delay = delay;
    }

    @Override
    public void run(SourceContext<WaterSensor> ctx) throws Exception {
        Random random = new Random();
        int count = 1;
        List<String> sensors = Collections.singletonList("sensor_1");
        int sensorsSize = sensors.size();
        List<Integer> vcs = Arrays.asList(10, 15, 20, 25, 30, 35, 40, 45);
        int vcsSize = vcs.size();
        while (flag) {
            long now = System.currentTimeMillis();
            if (count % 3 == 0) {
                now -= random.nextInt(delay * 1000);
                count = 0;
            }
            WaterSensor data = new WaterSensor(
                    sensors.get(random.nextInt(sensorsSize)),
                    now,
                    vcs.get(random.nextInt(vcsSize))
            );
            ctx.collect(data);
            count++;
            TimeUnit.SECONDS.sleep(period);
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}
