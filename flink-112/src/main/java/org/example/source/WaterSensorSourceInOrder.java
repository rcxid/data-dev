package org.example.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.example.pojo.WaterSensor;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author vision
 * @version 1.0
 * @date 2021/4/7 20:51
 */
public class WaterSensorSourceInOrder implements SourceFunction<WaterSensor> {

    /**
     * 产生数据时间间隔
     */
    private final long period;
    /**
     * 停止数据源标志
     */
    private boolean flag = true;

    public WaterSensorSourceInOrder(long period) {
        this.period = period;
    }

    @Override
    public void run(SourceContext<WaterSensor> ctx) throws Exception {
        Random random = new Random();
        List<String> sensors = Arrays.asList("sensor_1", "sensor_2", "sensor_3", "sensor_4");
        List<Integer> vcs = Arrays.asList(10, 15, 20, 25, 30, 35, 40, 45);
        int sensorsSize = sensors.size();
        int vcsSize = vcs.size();
        while (flag) {
            WaterSensor data = new WaterSensor(
                    sensors.get(random.nextInt(sensorsSize)),
                    System.currentTimeMillis(),
                    vcs.get(random.nextInt(vcsSize))
            );
            ctx.collect(data);
            TimeUnit.SECONDS.sleep(period);
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}
