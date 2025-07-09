package org.example.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author rcxid
 * @version 1.0
 * @date 2021/4/9 18:11
 */
public class RandomNumSource implements SourceFunction<Integer> {

    private boolean cancel;
    private final Integer maxValue;

    public RandomNumSource(Integer maxValue) {
        this.maxValue = maxValue + 1;
    }

    @Override
    public void run(SourceContext<Integer> ctx) throws Exception {
        Random random = new Random();
        while (!cancel) {
            ctx.collect(random.nextInt(maxValue));
            TimeUnit.SECONDS.sleep(1);
        }
    }

    @Override
    public void cancel() {
        cancel = true;
    }
}
