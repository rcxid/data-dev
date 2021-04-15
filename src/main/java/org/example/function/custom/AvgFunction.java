package org.example.function.custom;

import org.apache.flink.table.functions.AggregateFunction;

/**
 * @author rcxid
 * @version 1.0
 * @date 2021/4/15 14:45
 */
public class AvgFunction extends AggregateFunction<Double, AvgAcc> {

    public void accumulate(AvgAcc acc, Integer vc) {
        acc.setCount(acc.getCount() + 1);
        acc.setSum(acc.getSum() + vc);
    }

    @Override
    public Double getValue(AvgAcc acc) {
        return acc.getSum() * 1.0 / acc.getCount();
    }

    @Override
    public AvgAcc createAccumulator() {
        return new AvgAcc(0L, 0);
    }
}
