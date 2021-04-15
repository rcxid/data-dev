package org.example.function.custom;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

/**
 * @author rcxid
 * @version 1.0
 * @date 2021/4/15 15:11
 */
public class TopN extends TableAggregateFunction<Tuple2<Integer, Integer>, TopAcc> {

    @Override
    public TopAcc createAccumulator() {
        return new TopAcc(0, 0);
    }

    public void accumulate(TopAcc acc, Integer vc) {
        if (vc > acc.getFirst()) {
            acc.setSecond(acc.getFirst());
            acc.setFirst(vc);
        } else if (vc > acc.getSecond()) {
            acc.setSecond(vc);
        }
    }

    public void emitValue(TopAcc acc, Collector<Tuple2<Integer, Integer>> out) {
        if (acc.getFirst() != Integer.MIN_VALUE) {
            out.collect(Tuple2.of(acc.getFirst(), 1));
        }
        if (acc.getSecond() != Integer.MIN_VALUE) {
            out.collect(Tuple2.of(acc.getSecond(), 2));
        }
    }
}
