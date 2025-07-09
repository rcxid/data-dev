package org.example.udaf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.AggregateFunction;

import java.util.HashMap;
import java.util.Map;


@FunctionHint(input = @DataTypeHint("STRING"), output = @DataTypeHint("ARRAY<STRING>"))
public class CollectSet extends AggregateFunction<String[], CollectSet.Accumulator> {
    public static class Accumulator {
        @DataTypeHint("MULTISET<STRING>")
        public Map<String, Integer> map;
    }

    @Override
    @DataTypeHint("ARRAY<STRING>")
    public String[] getValue(Accumulator accumulator) {
        return accumulator.map.keySet().toArray(new String[0]);
    }

    @Override
    public Accumulator createAccumulator() {
        Accumulator acc = new Accumulator();
        acc.map = new HashMap<>();
        return acc;
    }

    @Override
    public boolean isDeterministic() {
        return false;
    }

    public void accumulate(Accumulator accumulator, String value) {
        accumulator.map.put(value, 1);
    }

    public void retract(Accumulator accumulator, String value) {
        accumulator.map.remove(value);
    }

    public void merge(Accumulator accumulator, Iterable<Accumulator> it) {
        for (Accumulator acc : it) {
            accumulator.map.putAll(acc.map);
        }
    }

    public void resetAccumulator(Accumulator accumulator) {
        accumulator.map.clear();
    }
}
