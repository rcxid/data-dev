package org.example.function.custom;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * @author rcxid
 * @version 1.0
 * @date 2021/4/15 11:20
 */
public class ToUpperCase extends ScalarFunction {
    public String eval(String input) {
        return input.toUpperCase();
    }
}
