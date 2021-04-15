package org.example.function.custom;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @author rcxid
 * @version 1.0
 * @date 2021/4/15 14:29
 */
@FunctionHint(output = @DataTypeHint("ROW(word string, len int)"))
public class Split extends TableFunction<Row> {
    public void eval(String line) {
        if (line.length() == 0) {
            return;
        }
        String splitKey = ",";
        for (String word : line.split(splitKey)) {
            collect(Row.of(word, word.length()));
        }
    }
}
