package org.example.function.custom;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @author rcxid
 * @version 1.0
 * @date 2021/4/15 14:47
 */
@Data
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class AvgAcc {
    private Long sum;
    private Integer count;
}
