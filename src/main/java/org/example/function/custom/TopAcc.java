package org.example.function.custom;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @author rcxid
 * @version 1.0
 * @date 2021/4/15 15:12
 */
@Data
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class TopAcc {
    private Integer first = Integer.MIN_VALUE;
    private Integer second = Integer.MIN_VALUE;
}
