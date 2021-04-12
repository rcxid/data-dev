package org.example.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @author rcxid
 * @version 1.0
 * @date 2021/4/12 18:16
 */
@Data
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class HotItem {
    private Long itemId;
    private Long count;
    private Long windowEndTime;
}
