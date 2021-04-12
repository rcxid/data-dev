package org.example.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @author rcxid
 * @version 1.0
 * @date 2021/4/12 19:05
 */
@Data
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class PageCount {
    private String url;
    private Long count;
    private Long windowEndTime;
}
