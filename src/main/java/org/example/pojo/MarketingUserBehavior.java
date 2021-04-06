package org.example.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @author vision
 * @version 1.0
 * @date 2021/4/6 19:29
 */
@Data
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class MarketingUserBehavior {
    private Long userId;
    private String behavior;
    private String channel;
    private Long timestamp;
}
