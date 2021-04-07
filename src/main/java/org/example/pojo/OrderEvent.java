package org.example.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @author vision
 * @version 1.0
 * @date 2021/4/6 20:05
 */
@Data
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class OrderEvent {
    private Long orderId;
    private String eventType;
    private String txId;
    private Long eventTime;
}
