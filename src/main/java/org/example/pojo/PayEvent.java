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
public class PayEvent {
    private String txId;
    private String payChannel;
    private Long eventTime;
}
