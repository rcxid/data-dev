package org.example.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @author vision
 * @version 1.0
 * @date 2021/4/4 14:07
 */
@Data
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class WaterSensor {
    private String name;
    private Long ts;
    private Integer vc;
}
