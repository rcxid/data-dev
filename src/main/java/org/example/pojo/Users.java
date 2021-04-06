package org.example.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @author vision
 * @version 1.0
 * @date 2021/4/4 20:19
 */
@Data
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class Users {
    private Integer id;
    private String name;
    private String pwd;
}
