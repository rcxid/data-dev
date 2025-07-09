package org.example.test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * @author rcxid
 * @version 1.0
 * @date 2021/4/12 19:48
 */
public class TimeFormat {
    public static void main(String[] args) {
        DateTimeFormatter df = DateTimeFormatter.ofPattern("dd/MM/yyyy:HH:mm:ss");
        LocalDateTime parse = LocalDateTime.parse("11/04/2021:13:35:13", df);
        System.out.println(parse.toEpochSecond(ZoneOffset.ofHours(8)));
    }
}
