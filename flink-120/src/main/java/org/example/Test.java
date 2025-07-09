package org.example;

import java.util.ArrayList;
import java.util.List;

public class Test {
    public static void main(String[] args) {
        List<String> list = new ArrayList<>();
        String a = "fdsfasdfasdfadsfasdfasdf";
        String b = "fdsfasdfasdfadsfasdfasd";
        StringBuilder builder = new StringBuilder();
        builder.append(b);
        builder.append("f");
        String c = builder.toString();
        list.add(a);
        if (list.contains(c)) {
            System.out.println("1");
        } else {
            System.out.println("2");
        }

    }
}
