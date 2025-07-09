package org.example.test;

/**
 * @author rcxid
 * @version 1.0
 * @date 2021/4/10 11:01
 */
public class StaticClassTest {

    private String name;



    public static void main(String[] args) {
        StaticClass c = new StaticClass("a");
        System.out.println(c.id);
    }


    public static class StaticClass {
        private String id;

        public StaticClass(String id) {
            this.id = id;
        }
    }
}
