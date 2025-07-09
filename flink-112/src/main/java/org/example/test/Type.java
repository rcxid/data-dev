package org.example.test;

/**
 * @author vision
 * @version 1.0
 * @date 2021/4/3 12:41
 */
public enum Type {
    /**
     * A
     */
    TYPE_A(1),
    /**
     * B
     */
    TYPE_B(2),
    /**
     * C
     */
    TYPE_C(3),
    /**
     * D
     */
    TYPE_D(4);

    private final int num;

    Type(int num) {
        this.num = num;
    }

    public int getNum() {
        return num;
    }
}
