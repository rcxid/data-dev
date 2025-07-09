package org.example.function.custom;

/**
 * @author rcxid
 * @version 1.0
 * @date 2021/4/15 15:12
 */
public class TopAcc {
    private Integer first = Integer.MIN_VALUE;
    private Integer second = Integer.MIN_VALUE;

    public TopAcc() {
    }

    public TopAcc(Integer first, Integer second) {
        this.first = first;
        this.second = second;
    }

    public Integer getFirst() {
        return first;
    }

    public void setFirst(Integer first) {
        this.first = first;
    }

    public Integer getSecond() {
        return second;
    }

    public void setSecond(Integer second) {
        this.second = second;
    }
}
