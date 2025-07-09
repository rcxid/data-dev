package org.example.function.custom;

/**
 * @author rcxid
 * @version 1.0
 * @date 2021/4/15 14:47
 */
public class AvgAcc {
    private Long sum;
    private Integer count;

    public AvgAcc() {
    }

    public AvgAcc(Long sum, Integer count) {
        this.sum = sum;
        this.count = count;
    }

    public Long getSum() {
        return sum;
    }

    public void setSum(Long sum) {
        this.sum = sum;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }
}
