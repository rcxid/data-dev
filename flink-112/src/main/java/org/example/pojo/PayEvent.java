package org.example.pojo;

/**
 * @author vision
 * @version 1.0
 * @date 2021/4/6 20:05
 */
public class PayEvent {
    private String txId;
    private String payChannel;
    private Long eventTime;

    public PayEvent() {
    }

    public PayEvent(String txId, String payChannel, Long eventTime) {
        this.txId = txId;
        this.payChannel = payChannel;
        this.eventTime = eventTime;
    }

    public String getTxId() {
        return txId;
    }

    public void setTxId(String txId) {
        this.txId = txId;
    }

    public String getPayChannel() {
        return payChannel;
    }

    public void setPayChannel(String payChannel) {
        this.payChannel = payChannel;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }
}
