package org.example.pojo;

/**
 * @author vision
 * @version 1.0
 * @date 2021/4/6 19:29
 */
public class MarketingUserBehavior {
    private Long userId;
    private String behavior;
    private String channel;
    private Long timestamp;

    public MarketingUserBehavior() {
    }

    public MarketingUserBehavior(Long userId, String behavior, String channel, Long timestamp) {
        this.userId = userId;
        this.behavior = behavior;
        this.channel = channel;
        this.timestamp = timestamp;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getBehavior() {
        return behavior;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
}
