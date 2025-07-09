package org.example.pojo;

/**
 * @author rcxid
 * @version 1.0
 * @date 2021/4/12 19:05
 */
public class PageCount {
    private String url;
    private Long count;
    private Long windowEndTime;

    public PageCount() {
    }

    public PageCount(String url, Long count, Long windowEndTime) {
        this.url = url;
        this.count = count;
        this.windowEndTime = windowEndTime;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public Long getWindowEndTime() {
        return windowEndTime;
    }

    public void setWindowEndTime(Long windowEndTime) {
        this.windowEndTime = windowEndTime;
    }
}
