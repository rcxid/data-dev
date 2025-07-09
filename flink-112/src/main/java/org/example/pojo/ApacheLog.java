package org.example.pojo;

/**
 * @author rcxid
 * @version 1.0
 * @date 2021/4/12 19:09
 */
public class ApacheLog {
    private String ip;
    private Long ts;
    private String type;
    private String url;

    public ApacheLog() {
    }

    public ApacheLog(String ip, Long ts, String type, String url) {
        this.ip = ip;
        this.ts = ts;
        this.type = type;
        this.url = url;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
}
