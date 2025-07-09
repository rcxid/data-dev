package org.apache.flink.streaming.connectors.redis.feature.config;

import java.io.Serializable;

public class RedisFeatureConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String host;
    private final Integer port;
    private final String password;
    private final Integer ttl;

    public RedisFeatureConfig(String host, Integer port, String password, Integer ttl) {
        this.host = host;
        this.port = port;
        this.password = password;
        this.ttl = ttl;
    }

    public RedisFeatureConfig(String host, String port, String password, String ttl) {
        this.host = host;
        if (port == null) {
            this.port = RedisFeatureOptions.PORT.defaultValue();
        } else {
            this.port = Integer.parseInt(port);
        }
        this.password = password;
        if (ttl == null) {
            this.ttl = RedisFeatureOptions.TTL.defaultValue();
        } else {
            this.ttl = Integer.parseInt(ttl);
        }
    }

    public String getHost() {
        return host;
    }

    public Integer getPort() {
        return port;
    }

    public String getPassword() {
        return password;
    }

    public Integer getTtl() {
        return ttl;
    }
}
