package org.apache.flink.streaming.connectors.redis.feature.config;

import org.apache.flink.table.types.logical.LogicalType;

import java.io.Serializable;

public class RedisFeatureColumnConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String name;
    private final LogicalType logicalType;

    public RedisFeatureColumnConfig(String name, LogicalType logicalType) {
        this.name = name;
        this.logicalType = logicalType;
    }

    public String getName() {
        return name;
    }

    public LogicalType getLogicalType() {
        return logicalType;
    }
}
