package org.apache.flink.streaming.connectors.redis.feature.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class RedisFeatureOptions {
    public static final ConfigOption<String> HOST =
            ConfigOptions.key("host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Required host for connect to redis");

    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(6379)
                    .withDescription("Optional port for connect to redis");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional password for connect to redis");

    public static final ConfigOption<Integer> TTL =
            ConfigOptions.key("ttl")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Optional ttl for insert to redis");
}
