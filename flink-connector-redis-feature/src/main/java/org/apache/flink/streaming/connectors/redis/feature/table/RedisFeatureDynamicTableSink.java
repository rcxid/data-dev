package org.apache.flink.streaming.connectors.redis.feature.table;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.redis.feature.config.RedisFeatureColumnConfig;
import org.apache.flink.streaming.connectors.redis.feature.config.RedisFeatureConfig;
import org.apache.flink.streaming.connectors.redis.feature.config.RedisFeatureOptions;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.types.RowKind;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RedisFeatureDynamicTableSink implements DynamicTableSink {

    private final ReadableConfig config;
    private final ResolvedSchema resolvedSchema;
    private final Map<String, String> properties;
    private final RedisFeatureConfig featureConfig;

    public RedisFeatureDynamicTableSink(ReadableConfig config, Map<String, String> properties, ResolvedSchema resolvedSchema) {
        this.config = config;
        this.properties = properties;
        this.resolvedSchema = resolvedSchema;
        String host = properties.get(RedisFeatureOptions.HOST.key());
        String port = properties.get(RedisFeatureOptions.PORT.key());
        String password = properties.get(RedisFeatureOptions.PASSWORD.key());
        String ttl = properties.get(RedisFeatureOptions.TTL.key());
        this.featureConfig = new RedisFeatureConfig(host, port, password, ttl);
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        List<RedisFeatureColumnConfig> columns = resolvedSchema.getColumns().stream().map(new Function<Column, RedisFeatureColumnConfig>() {
            @Override
            public RedisFeatureColumnConfig apply(Column column) {
                return new RedisFeatureColumnConfig(column.getName(), column.getDataType().getLogicalType());
            }
        }).collect(Collectors.toList());
        return SinkFunctionProvider.of(new RedisFeatureSinkFunction<>(columns, featureConfig), 1);
    }

    @Override
    public DynamicTableSink copy() {
        return new RedisFeatureDynamicTableSink(config, properties, resolvedSchema);
    }

    @Override
    public String asSummaryString() {
        return "REDIS-FEATURE";
    }
}
