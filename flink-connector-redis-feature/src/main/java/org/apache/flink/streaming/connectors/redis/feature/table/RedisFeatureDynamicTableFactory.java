package org.apache.flink.streaming.connectors.redis.feature.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.redis.feature.config.RedisFeatureOptions;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.logical.*;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RedisFeatureDynamicTableFactory implements DynamicTableSinkFactory {

    /**
     * 指定connector名称: 'connector' = 'redis-feature'
     */
    public static final String IDENTIFIER = "redis-feature";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    /**
     * 规定连接器的必选项
     *
     * @return 连接器必选项
     */
    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(RedisFeatureOptions.HOST);
        return options;
    }

    /**
     * 规定连接器的可选项
     *
     * @return 连接器可选项
     */
    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(RedisFeatureOptions.PORT);
        options.add(RedisFeatureOptions.PASSWORD);
        options.add(RedisFeatureOptions.TTL);
        return options;
    }

    /**
     * 创建动态sink表
     *
     * @param context context
     * @return 动态sink表
     */
    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig config = helper.getOptions();
        helper.validate();
        // check datatype
        // first field must be string type as redis key
        // other filed can be integer、long、float、string type as redis hash field
        ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
        // 字段数量和类型检查
        checkDateSchema(schema);
        return new RedisFeatureDynamicTableSink(
                config,
                context.getCatalogTable().getOptions(),
                context.getCatalogTable().getResolvedSchema());
    }

    private void checkDateSchema(ResolvedSchema schema) {
        List<Column> columns = schema.getColumns();
        if (columns.size() < 2) {
            throw new ValidationException("Schema must have a key filed and a value field!");
        }
        Column keyColumn = columns.get(0);
        if (!keyColumn.getName().equals("key")) {
            throw new ValidationException("Schema first filed name must be key!");
        }
        if (!(keyColumn.getDataType().getLogicalType() instanceof VarCharType)) {
            throw new ValidationException("Schema first filed must be string type!");
        }
        for (int i = 1; i < columns.size(); i++) {
            // 判断字段类型
            LogicalType logicalType = columns.get(i).getDataType().getLogicalType();
            if (!(logicalType instanceof VarCharType) &&
                    !(logicalType instanceof IntType) &&
                    !(logicalType instanceof BigIntType) &&
                    !(logicalType instanceof FloatType)) {
                throw new ValidationException("Schema other filed must be string、int、bigint、float type!");
            }
        }
    }
}
