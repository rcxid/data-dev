package org.apache.flink.streaming.connectors.redis.feature.table;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.redis.feature.config.RedisFeatureColumnConfig;
import org.apache.flink.streaming.connectors.redis.feature.config.RedisFeatureConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.types.RowKind;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RedisFeatureSinkFunction<IN> extends RichSinkFunction<IN> {

    private static final long serialVersionUID = 1L;

    private final List<RedisFeatureColumnConfig> columns;
    private final RedisFeatureConfig featureConfig;
    private RedisClient client;
    private StatefulRedisConnection<byte[], byte[]> connect;
    private RedisAsyncCommands<byte[], byte[]> async;

    public RedisFeatureSinkFunction( List<RedisFeatureColumnConfig> columns, RedisFeatureConfig featureConfig) {
        this.columns = columns;
        this.featureConfig = featureConfig;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        String url = String.format("redis://:%s@%s:%s", featureConfig.getPassword(), featureConfig.getHost(), featureConfig.getPort());
        client = RedisClient.create(url);
        connect = client.connect(ByteArrayCodec.INSTANCE);
        async = connect.async();
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        connect.close();
        client.close();
        super.close();
    }

    @Override
    public void invoke(IN value, Context context) throws Exception {
        RowData rowData = (RowData) value;
        RowKind kind = rowData.getRowKind();
        if (kind == RowKind.INSERT) {
            byte[] redisKey = rowData.getString(0).toString().getBytes();
            Map<byte[], byte[]> data = new HashMap<>();
            for (int i = 1; i < columns.size(); i++) {
                RedisFeatureColumnConfig column = columns.get(i);
                byte[] bytes = serializeData(column, rowData, i);
                if (bytes.length > 0) {
                    data.put(column.getName().getBytes(), bytes);
                }
            }
            async.hset(redisKey, data);
            async.expire(redisKey, featureConfig.getTtl() * 24 * 3600);
        }
        super.invoke(value, context);
    }

    private byte[] serializeData(RedisFeatureColumnConfig column, RowData rowData, int index) {
        LogicalType logicalType = column.getLogicalType();
        if (logicalType instanceof IntType) {
            return intToByteArray(rowData.getInt(index));
        } else if (logicalType instanceof BigIntType) {
            return longToByteArray(rowData.getLong(index));
        } else if (logicalType instanceof FloatType) {
            return floatToByteArray(rowData.getFloat(index));
        } else if (logicalType instanceof VarCharType) {
            return rowData.getString(index).toString().getBytes();
        } else {
            return new byte[0];
        }
    }

    private byte[] intToByteArray(int value) {
        return new byte[]{
                (byte) (value >>> 24),  // 高8位
                (byte) (value >>> 16),  // 次高8位
                (byte) (value >>> 8),   // 次低8位
                (byte) value            // 低8位
        };
    }

    private byte[] longToByteArray(long value) {
        return new byte[]{
                (byte) (value >>> 56), // 高8位
                (byte) (value >>> 48),
                (byte) (value >>> 40),
                (byte) (value >>> 32),
                (byte) (value >>> 24),
                (byte) (value >>> 16),
                (byte) (value >>> 8),
                (byte) value           // 低8位
        };
    }

    private byte[] floatToByteArray(float value) {
        int intBits = Float.floatToIntBits(value);
        return new byte[]{
                (byte) (intBits >>> 24),
                (byte) (intBits >>> 16),
                (byte) (intBits >>> 8),
                (byte) intBits
        };
    }
}
