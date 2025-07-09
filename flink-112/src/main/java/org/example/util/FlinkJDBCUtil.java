package org.example.util;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;

/**
 * @author rcxid
 * @version 1.0
 * @date 2021/4/26 9:05
 */
public class FlinkJDBCUtil {

    /**
     * url
     */
    private final String url;
    /**
     * 驱动全类名
     */
    private final String driver;
    /**
     * 用户名
     */
    private final String username;
    /**
     * 密码
     */
    private final String password;

    public FlinkJDBCUtil(String url, String driver, String username, String password) {
        this.url = url;
        this.driver = driver;
        this.username = username;
        this.password = password;
    }

    /**
     * 自定义Clickhouse sink
     *
     * @param table 表名
     * @param tClass 保存数据类型
     * @param <T> 泛型
     * @return sink function
     */
    public <T> SinkFunction<T> getJDBCSinks(String table, Class<T> tClass) {
        StringBuilder sql = new StringBuilder();
        sql.append("insert into ").append(table).append("(");
        Field[] fields = tClass.getDeclaredFields();
        for (Field field : fields) {
            sql.append(field.getName()).append(",");
        }
        sql.deleteCharAt(sql.length() - 1);
        sql.append(") values (");
        for (Field ignored : fields) {
            sql.append("?,");
        }
        sql.deleteCharAt(sql.length() -1);
        sql.append(")");
        return getJDBCSink(sql.toString(), tClass);
    }

    /**
     * 获取jdbc sink
     *
     * @param sql sql
     * @param tClass tClass
     * @param <T> 泛型T
     * @return sinkFunction
     */
    private  <T> SinkFunction<T> getJDBCSink(String sql, Class<T> tClass) {
        return JdbcSink.sink(
                sql,
                (ps, t) -> {
                    Field[] fields = tClass.getDeclaredFields();
                    for (int i = 0; i < fields.length; i++) {
                        try {
                            fields[i].setAccessible(true);
                            ps.setObject(i + 1, fields[i].get(t));
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }
                    }
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(1)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(url)
                        .withDriverName(driver)
                        .withUsername(username)
                        .withPassword(password)
                        .build()
        );
    }
}
