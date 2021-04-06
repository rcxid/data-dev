package org.example.sink;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;
import org.example.pojo.Users;

/**
 * @author vision
 * @version 1.0
 * @date 2021/4/4 19:54
 */
public class FlinkRedisSink {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        FlinkJedisPoolConfig redisConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("bigdata")
                .setPort(6379)
                .setMaxTotal(100)
                .setTimeout(1000 * 10)
                .build();

        source.flatMap(new FlatMapFunction<String, Users>() {
            @Override
            public void flatMap(String line, Collector<Users> collector) throws Exception {
                String[] split = line.split(",");
                collector.collect(new Users(Integer.valueOf(split[0]), split[1], split[2]));
            }
        }).addSink(new RedisSink<>(redisConfig, new RedisMapper<Users>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.SET);
            }

            @Override
            public String getKeyFromData(Users users) {
                return users.getName();
            }

            @Override
            public String getValueFromData(Users users) {
                return JSON.toJSONString(users);
            }
        }));



        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
