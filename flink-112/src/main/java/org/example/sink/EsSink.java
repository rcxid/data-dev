package org.example.sink;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;
import org.example.pojo.Users;

import java.util.Collections;
import java.util.List;

/**
 * @author vision
 * @version 1.0
 * @date 2021/4/4 19:54
 */
public class EsSink {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Users> users = source.flatMap(new FlatMapFunction<String, Users>() {
            @Override
            public void flatMap(String line, Collector<Users> collector) throws Exception {
                String[] split = line.split(",");
                collector.collect(new Users(Integer.valueOf(split[0]), split[1], split[2]));
            }
        });

        List<HttpHost> esHost = Collections.singletonList(new HttpHost("elasticsearch", 9200));


        ElasticsearchSink.Builder<Users> esBuild = new ElasticsearchSink.Builder<>(esHost, new ElasticsearchSinkFunction<Users>() {
            @Override
            public void process(Users users, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                IndexRequest request = Requests.indexRequest("user")
                        .type("_doc")
                        .id(users.getId() + "")
                        .source(JSON.toJSONString(users), XContentType.JSON);
                requestIndexer.add(request);
            }
        });
        esBuild.setBulkFlushMaxActions(1);

        users.addSink(esBuild.build());

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
