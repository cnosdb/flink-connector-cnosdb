package org.example;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.cnosdb.CnosDBConfig;
import org.apache.flink.streaming.connectors.cnosdb.CnosDBPoint;
import org.apache.flink.streaming.connectors.cnosdb.CnosDBSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class CnosDBSinkExample {
    private static final Logger LOG = LoggerFactory.getLogger(CnosDBSinkExample.class);

    private static final int N = 10000;

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<String> dataList = new ArrayList<>();
        for (int i = 0; i < N; ++i) {
            String id = "server" + String.valueOf(i);
            dataList.add("cpu#" + id);
            dataList.add("mem#" + id);
            dataList.add("disk#" + id);
        }
        DataStream<String> source = env.fromElements(dataList.toArray(new String[0]));


        DataStream<CnosDBPoint> dataStream = source.map(
                new RichMapFunction<String, CnosDBPoint>() {
                    @Override
                    public CnosDBPoint map(String s) throws Exception {
                        String[] input = s.split("#");

                        String measurement = input[0];
                        long timestamp = System.currentTimeMillis();

                        HashMap<String, String> tags = new HashMap<>();
                        tags.put("host", input[1]);
                        tags.put("region", "region#" + String.valueOf(input[1].hashCode() % 20));

                        HashMap<String, Object> fields = new HashMap<>();
                        fields.put("value1", input[1].hashCode() % 100);
                        fields.put("value2", input[1].hashCode() % 50);

                        return new CnosDBPoint(measurement, timestamp, tags, fields);
                    }
                }
        );

        CnosDBConfig cnosDBConfig = CnosDBConfig.builder()
                .url("http://localhost:8902")
                .database("db_flink_test")
                .username("root")
                .password("")
                .build();

        dataStream.addSink(new CnosDBSink(cnosDBConfig));
        env.execute("CnosDB Sink Example");
    }

}
