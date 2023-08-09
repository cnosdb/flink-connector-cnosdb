package com.cnosdb.flink.streaming.connectors.cnosdb;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

public class CnosDBSink extends RichSinkFunction<CnosDBPoint> {
    private transient CnosDB cnosDBClient;
    private final CnosDBConfig cnosDBConfig;

    public CnosDBSink(CnosDBConfig cnosDBConfig) {
        this.cnosDBConfig = Preconditions.checkNotNull(cnosDBConfig, "CnosDB client config should not be null");
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        cnosDBClient = new CnosDB(this.cnosDBConfig);
        super.open(parameters);
    }

    @Override
    public void invoke(CnosDBPoint value, Context context) throws Exception {

        if (StringUtils.isNullOrWhitespaceOnly(value.getMeasurement())) {
            throw new RuntimeException("No measurement defined");
        }
        cnosDBClient.write(value);
    }

    @Override
    public void close() throws Exception {
    }
}
