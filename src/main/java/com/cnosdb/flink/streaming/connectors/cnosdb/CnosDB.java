package com.cnosdb.flink.streaming.connectors.cnosdb;


import okhttp3.*;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;


public class CnosDB {
    OkHttpClient client;
    CnosDBConfig cnosDBConfig;

    private static final MediaType TEXT =
            MediaType.get("text/plain; charset=utf-8");

    private static final String TENANT_QUERY_KEY = "tenant";
    private static final String DATABASE_QUERY_KEY = "db";

    CnosDB(CnosDBConfig config) {
        this.client = new OkHttpClient();
        this.cnosDBConfig = config;
    }

    public void write(CnosDBPoint point) throws Exception{
        Preconditions.checkNotNull(point, "point can not be null");
        RequestBody body = RequestBody.create(point.lineProtocol(), TEXT);
        String urlString = Preconditions.checkNotNull(cnosDBConfig.getUrl(), "url can not be null");
        HttpUrl.Builder httpBuilder  = Preconditions.checkNotNull(HttpUrl.parse(urlString),  String.format("url:%s is invalid", urlString))
                .newBuilder();
        httpBuilder.addPathSegments("api/v1/write");

        String tenant = this.cnosDBConfig.getTenant();
        if (!StringUtils.isNullOrWhitespaceOnly(tenant)) {
            httpBuilder.addQueryParameter(TENANT_QUERY_KEY, tenant);
        }

        httpBuilder.addQueryParameter(DATABASE_QUERY_KEY, cnosDBConfig.getDatabase());
        String credential = Credentials.basic(cnosDBConfig.getUsername(), cnosDBConfig.getPassword());

        Request request = new Request.Builder().url(httpBuilder.build())
                .header("Authorization", credential)
                .post(body)
                .build();
        Response response = client.newCall(request).execute();
        if (response.code() != 200) {
            assert response.body() != null;
            throw new RuntimeException(response.body().string());
        }
    }

}
