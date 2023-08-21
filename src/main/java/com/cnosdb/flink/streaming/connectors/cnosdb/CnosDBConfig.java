package com.cnosdb.flink.streaming.connectors.cnosdb;

import okhttp3.HttpUrl;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

public class CnosDBConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private HttpUrl url;

    private String username;

    private String password;

    private String tenant;

    private String database;

    public CnosDBConfig(CnosDBConfig.Builder builder) {
        Preconditions.checkArgument(builder != null, "CnosDBConfig builder can not be null");

        String urlString = Preconditions.checkNotNull(builder.getUrl(), "host can not be null");
        this.url = Preconditions.checkNotNull(HttpUrl.parse(urlString), String.format("url:%s is invalid", urlString));
        this.username = Preconditions.checkNotNull(builder.getUsername(), "username can not be null");
        this.password = Preconditions.checkNotNull(builder.getPassword(), "password can not be null");
        this.tenant = builder.getTenant();
        this.database = Preconditions.checkNotNull(builder.getDatabase(), "database name can not be null");

    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String url;
        private String username;
        private String password;

        private String tenant;
        private String database;

        public String getUrl() {
            return url;
        }

        public String getUsername() {
            return username;
        }

        public String getPassword() {
            return password;
        }

        public String getTenant() {
            return tenant;
        }

        public String getDatabase() {
            return database;
        }

        public Builder url(String url) {
            this.url = url;
            return this;
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder tenant(String tenant) {
            this.tenant = tenant;
            return this;
        }

        public Builder database(String database) {
            this.database = database;
            return this;
        }

        public CnosDBConfig build() {
            return new CnosDBConfig(this);
        }
    }

    public HttpUrl getUrl() {
        return url;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getTenant() {
        return tenant;
    }

    public String getDatabase() {
        return database;
    }
}
