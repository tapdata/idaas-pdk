package io.tapdata.connector.postgres.config;

import io.tapdata.config.CommonDbConfig;

import java.io.Serializable;

/**
 * Postgres database config
 *
 * @author Jarad
 * @date 2022/4/18
 */
public class PostgresConfig extends CommonDbConfig implements Serializable {

    private String logPluginName = "decoderbufs";

    public PostgresConfig() {
        setDbType("postgresql");
    }

    public String getJdbcDriver() {
        return "org.postgresql.Driver";
    }

    public String getLogPluginName() {
        return logPluginName;
    }

    public void setLogPluginName(String logPluginName) {
        this.logPluginName = logPluginName;
    }
}
