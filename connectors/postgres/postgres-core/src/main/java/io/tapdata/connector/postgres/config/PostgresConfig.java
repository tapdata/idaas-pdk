package io.tapdata.connector.postgres.config;

import io.tapdata.config.CommonDbConfig;
import io.tapdata.kit.EmptyKit;

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

    }

    public String getDatabaseUrl() {
        if (EmptyKit.isNotEmpty(this.getExtParams()) && !this.getExtParams().startsWith("?")) {
            this.setExtParams("?" + this.getExtParams());
        }
        return String.format(this.getDatabaseUrlPattern(), this.getHost(), this.getPort(), this.getDatabase(), this.getExtParams());
    }

    public String getJdbcDriver() {
        return "org.postgresql.Driver";
    }

    public String getDatabaseUrlPattern() {
        // last %s reserved for extend params
        return "jdbc:postgresql://%s:%d/%s%s";
    }

    public String getLogPluginName() {
        return logPluginName;
    }

    public void setLogPluginName(String logPluginName) {
        this.logPluginName = logPluginName;
    }
}
