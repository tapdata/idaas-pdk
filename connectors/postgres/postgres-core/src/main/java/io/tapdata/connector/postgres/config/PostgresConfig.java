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

    private static final long serialVersionUID = 1L;
    private static final String TAG = PostgresConfig.class.getSimpleName();

    private String jdbcDriver = "org.postgresql.Driver";
    private String databaseUrlPattern = "jdbc:postgresql://%s:%d/%s%s"; // last %s reserved for extend params
    private String extParams = "";
    private String host;
    private int port;
    private String database;
    private String schema;
    private String user;
    private String password;
    private String logPluginName = "decoderbufs";
    private int insertBatchSize = 1000;

    public String getDatabaseUrl() {
        if (EmptyKit.isNotEmpty(extParams) && !extParams.startsWith("?")) {
            extParams = "?" + extParams;
        }
        return String.format(this.getDatabaseUrlPattern(), this.getHost(), this.getPort(), this.getDatabase(), this.getExtParams());
    }

    public int getInsertBatchSize() {
        return insertBatchSize;
    }

    public void setInsertBatchSize(int insertBatchSize) {
        this.insertBatchSize = insertBatchSize;
    }

    public String getLogPluginName() {
        return logPluginName;
    }

    public void setLogPluginName(String logPluginName) {
        this.logPluginName = logPluginName;
    }
}
