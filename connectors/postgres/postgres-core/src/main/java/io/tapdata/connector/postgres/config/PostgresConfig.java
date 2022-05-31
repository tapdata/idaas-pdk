package io.tapdata.connector.postgres.config;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tapdata.connector.postgres.kit.EmptyKit;
import io.tapdata.entity.logger.TapLogger;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/**
 * Postgres database config
 *
 * @author Jarad
 * @date 2022/4/18
 */
public class PostgresConfig implements Serializable {

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

    public static PostgresConfig load(String jsonFile) {
        try {
            ObjectMapper mapper = new ObjectMapper(new JsonFactory());
            return mapper.readValue(new File(jsonFile), PostgresConfig.class);
        } catch (IOException e) {
            TapLogger.error(TAG, "postgres config file is invalid!");
            e.printStackTrace();
            return new PostgresConfig();
        }
    }

    /**
     * load postgresql database config
     *
     * @param map config attributes in Map
     * @return Config
     */
    public static PostgresConfig load(Map<String, Object> map) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue((new ObjectMapper()).writeValueAsString(map), PostgresConfig.class);
        } catch (IOException e) {
            TapLogger.error(TAG, "postgres config map is invalid!");
            e.printStackTrace();
            return new PostgresConfig();
        }
    }

    /**
     * get real database url for postgresql-jdbc
     *
     * @return DB URL
     */
    public String getDatabaseUrl() {
        if (EmptyKit.isNotEmpty(extParams) && !extParams.startsWith("?")) {
            extParams = "?" + extParams;
        }
        return String.format(this.getDatabaseUrlPattern(), this.getHost(), this.getPort(), this.getDatabase(), this.getExtParams());
    }

    public String getJdbcDriver() {
        return jdbcDriver;
    }

    public void setJdbcDriver(String jdbcDriver) {
        this.jdbcDriver = jdbcDriver;
    }

    public String getDatabaseUrlPattern() {
        return databaseUrlPattern;
    }

    public void setDatabaseUrlPattern(String databaseUrlPattern) {
        this.databaseUrlPattern = databaseUrlPattern;
    }

    public String getExtParams() {
        return extParams;
    }

    public void setExtParams(String extParams) {
        this.extParams = extParams;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
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
