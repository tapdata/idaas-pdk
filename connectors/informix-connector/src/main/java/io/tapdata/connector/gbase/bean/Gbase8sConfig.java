package io.tapdata.connector.gbase.bean;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/**
 * Mysql database config
 *
 * @author Jarad
 * @date 2022/4/18
 */
public class Gbase8sConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private String jdbcDriver = "com.gbasedbt.jdbc.Driver";
    private String databaseUrlPattern = "jdbc:gbasedbt-sqli://%s:%d/%s:%s"; // last %s reserved for extend params
    private String extParams;
    private String host;
    private int port;
    private String database;
    private String user;
    private String password;
    private int insertBatchSize = 1000;

    public static Gbase8sConfig load(String jsonFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new JsonFactory());
        return mapper.readValue(new File(jsonFile), Gbase8sConfig.class);
    }

    /**
     * load mysql database config
     *
     * @param map config attributes in Map
     * @return Config
     * @throws IOException IO exception
     */
    public static Gbase8sConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue((new ObjectMapper()).writeValueAsString(map), Gbase8sConfig.class);
    }

    /**
     * get real database url for mysql-jdbc
     *
     * @return DB URL
     */
    public String getDatabaseUrl() {
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
}
