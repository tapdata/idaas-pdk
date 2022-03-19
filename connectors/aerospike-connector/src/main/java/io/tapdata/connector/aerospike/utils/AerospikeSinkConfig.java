package io.tapdata.connector.aerospike.utils;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

public class AerospikeSinkConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private String seedHosts;
    private String keyspace;
    private String columnName;
    private String userName;
    private String password;
    private String keySet;
    private int maxConcurrentRequests = 100;
    private int timeoutMs = 100;
    private int retries = 1;

    public static AerospikeSinkConfig load(String jsonFile) throws  IOException{
        ObjectMapper mapper = new ObjectMapper(new JsonFactory());
        return mapper.readValue(new File(jsonFile),AerospikeSinkConfig.class);
    }

    public static AerospikeSinkConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        // TODO check map
        return mapper.readValue((new ObjectMapper()).writeValueAsString(map), AerospikeSinkConfig.class);
    }

    public AerospikeSinkConfig() {
    }

    public String getSeedHosts() {
        return this.seedHosts;
    }

    public String getKeyspace() {
        return this.keyspace;
    }

    public String getColumnName() {
        return this.columnName;
    }

    public String getUserName() {
        return this.userName;
    }

    public String getPassword() {
        return this.password;
    }

    public String getKeySet() {
        return this.keySet;
    }

    public int getMaxConcurrentRequests() {
        return this.maxConcurrentRequests;
    }

    public int getTimeoutMs() {
        return this.timeoutMs;
    }

    public int getRetries() {
        return this.retries;
    }

    public AerospikeSinkConfig setSeedHosts(String seedHosts) {
        this.seedHosts = seedHosts;
        return this;
    }

    public AerospikeSinkConfig setKeyspace(String keyspace) {
        this.keyspace = keyspace;
        return this;
    }

    public AerospikeSinkConfig setColumnName(String columnName) {
        this.columnName = columnName;
        return this;
    }

    public AerospikeSinkConfig setUserName(String userName) {
        this.userName = userName;
        return this;
    }

    public AerospikeSinkConfig setPassword(String password) {
        this.password = password;
        return this;
    }

    public AerospikeSinkConfig setKeySet(String keySet) {
        this.keySet = keySet;
        return this;
    }

    public AerospikeSinkConfig setMaxConcurrentRequests(int maxConcurrentRequests) {
        this.maxConcurrentRequests = maxConcurrentRequests;
        return this;
    }

    public AerospikeSinkConfig setTimeoutMs(int timeoutMs) {
        this.timeoutMs = timeoutMs;
        return this;
    }

    public AerospikeSinkConfig setRetries(int retries) {
        this.retries = retries;
        return this;
    }
}