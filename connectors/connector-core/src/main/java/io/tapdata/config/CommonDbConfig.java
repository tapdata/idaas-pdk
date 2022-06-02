package io.tapdata.config;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.BeanUtils;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;

import java.io.Serializable;
import java.util.Map;

public abstract class CommonDbConfig implements Serializable {

    private static final String TAG = CommonDbConfig.class.getSimpleName();
    private static final JsonParser jsonParser = InstanceFactory.instance(JsonParser.class);
    private static final BeanUtils beanUtils = InstanceFactory.instance(BeanUtils.class);

    private String extParams;
    private String host;
    private int port;
    private String database;
    private String schema;
    private String user;
    private String password;

    public CommonDbConfig load(String json) {
        try {
            beanUtils.copyProperties(jsonParser.fromJson(json, this.getClass()), this);
            return this;
        } catch (Exception e) {
            TapLogger.error(TAG, "config json file is invalid!");
            e.printStackTrace();
        }
        return null;
    }

    public CommonDbConfig load(Map<String, Object> map) {
        try {
            return beanUtils.mapToBean(map, this);
        } catch (Exception e) {
            TapLogger.error(TAG, "config map is invalid!");
            e.printStackTrace();
        }
        return null;
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
}
