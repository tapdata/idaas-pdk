package io.tapdata.connector.oracle.config;

import io.tapdata.common.CommonDbConfig;
import io.tapdata.kit.EmptyKit;

import java.io.Serializable;
import java.util.Map;

/**
 * Oracle database config
 *
 * @author Jarad
 * @date 2022/6/2
 */
public class OracleConfig extends CommonDbConfig implements Serializable {

    private String logPluginName = "";

    public OracleConfig() {
        //customize oracle config
        setDbType("oracle");
        setJdbcDriver("oracle.jdbc.driver.OracleDriver");
    }

    @Override
    public OracleConfig load(Map<String, Object> map) {
        CommonDbConfig config = super.load(map);
        if (EmptyKit.isEmpty(config.getSchema())) {
            config.setSchema(getUser().toUpperCase());
        }
        return (OracleConfig) config;
    }

    @Override
    public String getDatabaseUrlPattern() {
        return "jdbc:" + this.getDbType() + ":thin:@//%s:%d/%s%s";
    }

    public String getLogPluginName() {
        return logPluginName;
    }

    public void setLogPluginName(String logPluginName) {
        this.logPluginName = logPluginName;
    }
}

