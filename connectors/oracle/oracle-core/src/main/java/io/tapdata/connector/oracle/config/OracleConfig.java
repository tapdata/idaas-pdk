package io.tapdata.connector.oracle.config;

import io.tapdata.config.CommonDbConfig;

import java.io.Serializable;

/**
 * Oracle database config
 *
 * @author Jarad
 * @date 2022/6/2
 */
public class OracleConfig extends CommonDbConfig implements Serializable {

    private String logPluginName = "";

    public OracleConfig() {
        setDbType("oracle");
        setJdbcDriver("oracle.jdbc.driver.OracleDriver");
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

