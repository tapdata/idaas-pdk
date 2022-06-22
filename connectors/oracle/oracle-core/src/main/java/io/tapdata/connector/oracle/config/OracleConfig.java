package io.tapdata.connector.oracle.config;

import io.tapdata.common.CommonDbConfig;
import io.tapdata.kit.EmptyKit;

import java.io.Serializable;
import java.time.ZoneId;
import java.util.Map;
import java.util.TimeZone;

/**
 * Oracle database config
 *
 * @author Jarad
 * @date 2022/6/2
 */
public class OracleConfig extends CommonDbConfig implements Serializable {

    private String logPluginName = "";
    private String pdb = "";
    private Integer fetchSize = 100;
    private ZoneId sysZoneId = TimeZone.getDefault().toZoneId();

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

    public String getPdb() {
        return pdb;
    }

    public void setPdb(String pdb) {
        this.pdb = pdb;
    }

    public Integer getFetchSize() {
        return fetchSize;
    }

    public void setFetchSize(Integer fetchSize) {
        this.fetchSize = fetchSize;
    }

    public ZoneId getSysZoneId() {
        return sysZoneId;
    }

    public void setSysZoneId(ZoneId sysZoneId) {
        this.sysZoneId = sysZoneId;
    }
}

