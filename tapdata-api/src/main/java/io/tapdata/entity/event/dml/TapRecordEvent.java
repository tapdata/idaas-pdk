package io.tapdata.entity.event.dml;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapTable;

public class TapRecordEvent extends TapEvent {
    /**
     * 数据源的类型， mysql一类
     */
    private String connector;
    /**
     * 数据源的版本
     */
    private String connectorVersion;

    /**
     * Table name of the record
     */
    protected TapTable table;

    public TapTable getTable() {
        return table;
    }

    public void setTable(TapTable table) {
        this.table = table;
    }

    public String getConnector() {
        return connector;
    }

    public void setConnector(String connector) {
        this.connector = connector;
    }

    public String getConnectorVersion() {
        return connectorVersion;
    }

    public void setConnectorVersion(String connectorVersion) {
        this.connectorVersion = connectorVersion;
    }

}
