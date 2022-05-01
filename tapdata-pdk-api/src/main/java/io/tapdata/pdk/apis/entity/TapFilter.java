package io.tapdata.pdk.apis.entity;

import io.tapdata.entity.utils.DataMap;

public class TapFilter {
    protected String tableId;
    protected DataMap match;

    public String getTableId() {
        return tableId;
    }

    public void setTableId(String tableId) {
        this.tableId = tableId;
    }

    public DataMap getMatch() {
        return match;
    }

    public void setMatch(DataMap match) {
        this.match = match;
    }
}
