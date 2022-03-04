package io.tapdata.pdk.apis.entity.ddl;

import java.util.List;

public class TapTableOptions {
    private TapTable table;
    public static final String SYNC_MODE_INITIAL = "full_refresh";
    public static final String SYNC_MODE_INCREMENTAL = "incremental";
    /**
     * sync modes of TapOpenAPIStream#SYNC_MODE_INCREMENTAL or TapOpenAPIStream#SYNC_MODE_INITIAL
     */
    private List<String> syncModes;

    public TapTable getTable() {
        return table;
    }

    public void setTable(TapTable table) {
        this.table = table;
    }

    public List<String> getSyncModes() {
        return syncModes;
    }

    public void setSyncModes(List<String> syncModes) {
        this.syncModes = syncModes;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("TapTableOptions").append(": ");
        builder.append("table=").append(table);
        return builder.toString();
    }
}
