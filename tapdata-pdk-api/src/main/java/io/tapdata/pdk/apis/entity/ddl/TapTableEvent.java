package io.tapdata.pdk.apis.entity.ddl;

public class TapTableEvent extends TapDDLEvent {
    public static final String TYPE_CREATE_TABLE = "c";
    public static final String TYPE_ALTER_TABLE_NAME = "an";
    public static final String TYPE_DROP_TABLE = "d";
    public static final String TYPE_CLEAR_TABLE = "cr";
    private String type;

    private TapTable table;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public TapTable getTable() {
        return table;
    }

    public void setTable(TapTable table) {
        this.table = table;
    }
}
