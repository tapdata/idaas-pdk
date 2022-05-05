package io.tapdata.entity.event.ddl.table;

import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.schema.TapTable;

public class TapCreateTableEvent extends TapTableEvent {
    private TapTable table;

    public TapTable getTable() {
        return table;
    }

    public void setTable(TapTable table) {
        this.table = table;
    }
}
