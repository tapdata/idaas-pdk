package io.tapdata.entity.event.ddl.table;

import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.schema.TapTable;

public class TapClearTableEvent extends TapTableEvent {
    public static final int TYPE = 202;
    public TapClearTableEvent() {
        super(TYPE);
    }
}
