package io.tapdata.entity.event.ddl.table;

import io.tapdata.entity.event.ddl.entity.FieldAttrChange;

import java.util.List;

public class TapAlterFieldDataTypeEvent extends TapTableEvent {
    public static final int TYPE = 302;
    private List<FieldAttrChange<String>> nameChanges;

    public TapAlterFieldDataTypeEvent() {
        super(TYPE);
    }
}
