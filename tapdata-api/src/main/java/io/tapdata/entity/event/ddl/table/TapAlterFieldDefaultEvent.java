package io.tapdata.entity.event.ddl.table;

import io.tapdata.entity.event.ddl.entity.FieldAttrChange;

import java.util.List;

public class TapAlterFieldDefaultEvent extends TapTableEvent {
    public static final int TYPE = 303;

    private List<FieldAttrChange<Object>> defaultChanges;

    public TapAlterFieldDefaultEvent() {
        super(TYPE);
    }
}
