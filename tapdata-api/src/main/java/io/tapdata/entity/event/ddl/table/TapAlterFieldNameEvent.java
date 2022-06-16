package io.tapdata.entity.event.ddl.table;

import io.tapdata.entity.event.ddl.entity.FieldAttrChange;

import java.util.List;

public class TapAlterFieldNameEvent extends TapTableEvent {
    public static final int TYPE = 304;
    private List<FieldAttrChange<String>> nameChanges;

    public TapAlterFieldNameEvent() {
        super(TYPE);
    }
}
