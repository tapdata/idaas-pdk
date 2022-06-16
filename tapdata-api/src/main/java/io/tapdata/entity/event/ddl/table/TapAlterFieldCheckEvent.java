package io.tapdata.entity.event.ddl.table;

import io.tapdata.entity.event.ddl.entity.FieldAttrChange;

import java.util.List;

public class TapAlterFieldCheckEvent extends TapTableEvent {
    public static final int TYPE = 308;
    private List<FieldAttrChange<String>> checkChanges;

    public TapAlterFieldCheckEvent() {
        super(TYPE);
    }
}
