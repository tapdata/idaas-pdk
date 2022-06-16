package io.tapdata.entity.event.ddl.table;

import io.tapdata.entity.event.ddl.entity.FieldAttrChange;

import java.util.List;

public class TapAlterFieldConstraintEvent extends TapTableEvent {
    public static final int TYPE = 301;
    private List<FieldAttrChange<String>> constraintChanges;

    public TapAlterFieldConstraintEvent() {
        super(TYPE);
    }
}
