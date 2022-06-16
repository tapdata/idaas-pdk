package io.tapdata.entity.event.ddl.table;

import io.tapdata.entity.event.ddl.entity.FieldAttrChange;

import java.util.List;

public class TapAlterFieldPrimaryKeyEvent extends TapTableEvent {
    public static final int TYPE = 305;

    private List<FieldAttrChange<List<String>>> primaryKeyChanges;

    public TapAlterFieldPrimaryKeyEvent() {
        super(TYPE);
    }
}
