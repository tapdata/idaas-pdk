package io.tapdata.entity.event.ddl.table;

import io.tapdata.entity.event.ddl.entity.FieldAttrChange;

import java.util.List;

public class TapAlterFieldNotNullEvent extends TapTableEvent {
    public static final int TYPE = 309;

    private List<FieldAttrChange<Boolean>> notNullChanges;

    public TapAlterFieldNotNullEvent() {
        super(TYPE);
    }
}
