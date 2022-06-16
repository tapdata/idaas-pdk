package io.tapdata.entity.event.ddl.table;

import io.tapdata.entity.schema.TapField;

import java.util.List;

public class TapNewFieldEvent extends TapTableEvent {
    public static final int TYPE = 307;
    private List<TapField> newFields;

    public TapNewFieldEvent() {
        super(TYPE);
    }
}
