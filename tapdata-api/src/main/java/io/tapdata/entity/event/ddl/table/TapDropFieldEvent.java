package io.tapdata.entity.event.ddl.table;

import io.tapdata.entity.schema.TapField;

import java.util.List;

public class TapDropFieldEvent extends TapTableEvent {
    public static final int TYPE = 306;
    private List<String> fields;

    public TapDropFieldEvent() {
        super(TYPE);
    }
}
