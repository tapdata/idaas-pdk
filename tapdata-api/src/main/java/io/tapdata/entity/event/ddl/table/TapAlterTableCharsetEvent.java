package io.tapdata.entity.event.ddl.table;

import io.tapdata.entity.event.ddl.entity.FieldAttrChange;

import java.util.List;

public class TapAlterTableCharsetEvent extends TapTableEvent {
    public static final int TYPE = 200;
    private String charset;

    public TapAlterTableCharsetEvent() {
        super(TYPE);
    }
}
