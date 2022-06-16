package io.tapdata.entity.event.ddl.table;

import io.tapdata.entity.event.ddl.entity.FieldAttrChange;

import java.util.List;

public class TapAlterFieldCommentEvent extends TapTableEvent {
    public static final int TYPE = 300;
    private List<FieldAttrChange<String>> comments;

    public TapAlterFieldCommentEvent() {
        super(TYPE);
    }
}
