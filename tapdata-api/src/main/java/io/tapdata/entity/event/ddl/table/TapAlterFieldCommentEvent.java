package io.tapdata.entity.event.ddl.table;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.entity.FieldAttrChange;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.TapUtils;

import java.util.ArrayList;
import java.util.List;

public class TapAlterFieldCommentEvent extends TapTableEvent {
    public static final int TYPE = 300;
    private List<FieldAttrChange<String>> comments;

    public TapAlterFieldCommentEvent() {
        super(TYPE);
    }
    public void clone(TapEvent tapEvent) {
        super.clone(tapEvent);
        if (tapEvent instanceof TapAlterFieldCommentEvent) {
            TapAlterFieldCommentEvent alterFieldCommentEvent = (TapAlterFieldCommentEvent) tapEvent;
            if (comments != null)
                alterFieldCommentEvent.comments = new ArrayList<>(comments);
        }
    }
}
