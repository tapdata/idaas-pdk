package io.tapdata.entity.event.ddl.table;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.entity.FieldAttrChange;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.TapUtils;

import java.util.ArrayList;
import java.util.List;

public class TapAlterFieldDataTypeEvent extends TapTableEvent {
    public static final int TYPE = 302;
    private List<FieldAttrChange<String>> nameChanges;

    public TapAlterFieldDataTypeEvent() {
        super(TYPE);
    }

    public void clone(TapEvent tapEvent) {
        super.clone(tapEvent);
        if (tapEvent instanceof TapAlterFieldDataTypeEvent) {
            TapAlterFieldDataTypeEvent alterFieldDataTypeEvent = (TapAlterFieldDataTypeEvent) tapEvent;
            if (nameChanges != null)
                alterFieldDataTypeEvent.nameChanges = new ArrayList<>(nameChanges);
        }
    }
}
