package io.tapdata.entity.event.ddl.table;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.entity.FieldAttrChange;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.TapUtils;

import java.util.ArrayList;
import java.util.List;

public class TapAlterFieldCheckEvent extends TapTableEvent {
    public static final int TYPE = 308;
    private List<FieldAttrChange<String>> checkChanges;

    public TapAlterFieldCheckEvent() {
        super(TYPE);
    }

    public void clone(TapEvent tapEvent) {
        super.clone(tapEvent);
        if (tapEvent instanceof TapAlterFieldCheckEvent) {
            TapAlterFieldCheckEvent alterFieldCheckEvent = (TapAlterFieldCheckEvent) tapEvent;
            if (checkChanges != null)
                alterFieldCheckEvent.checkChanges = new ArrayList<>(checkChanges);
        }
    }
}
