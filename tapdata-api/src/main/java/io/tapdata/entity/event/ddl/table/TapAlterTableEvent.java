package io.tapdata.entity.event.ddl.table;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.field.TapFieldItem;
import io.tapdata.entity.schema.TapField;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class TapAlterTableEvent extends TapTableEvent {
    public static final int TYPE = 200;
    private List<TapFieldItem> fieldItems;

    public TapAlterTableEvent add(TapFieldItem fieldItem) {
        if(fieldItems == null) {
            fieldItems = new CopyOnWriteArrayList<>();
        }
        fieldItems.add(fieldItem);
        return this;
    }

    public TapAlterTableEvent() {
        super(TYPE);
    }

    @Override
    public void clone(TapEvent tapEvent) {
        super.clone(tapEvent);
        if(tapEvent instanceof TapAlterTableEvent) {
            TapAlterTableEvent alterTableEvent = (TapAlterTableEvent) tapEvent;
            if(fieldItems != null)
                alterTableEvent.fieldItems = new CopyOnWriteArrayList<>(fieldItems);
        }
    }

    public List<TapFieldItem> getFieldItems() {
        return fieldItems;
    }

    public void setFieldItems(List<TapFieldItem> fieldItems) {
        this.fieldItems = fieldItems;
    }
}
