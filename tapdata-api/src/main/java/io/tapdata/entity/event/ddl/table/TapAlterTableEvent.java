package io.tapdata.entity.event.ddl.table;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapField;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class TapAlterTableEvent extends TapTableEvent {
    public static final int TYPE = 200;
    private Map<String, TapField> changedNameFields;
    private List<TapField> insertFields;
    private List<String> deleteFields;

    public TapAlterTableEvent() {
        super(TYPE);
    }

    @Override
    public void clone(TapEvent tapEvent) {
        super.clone(tapEvent);
        if(tapEvent instanceof TapAlterTableEvent) {
            TapAlterTableEvent alterTableEvent = (TapAlterTableEvent) tapEvent;
            if(changedNameFields != null)
                alterTableEvent.changedNameFields = new ConcurrentHashMap<>(changedNameFields);
            if(insertFields != null)
                alterTableEvent.insertFields = new CopyOnWriteArrayList<>(insertFields);
            if(deleteFields != null)
                alterTableEvent.deleteFields = new CopyOnWriteArrayList<>(deleteFields);

        }
    }

    public Map<String, TapField> getChangedNameFields() {
        return changedNameFields;
    }

    public void setChangedNameFields(Map<String, TapField> changedNameFields) {
        this.changedNameFields = changedNameFields;
    }

    public List<TapField> getInsertFields() {
        return insertFields;
    }

    public void setInsertFields(List<TapField> insertFields) {
        this.insertFields = insertFields;
    }

    public List<String> getDeleteFields() {
        return deleteFields;
    }

    public void setDeleteFields(List<String> deleteFields) {
        this.deleteFields = deleteFields;
    }
}
