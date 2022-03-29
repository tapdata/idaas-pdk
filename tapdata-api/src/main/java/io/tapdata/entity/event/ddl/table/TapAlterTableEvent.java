package io.tapdata.entity.event.ddl.table;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class TapAlterTableEvent extends TapTableEvent {
    //
    private Map<String, TapField> changedNameFields;
    private List<TapField> insertFields;
    private List<String> deletedFields;

    /**
     * 表名
     */
    private String name;
    /**
     * 存储引擎， innoDB
     */
    private String storageEngine;
    /**
     * 字符编码
     */
    private String charset;

    public void clone(TapEvent tapEvent) {
        super.clone(tapEvent);
        if(tapEvent instanceof TapAlterTableEvent) {
            TapAlterTableEvent alterTableEvent = (TapAlterTableEvent) tapEvent;
            if(changedNameFields != null)
                alterTableEvent.changedNameFields = new ConcurrentHashMap<>(changedNameFields);
            if(insertFields != null)
                alterTableEvent.insertFields = new CopyOnWriteArrayList<>(insertFields);
            if(deletedFields != null)
                alterTableEvent.deletedFields = new CopyOnWriteArrayList<>(deletedFields);
            alterTableEvent.name = name;
            alterTableEvent.storageEngine = storageEngine;
            alterTableEvent.charset = charset;
        }
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getStorageEngine() {
        return storageEngine;
    }

    public void setStorageEngine(String storageEngine) {
        this.storageEngine = storageEngine;
    }

    public String getCharset() {
        return charset;
    }

    public void setCharset(String charset) {
        this.charset = charset;
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

    public List<String> getDeletedFields() {
        return deletedFields;
    }

    public void setDeletedFields(List<String> deletedFields) {
        this.deletedFields = deletedFields;
    }
}
