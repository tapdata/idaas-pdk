package io.tapdata.entity.event.dml;


import io.tapdata.entity.schema.TapTable;

import java.util.Map;

public class TapInsertDMLEvent extends TapDMLEvent {
    /**
     * The latest record, after insert and update
     * Value format should follow TapType formats
     */
    private Map<String, Object> after;
    public TapInsertDMLEvent init() {
        time = System.currentTimeMillis();
        return this;
    }

    public TapInsertDMLEvent referenceTime(Long referenceTime) {
        this.referenceTime = referenceTime;
        return this;
    }

    public TapInsertDMLEvent after(Map<String, Object> after) {
        this.after = after;
        return this;
    }

    public TapInsertDMLEvent table(TapTable table) {
        this.table = table;
        return this;
    }

    public Map<String, Object> getAfter() {
        return after;
    }

    public void setAfter(Map<String, Object> after) {
        this.after = after;
    }
}
