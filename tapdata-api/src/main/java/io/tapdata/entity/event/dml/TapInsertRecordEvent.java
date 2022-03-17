package io.tapdata.entity.event.dml;


import io.tapdata.entity.schema.TapTable;

import java.util.Map;

public class TapInsertRecordEvent extends TapRecordEvent {
    /**
     * The latest record, after insert and update
     * Value format should follow TapType formats
     */
    private Map<String, Object> after;
    public TapInsertRecordEvent init() {
        time = System.currentTimeMillis();
        return this;
    }

    public TapInsertRecordEvent referenceTime(Long referenceTime) {
        this.referenceTime = referenceTime;
        return this;
    }

    public TapInsertRecordEvent after(Map<String, Object> after) {
        this.after = after;
        return this;
    }

    public TapInsertRecordEvent table(TapTable table) {
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