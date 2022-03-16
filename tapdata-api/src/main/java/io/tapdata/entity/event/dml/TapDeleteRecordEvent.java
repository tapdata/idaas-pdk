package io.tapdata.entity.event.dml;


import io.tapdata.entity.schema.TapTable;

import java.util.Map;

public class TapDeleteRecordEvent extends TapRecordEvent {

    private Map<String, Object> before;
    public TapDeleteRecordEvent init() {
        time = System.currentTimeMillis();
        return this;
    }

    public TapDeleteRecordEvent referenceTime(Long referenceTime) {
        this.referenceTime = referenceTime;
        return this;
    }

    public TapDeleteRecordEvent before(Map<String, Object> before) {
        this.before = before;
        return this;
    }
    public TapDeleteRecordEvent table(TapTable table) {
        this.table = table;
        return this;
    }
    public Map<String, Object> getBefore() {
        return before;
    }

    public void setBefore(Map<String, Object> before) {
        this.before = before;
    }
}
