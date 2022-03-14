package io.tapdata.entity.event.dml;


import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.value.TapValue;

import java.util.List;
import java.util.Map;

public class TapDeleteDMLEvent extends TapDMLEvent {

    private Map<String, Object> before;
    public TapDeleteDMLEvent init() {
        time = System.currentTimeMillis();
        return this;
    }

    public TapDeleteDMLEvent referenceTime(Long referenceTime) {
        this.referenceTime = referenceTime;
        return this;
    }

    public TapDeleteDMLEvent before(Map<String, Object> before) {
        this.before = before;
        return this;
    }
    public TapDeleteDMLEvent table(TapTable table) {
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
