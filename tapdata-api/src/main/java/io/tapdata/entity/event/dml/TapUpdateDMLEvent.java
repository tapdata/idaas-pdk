package io.tapdata.entity.event.dml;


import io.tapdata.entity.value.TapValue;

import java.util.LinkedHashMap;
import java.util.Map;

public class TapUpdateDMLEvent extends TapDMLEvent {
    /**
     * The latest record, after insert and update
     * Value format should follow TapType formats
     */
    private Map<String, Object> after;
    /**
     * The last record, especially before update and delete
     * Value format should follow TapType formats
     */
    private Map<String, Object> before;

    public Map<String, Object> getAfter() {
        return after;
    }

    public void setAfter(Map<String, Object> after) {
        this.after = after;
    }

    public Map<String, Object> getBefore() {
        return before;
    }

    public void setBefore(Map<String, Object> before) {
        this.before = before;
    }

    public void putAfterValue(String name, Object value) {
        if(this.after == null) {
            this.after = new LinkedHashMap<>();
        }
        this.after.put(name, value);
    }

    public void removeAfterValue(String name) {
        if(this.after != null) {
            this.after.remove(name);
        }
    }
}
