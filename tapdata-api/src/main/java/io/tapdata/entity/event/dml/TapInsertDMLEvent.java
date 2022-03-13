package io.tapdata.entity.event.dml;


import java.util.Map;

public class TapInsertDMLEvent extends TapDMLEvent {
    /**
     * The latest record, after insert and update
     * Value format should follow TapType formats
     */
    private Map<String, Object> after;

    public Map<String, Object> getAfter() {
        return after;
    }

    public void setAfter(Map<String, Object> after) {
        this.after = after;
    }
}
