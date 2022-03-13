package io.tapdata.entity.event.dml;


import io.tapdata.entity.value.TapValue;

import java.util.List;
import java.util.Map;

public class TapDeleteDMLEvent extends TapDMLEvent {

    private Map<String, Object> before;

    public Map<String, Object> getBefore() {
        return before;
    }

    public void setBefore(Map<String, Object> before) {
        this.before = before;
    }
}
