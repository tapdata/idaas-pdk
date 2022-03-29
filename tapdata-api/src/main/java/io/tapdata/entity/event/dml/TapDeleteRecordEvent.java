package io.tapdata.entity.event.dml;


import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.TapUtils;

import java.util.LinkedHashMap;
import java.util.Map;

public class TapDeleteRecordEvent extends TapRecordEvent {

    private Map<String, Object> before;

    public void clone(TapDeleteRecordEvent deleteRecordEvent) {
        super.clone(deleteRecordEvent);
        if(before != null)
            deleteRecordEvent.before = InstanceFactory.instance(TapUtils.class).cloneMap(before);
    }

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
